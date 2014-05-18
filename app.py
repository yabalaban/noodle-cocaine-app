# -*- coding: utf-8 -*-
"""app.py

В это модуле описано flask приложение, которое используется в PaaS Cocaine.
Однако оно так же может быть использовано и вне этого сервиса. Для этого
нужно будет избавиться от использования сервисов cocaine (заменить на эквивалентные).
Приложение представляет собой сервис, который принимает http запросы, валидирует
данные из запроса, обрабатывает их и складывает в hbase через hbase rest api.

Attributes:
    storage (cocaine.Service()): сервис, предоставляющий доступ к хранилищу
        PaaS Cocaine. В данном случае используется файловое хранилище для
        хранения и использования модулей обработки запросов.
    connection (starbase.Connection()): сервис, предоставляющий доступ к
        мастеру hbase через hbase rest api.
    api_url (str): адрес сервиса для валидации запроса
    memcache (memcache.Memcache()): объект для доступа к memcache.
"""
import json
import urllib2
import syslog
import imp

from cache import Memcache
from excepts import InvalidUsage
from flask import Flask, request, jsonify
from starbase import Connection
from cocaine.services import Service

storage = Service('storage')
connection = Connection(host='85.10.254.212', port='20550')
api_url = 'http://85.10.254.215:8000/api/key/validate?key=%s&collection=%s'
app = Flask(__name__)
memcache = Memcache()


def check_key(key, collection):
    """check_key(key, collection)

    Валидация соответсвия ключа с коллекцией. Валидация проводится путём запроса
    на сервис с валидацией. Для улучшения проиводительности используется memcache.
    Сервис валидации возвращает project id, который используется для определения
    таблицы в hbase. Сначала проверяется наличие данных в memcache. В случае, если
    они отсутствуют, делается запрос. При успешном запросе данные складываются в
    memcache на 10 минут.

    Args:
        key (str): ключ, который известно только пользователю
        collection (str): название таблицы в hbase

    Returns:
        Пара значений: project_id и сообщение. В случае, если валидация не пройдена,
        возвращается пара None, сообщение. В ином случае project_id, "OK"
    """
    project_id = memcache.get(collection)
    if project_id is None:
        url = api_url % (key, collection)
        try:
            data = urllib2.urlopen(url).read()
        except urllib2.HTTPError:
            return None, "Http error."
        response = json.loads(data)
        if response['status'] == 200:
            memcache.set(collection, response['project_id'])
            memcache.set(collection + '_key', key)
            return response['project_id'], 'OK'
        else:
            return None, "Invalid table name/secret key."
    else:
        collection_key = memcache.get(collection + '_key')
        if collection_key == key:
            return project_id, "OK"
        else:
            return None, "Invalid secret key."


def process_input(project_id, collection, secret_key, inp):
    """process_input(project_id, collection, secret_key, inp)

    Обработка входящего словаря специальным объектом.

    Args:
        project_id (int): id проекта
        collection (str): имя таблицы в hbase
        secret_key (str): ключ, известный только пользователю
        inp (dict): данные из запроса

    Returns:
        dict с обработанным данными, если объект обработки существует
        None в ином случае
    """
    key = project_id + '_' + collection + '_' + secret_key + '_pyc'
    proc_obj = memcache.get(str(key))
    if proc_obj is None:
        obj = imp.load_compiled(key[:-4], '/var/cache/cocaine/processing_codes/' + key[:-4] + '.pyc')
        proc_obj = obj.ProcessingClass()
        memcache.set(str(key), proc_obj, 3600)
    return proc_obj.process(inp)


@app.route('/processing_code/<string:collection>/<string:key>', methods=['GET', 'POST'])
def upload(collection, key):
    """upload(collection, key)

    Обработчик запроса по адресу '/processing_code/<string:collection>/<string:key>'
    Загружает файл объекта обработки в файловое хранилище.
    Для успешного сохранения необходимо валидировать название таблицы и ключа.

    Args:
        collection (str): название таблицы hbase
        key (str): секретный ключ, известный только пользователю
    """
    if request.method == 'POST':
        project_id, message = check_key(str(key), str(collection))
        if not project_id:
            return message
        else:
            f = request.files.get('file')
            storage.write('processing_codes', project_id + '_' + collection + '_' + key + '.pyc',
                          f.stream.getvalue(), ['processing'])
    return 'OK'


@app.route('/stream', methods=['GET', 'POST'])
def stream():
    """stream

    Обработчик запроса по адресу '/stream'
    Принимает запросы только с content-type application/json. Сохраняет данные из входящего запроса в hbase.
    Перед сохранением валидирует ключ и название столбца и использует объект-обработчик данных.

    Returns:
        'OK', если всё произошло успешно. В ином случае будет получено сообщение об ошибке.
    """
    if request.content_type is not None and 'application/json' in request.content_type:
        inp = json.loads(request.data)
        table = inp.get('collection')
        secret_key = inp.get('key')
        # Один/Все входные параметры отсутствуют
        if table is None or secret_key is None:
            raise InvalidUsage('Invalid parameters.', status_code=401)
        # Валидация запроса по ключу и азванию таблицы
        project_id, message = check_key(str(secret_key), str(table))
        if not project_id:
            # Валидация не пройдена
            raise InvalidUsage(message, status_code=401)
        # Подключение к таблице hbase
        t = connection.table(project_id + '_' + table)
        if not t.exists():
            # Такая таблица отсутствует
            raise InvalidUsage('Illegal collection name.')
        else:
            try:
                batch = t.batch()
                for row in inp.get('data'):
                    key = str(row.get('id'))
                    values = row.get('values')
                    for k in values.keys():
                        val = values[k]
                        if isinstance(val, basestring):
                            val = val.encode('utf-8')
                        values[k] = val
                    # Обработка входных данных
                    output = process_input(project_id, table, secret_key, values)
                    if output is None:
                        # Обработчик вернул ничего
                        continue
                    batch.insert(key, output)
                batch.commit()
                # Запись в лог об успешном запросе
                urllib2.urlopen('http://localhost:8001/' + table).read()
            except Exception as e:
                raise InvalidUsage(e.message)
            return 'OK'
    else:
        raise InvalidUsage('Only "application/json" content type is allowed.', status_code=415)


@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

if __name__ == '__main__':
    app.run(debug=True)

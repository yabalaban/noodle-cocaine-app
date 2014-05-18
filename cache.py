# -*- coding: utf-8 -*-
"""cache.py

В данном модуле содержится класс-обёртка к сервису memcache.

"""
import memcache


class Memcache():
    """Memcache

    Класс-обёртка к сервису memcache

    Attributes:
        hostname (str): адрес memcache сервера
        server (memcache.Client()): клиент memcache сервера
    """
    hostname = ''
    server = None

    def __init__(self, ip='127.0.0.1', port='11211'):
        """__init__(ip='127.0.0.1', port='11211')
        Конструктор класса, создающий клиента memcache сервера.

        Args:
            ip (str, optional): адрес memcache сервера
            port (str, optional): порт memcache сервера
        """
        self.hostname = '%s:%s' % (ip, port)
        self.server = memcache.Client([self.hostname])

    def set(self, key, value, expiration_date=600):
        """set(key, value, expiration_date=600)
        Сохраняет данный объект по заданному ключу на заданное время.

        Args:
            key (str): ключ, по которому следует сохранить объект
            value (obj): объект, который будет сохранен
            expiration_date (int, optional): время, на которое будет сохранен объект
        """
        self.server.set(key, value, expiration_date)

    def get(self, key):
        """get(key)
        Возвращает объект, предположительно хранящийся по заданному ключу.

        Args:
            key (str): ключ, по которому предположительно хранится объект

        Returns:
            Объект, если по данному ключу он существует. None в ином случае.
        """
        return self.server.get(key)

    def delete(self, key):
        """delete(key)
        Удаляет объект, предположительно хранящийся по заданному ключу.

        Args:
            key (str): ключ, по которому предположительно хранится объект
        """
        self.server.delete(key)
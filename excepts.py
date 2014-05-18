# -*- coding: utf-8 -*-
"""excepts.py

Этот модуль ипользуется для определения исключений, которые будут использованы в проекте.

"""

class InvalidUsage(Exception):
    """ InvalidUsage

    Класс, наследуемый от класса Exception.
    Содержит http код ошибки и текст ошибки.

    Attributes:
        status_code (int, optional): http код ошибки, 400 по умолчанию
    """
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        """__init_(message, status_code=None, payload=None)

        Конструктор класса, создающий объект исключения InvalidUsage.

        Args:
            message (str): сообщение об исключении
            status_code (int, optional): код ошибки
            payload (int, optional): дополнительная информация об исключении
        """
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        """to_dict()

        Представление объекта в виде словаря.

        Returns:
            Объект класса dict с информацией об ошибках. Сообщение исключения доступно
            по ключу 'message'.
        """
        response = dict(self.payload or ())
        response['message'] = self.message
        return response
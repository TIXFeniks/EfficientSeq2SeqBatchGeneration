from processor import BaseProcessor
from flask import Flask, request
from exceptions import MessagedException


class InvalidRequestSizeException(MessagedException):
    """
    Raised when the request was called with empty strings
    """
    pass


class Server:
    """
    Класс, занимающийся принятием и отправкой http запросов
    """
    def __init__(self, processor: BaseProcessor, host='localhost', port=2532, timeout=5, max_query_len=50):
        self.app = Flask('balancer')
        self.timeout = timeout
        self.host = host
        self.port = port
        self.max_query_len = max_query_len
        self.register_callbacks()
        self.processor = processor

    def register_callbacks(self):
        """
        Привязывает обработчик http запросов к пути запроса
        """
        self.app.route('/', methods=['GET'])(self.get_query)

    def get_query(self):
        """
        Обработчик запроса
        """
        query = request.args.get('query', '')
        assert self.max_query_len >= len(query) > 0, InvalidRequestSizeException

        result = self.processor.process_query(query, self.timeout)

        return result

    def run(self):
        """
        Запускает сервер
        """
        self.processor.start()

        self.app.run(host=self.host, port=self.port, threaded=True)

        self.processor.stop()

from threading import Thread, Event
from concurrent.futures import Future
from time import time
from pipeline import Pipeline
from workers.model import ModelApplier
from workers.tokenization import Tokenizer, Detokenizer
from workers.utils import Flattener
from exceptions import MessagedException
import sys


class ProcessingTask:
    """
    Представление задачи для обработки
    """
    max_id = 0

    def __init__(self, data):
        self.data = data
        ProcessingTask.max_id += 1
        self.id = ProcessingTask.max_id
        self.time_created = time()


class ProcessingQueueOverflowException(MessagedException):
    """
    Raised when the processing pipeline is full
    """
    pass


class BaseProcessor:
    """
    Базовый обработчик запросов
    """
    def __init__(self, *workers, queue_size=900):
        self.pipeline = Pipeline(Tokenizer(), *workers, Detokenizer())
        self.tasks = {}

        self.stop_event = None
        self.queue_thread = Thread(target=self.queue_worker)
        self.queue_thread.daemon = True

        self.queue_size = queue_size

    def start(self):
        """
        Запускает процессы обработки заданий
        """
        print("started worker")
        self.stop_event = Event()
        self.queue_thread.start()
        self.pipeline.start()

    def stop(self):
        """
        Останавливает процессы обработки заданий
        """
        self.stop_event.set()
        self.pipeline.stop()

    def process_query(self, query, timeout=None):
        """
        Обрабатывает запрос на вывод из модели
        :return: Обработанный запрос
        """
        if len(self.tasks) >= self.queue_size:
            print("Queue overflow", file=sys.stderr)
            raise ProcessingQueueOverflowException("Queue is full, try requesting later")
        task = ProcessingTask(query)
        print("A new task: ", task.data, " queue size: ", len(self.tasks), 'component queues', self.pipeline.q_sizes())
        future = Future()
        self.tasks[task.id] = future

        self.pipeline.input.put(task)
        try:
            return future.result(timeout)
        except TimeoutError as e:
            self.tasks.pop(task.id)
            raise e

    def queue_worker(self):
        """
        Поток получения запросов из self.pipeline
        """
        while not self.stop_event.is_set():
            result = self.pipeline.output.get()
            if result.id in self.tasks:
                self.tasks[result.id].set_result(result.data)
                self.tasks.pop(result.id)


class BaseModelProcessor(BaseProcessor):
    """
    Обработчик запросов, использующий модель
    """
    def __init__(self, *workers, model_path):
        super(BaseModelProcessor, self).__init__(*workers, ModelApplier(model_path), Flattener())


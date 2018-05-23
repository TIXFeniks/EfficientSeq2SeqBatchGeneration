from multiprocessing import Process, Queue


class MissingInputException(Exception):
    """
    Raised when input was not provided
    """
    pass


class Worker:
    """
    Этап обработки
    """
    def __init__(self):
        self.process = Process(target=self.worker)
        self.process.daemon = True
        self._input = None
        self._output = Queue()

    def before_start(self):
        """
        Выполняется перед запуском цикла обработки данного этапа
        """
        pass

    def worker(self):
        """
        Запускает основоной цикл
        """
        self.before_start()
        while True:
            self.job()

    def job(self):
        """
        Обработка задания
        """
        raise NotImplementedError()

    def start(self):
        """
        Запуск этапа
        """
        assert self._input is not None, MissingInputException
        self.process.start()

    def stop(self):
        """
        Остановка этапа обработки
        """
        self.process.terminate()

    @property
    def input(self):
        """
        Входная очередь задач
        """
        return self._input

    @input.setter
    def input(self, value):
        """
        Задаёт выодную очередь задач
        """
        self._input = value

    @property
    def output(self):
        """
        Выходная очередь задач
        """
        return self._output


class CustomWorker(Worker):
    """
    Этап обработки, выполняющий произвольную функцию
    """
    def __init__(self, job):
        super(CustomWorker, self).__init__()
        self._job = job

    def job(self):
        """
        Выполняет этап обработки
        """
        self._job(self)


class Pipeline:
    """
    Связывает этапы обработки
    """
    def __init__(self, *workers):
        self.output = self.input = Queue()
        self.started = False
        self.workers = []
        for worker in workers:
            self.add_worker(worker)

    def start(self):
        """
        Запускает этапы обработки
        """
        for worker in self.workers:
            worker.start()
        self.started = True

    def stop(self):
        """
        Останавливает этапы обработки
        """
        for worker in self.workers:
            worker.stop()
        self.started = False

    def add_worker(self, worker):
        """
        Добавляет этап обработки
        """
        worker.input = self.output
        self.output = worker.output

        self.workers.append(worker)

    def pop_worker(self):
        """
        Удаляет последний этап обработки
        """
        worker = self.workers.pop(-1)
        self.output = worker.input
        return worker

    def q_sizes(self):
        """
        Возвращает количество запросов, ожидающих в очереди между каждым из этапов
        """
        return [worker.input.qsize() for worker in self.workers]

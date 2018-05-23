from pipeline import Worker


class Flattener(Worker):
    """
    Этап разгруппирования запросов
    """
    def job(self):
        """
        Разгруппирует запросы
        """
        batch = self.input.get()
        for task in batch:
            self.output.put(task)
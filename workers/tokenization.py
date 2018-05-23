from pipeline import Worker


class Tokenizer(Worker):
    """
    Этап токенизации
    """
    def job(self):
        """
        Выполняет токенизацию
        """
        task = self.input.get()
        task.data = list(map(lambda ch: ord(ch), task.data))
        self.output.put(task)


class Detokenizer(Worker):
    """
    Этап детокенизации
    """
    def job(self):
        """
        Выполняет детокенизацию
        """
        task = self.input.get()
        task.data = ''.join(map(lambda ch: chr(ch), filter(lambda ch: ch >= 0, task.data)))
        self.output.put(task)


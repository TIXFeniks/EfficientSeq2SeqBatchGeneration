class BaseModel:
    """
    Интерфейс модели
    """
    def process(self, batch: list):
        """
        Обработать группу запросов
        :param batch: группа запросов для обработки
        :return: результат применения модели
        """

        raise NotImplementedError()


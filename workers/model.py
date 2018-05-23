from pipeline import Worker
import pickle as pkl
from exceptions import MessagedException


class ModelLoadingError(MessagedException):
    """
    Raised when model is not deserializable in current environment
    """
    pass


class ModelApplier(Worker):
    """
    Этап применения модели
    """
    def __init__(self, model_path):
        super(ModelApplier, self).__init__()
        with open(model_path, 'rb') as f:
            try:
                self.model = pkl.load(f)
            except ModuleNotFoundError as e:
                raise ModelLoadingError(
                    'Unable to deserialize(unpickle) this model in current environment. Try adding path to {} into PYTHONPATH variable'.format(
                        (e.path + '.' if e.path else '') + e.name))
            except pkl.UnpicklingError:
                raise ModelLoadingError('Invalid format of the model')

    def job(self):
        """
        Применяет модель к группе данных
        """
        batch = self.input.get()
        data = list(map(lambda task: task.data, batch))
        max_len = max(map(lambda x: len(x), data))

        def pad_sequence(seq):
            return seq + [-1] * (max_len - len(seq))

        data = list(map(pad_sequence, data))

        for i, result in enumerate(self.model.process(data)):
            batch[i].data = result
        self.output.put(batch)

from time import sleep
from math import ceil


class DummyTestModel:
    """
    A model, that sleeps asymptotically the same time as the Seq2Seq model

    """

    def __init__(self, alpha=0.01, parallel_size=1000, default_delay=0.05):
        """
        Init the dummy model
        :param alpha: parameter that scales the time consumed by process function
        :param parallel_size: total size that can be processed simultaneously
        """

        self.alpha = alpha
        self.parallel_size = parallel_size
        self.default_delay = default_delay

    def process(self, batch):
        """
        batch of data
        :param batch:
        :return: the same data as in batch
        """

        time = self.alpha
        lens = [len(seq) for seq in batch]

        max_len = max(lens)

        total_data = len(batch) * max_len

        time *= max_len * ceil(total_data / self.parallel_size)
        time += self.default_delay

        sleep(time)
        return batch
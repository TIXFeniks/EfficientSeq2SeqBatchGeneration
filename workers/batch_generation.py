from pipeline import Worker
from time import time
from sortedcontainers import SortedListWithKey
from math import floor
from queue import Empty


class TimeoutBatchGenerator(Worker):
    """
    Простой этап группировки запросов
    """
    def __init__(self, batch_size=32, timeout=4):
        super(TimeoutBatchGenerator, self).__init__()
        self.batch_size = batch_size
        self.timeout = timeout
        self.oldest = time()
        self.batch = []

    def before_start(self):
        """
        Инициализирует этап обработки
        """
        self.oldest = time()
        self.batch.clear()

    def job(self):
        """
        Выполняет группировку запросов
        """
        compute_right_now = False
        timeout = (self.timeout - (time() - self.oldest)) - self.timeout / 4  # TODO: fix the constant

        if timeout > 0 or len(self.batch) == 0:
            try:
                task = self.input.get(timeout=timeout if len(self.batch) > 0 else None)
                if task.time_created < self.oldest or len(self.batch) == 0:
                    self.oldest = task.time_created
                self.batch.append(task)
            except Empty:
                compute_right_now = True
        else:
            compute_right_now = True
        if len(self.batch) >= self.batch_size or compute_right_now:
            self.output.put(self.batch[:])
            self.before_start()


class TimeoutCostBatchGenerator(TimeoutBatchGenerator):
    """
    Эффективный этап группировки запросов
    """
    def __init__(self, batch_size=64, timeout=4, parallel_size=1000):
        super(TimeoutCostBatchGenerator, self).__init__(batch_size, timeout)

        self.parallel_size = parallel_size

        self.batch_by_time = SortedListWithKey(key=self.get_time)
        self.batch_by_len = SortedListWithKey(key=lambda _task: len(_task.data))
        self.total_len = 0
        self.best_batches = {}

    @staticmethod
    def get_time(task):
        """
        Возвращает время создания запроса
        """
        return task.time_created

    def job(self):
        """
        Группирует запросы
        """
        if len(self.batch_by_time) > 0:
            oldest = self.get_time(self.batch_by_time[0])
        else:
            oldest = time()

        compute_right_now = False
        timeout = (self.timeout - (time() - oldest)) - self.timeout / 10  # TODO: fix the constant

        if timeout > 0 or self.output.qsize() > 1 or len(self.batch_by_len) < self.batch_size / 4:
            try:
                task = self.input.get(timeout=timeout if len(self.batch_by_len) > 0 else None)
                self.total_len += len(task.data)
                self.batch_by_len.add(task)
                self.batch_by_time.add(task)
            except Empty:
                compute_right_now = True
        else:
            compute_right_now = True

        # print(len(self.batch_by_len))
        if len(self.batch_by_len) >= self.batch_size or compute_right_now:
            oldest_task = self.batch_by_time[0]

            best_cost = float('inf')

            best_left = 0
            best_right = 1

            idx_left = 0

            oldest_idx = float('inf')
            if timeout < 0:
                oldest_idx = idx_left = self.batch_by_len.index(oldest_task)

            while idx_left < len(self.batch_by_len):
                task = self.batch_by_len[idx_left]
                new_idx_left, new_idx_right, new_cost = self.get_best_batch(task)
                if oldest_idx < new_idx_left:
                    break
                if new_cost < best_cost:
                    best_cost = new_cost
                    best_left = new_idx_left
                    best_right = new_idx_right
                idx_left = max(new_idx_right, self.batch_by_len.bisect_right(task))

            batch = self.batch_by_len[best_left:best_right]
            self.output.put(batch)
            for task in batch:
                self.total_len -= len(task.data)
                self.batch_by_time.remove(task)
                self.batch_by_len.remove(task)

    def get_best_batch(self, task):
        """
        Возвращает наиболее эффективную группу, содержащую данный запрос
        """
        idx = self.batch_by_len.index(task)
        idx_left = self.batch_by_len.bisect_left(task)
        idx_right = self.batch_by_len.bisect_right(task)

        qlen = len(task.data)

        if (idx - idx_left + 1) * qlen > self.parallel_size:
            idx_left = idx - floor(self.parallel_size / qlen) + 1

        if (idx_right - idx_left + 1) * qlen > self.parallel_size:
            idx_right = idx_left + floor(self.parallel_size / qlen)
        if idx_right == idx_left:
            idx_right += 1

        return self.expand_batch(idx_left, idx_right)

    def expand_batch(self, idx_left, idx_right):
        """
        Дополняет группу, заданную границами в сортированном по длине массиве запросов, до максимальной эффективной
        """
        total_added_cost = 0

        qlen = len(self.batch_by_len[idx_right - 1].data)

        if not ((idx_right - idx_left + 1) * qlen < self.parallel_size and (
                idx_left > 0 or idx_right < len(self.batch_by_len))):
            return idx_left, idx_right, 0

        batch_len = idx_right - idx_left

        free_space = self.parallel_size - batch_len * qlen

        l_zeros = float('Inf')
        l_idx = -1
        if idx_left > 0:
            left_task = self.batch_by_len[idx_left - 1]
            l_len = len(left_task.data)
            l_diff = qlen - l_len
            l_idx = self.batch_by_len.bisect_key_left(l_len)
            n_take = min(idx_left - l_idx, floor(free_space / qlen))
            if n_take > 0:
                l_zeros = l_diff * n_take

        r_zeros = float('Inf')
        r_idx = -1
        r_len = qlen
        if idx_right < len(self.batch_by_len):
            right_task = self.batch_by_len[idx_right]

            r_len = len(right_task.data)
            r_diff = r_len - qlen
            r_idx = self.batch_by_len.bisect_key_right(r_len)
            n_take = min(r_idx - idx_right,
                         floor((free_space - batch_len) / qlen))
            if n_take > 0:
                r_zeros = batch_len * r_diff

        if r_zeros == float('inf') and l_zeros == float('inf'):
            return idx_left, idx_right, 0

        if l_zeros <= r_zeros:
            idx_left = l_idx
            total_added_cost += l_zeros
        else:
            idx_right = r_idx
            total_added_cost += r_zeros

        idx_left, idx_right, additional_cost = self.expand_batch(idx_left, idx_right)

        return idx_left, idx_right, total_added_cost + additional_cost


class NaiveBatchGenerator(Worker):
    """
    Этап наивной группировки запросов
    """
    def job(self):
        """
        Выполняет этап наивной группировки запросов
        """
        self.output.put([self.input.get()])

from multiprocessing import Lock, Value


class Counter(object):
    def __init__(self, value=None, lock=True):
        self.count = value or Value('i', 0, lock=lock)
        self.mutex = Lock()

    def increment(self, by=1):
        with self.mutex:
            self.count.value += by

    def value(self):
        with self.mutex:
            return self.count.value

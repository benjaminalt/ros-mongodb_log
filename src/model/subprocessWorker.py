from multiprocessing import Queue
from counter import Counter
from constants import WORKER_NODE_NAME
from threading import Thread

import subprocess
import string


class SubprocessWorker(object):
    def __init__(self, idnum, topic, collname, in_counter_value, out_counter_value,
                 drop_counter_value, queue_maxsize,
                 mongodb_host, mongodb_port, mongodb_name, nodename_prefix, cpp_logger, additional_parameters=[]):

        self.name = "SubprocessWorker-%4d-%s" % (idnum, topic)
        self.id = idnum
        self.topic = topic
        self.collname = collname
        self.queue = Queue(queue_maxsize)
        self.out_counter = Counter(out_counter_value)
        self.in_counter  = Counter(in_counter_value)
        self.drop_counter = Counter(drop_counter_value)
        self.worker_out_counter = Counter()
        self.worker_in_counter  = Counter()
        self.worker_drop_counter = Counter()
        self.mongodb_host = mongodb_host
        self.mongodb_port = mongodb_port
        self.mongodb_name = mongodb_name
        self.nodename_prefix = nodename_prefix
        self.quit = False
        self.qsize = 0

        self.thread = Thread(name=self.name, target=self.run)

        mongodb_host_port = "%s:%d" % (mongodb_host, mongodb_port)
        collection = "%s.%s" % (mongodb_name, collname)
        nodename = WORKER_NODE_NAME % (self.nodename_prefix, self.id, self.collname)

        self.process = subprocess.Popen([cpp_logger, "-t", topic, "-n", nodename,
                                         "-m", mongodb_host_port, "-c", collection] + additional_parameters,
                                        stdout=subprocess.PIPE)

        self.thread.start()

    def qsize(self):
        return self.qsize

    def run(self):
        while not self.quit:
            line = self.process.stdout.readline().rstrip()
            if line == "": continue
            arr = string.split(line, ":")
            self.in_counter.increment(int(arr[0]))
            self.out_counter.increment(int(arr[1]))
            self.drop_counter.increment(int(arr[2]))
            self.qsize = int(arr[3])

            self.worker_in_counter.increment(int(arr[0]))
            self.worker_out_counter.increment(int(arr[1]))
            self.worker_drop_counter.increment(int(arr[2]))

    def shutdown(self):
        self.quit = True
        self.process.kill()
        self.process.wait()
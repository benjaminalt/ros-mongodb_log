from multiprocessing import Process, Queue, Value, current_process
from counter import Counter
from pymongo import MongoClient, SLOW_ONLY
from pymongo.errors import InvalidDocument, InvalidStringData
from constants import WORKER_NODE_NAME
from random import randint
from datetime import datetime, timedelta
from Queue import Empty
import genpy
import rospy
import rostopic
import time


class WorkerProcess(object):
    def __init__(self, idnum, topic, collname, in_counter_value, out_counter_value,
                 drop_counter_value, queue_maxsize,
                 mongodb_host, mongodb_port, mongodb_name, nodename_prefix):
        self.name = "WorkerProcess-%4d-%s" % (idnum, topic)
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
        self.quit = Value('i', 0)

        self.process = Process(name=self.name, target=self.run)
        self.process.start()

    def init(self):
            self.mongoconn = MongoClient(self.mongodb_host, self.mongodb_port)
            self.mongodb = self.mongoconn[self.mongodb_name]
            self.mongodb.set_profiling_level = SLOW_ONLY

            self.collection = self.mongodb[self.collname]
            self.collection.count()

            self.queue.cancel_join_thread()

            rospy.init_node(WORKER_NODE_NAME % (self.nodename_prefix, self.id, self.collname),
                            anonymous=False)

            self.subscriber = None
            while not self.subscriber:
                try:
                    msg_class, real_topic, msg_eval = rostopic.get_topic_class(self.topic, blocking=True)
                    self.subscriber = rospy.Subscriber(real_topic, msg_class, self.enqueue, self.topic)
                except rostopic.ROSTopicIOException:
                    print("FAILED to subscribe, will keep trying %s" % self.name)
                    time.sleep(randint(1,10))
                except rospy.ROSInitException:
                    print("FAILED to initialize, will keep trying %s" % self.name)
                    time.sleep(randint(1,10))
                    self.subscriber = None

    def run(self):
        self.init()

        print("ACTIVE: %s" % self.name)

        # run the thread
        self.dequeue()

        # free connection
        # self.mongoconn.end_request()

    def is_quit(self):
        return self.quit.value == 1

    def shutdown(self):
        if not self.is_quit():
            #print("SHUTDOWN %s qsize %d" % (self.name, self.queue.qsize()))
            self.quit.value = 1
            self.queue.put("shutdown")
            while not self.queue.empty(): time.sleep(0.1)
        #print("JOIN %s qsize %d" % (self.name, self.queue.qsize()))
        self.process.join()
        self.process.terminate()

    def sanitize_value(self, v):
        if isinstance(v, rospy.Message):
            return self.message_to_dict(v)
        elif isinstance(v, genpy.rostime.Time):
            t = datetime.utcfromtimestamp(v.secs)
            return t + timedelta(microseconds=v.nsecs / 1000.)
        elif isinstance(v, genpy.rostime.Duration):
            return v.secs + v.nsecs / 1000000000.
        elif isinstance(v, list):
            return [self.sanitize_value(t) for t in v]
        else:
            return v

    def message_to_dict(self, val):
        d = {}
        for f in val.__slots__:
            d[f] = self.sanitize_value(getattr(val, f))
        return d

    def qsize(self):
        return self.queue.qsize()

    def enqueue(self, data, topic, current_time=None):
        if not self.is_quit():
            if self.queue.full():
                try:
                    self.queue.get_nowait()
                    self.drop_counter.increment()
                    self.worker_drop_counter.increment()
                except Empty:
                    pass
            #self.queue.put((topic, data, current_time or datetime.now()))
            self.queue.put((topic, data, rospy.get_time()))
            self.in_counter.increment()
            self.worker_in_counter.increment()

    def dequeue(self):
        while not self.is_quit():
            t = None
            try:
                t = self.queue.get(True)
            except IOError:
                # Anticipate Ctrl-C
                #print("Quit W1: %s" % self.name)
                self.quit.value = 1
                break
            if isinstance(t, tuple):
                self.out_counter.increment()
                self.worker_out_counter.increment()
                topic = t[0]
                msg   = t[1]
                ctime = t[2]

                if isinstance(msg, rospy.Message):
                    doc = self.message_to_dict(msg)
                    doc["__recorded"] = ctime or datetime.now()
                    doc["__topic"]    = topic
                    try:
                        #print(self.sep + threading.current_thread().getName() + "@" + topic+": ")
                        #pprint.pprint(doc)
                        self.collection.insert(doc)
                    except InvalidDocument, e:
                        print("InvalidDocument " + current_process().name + "@" + topic +": \n")
                        print e
                    except InvalidStringData, e:
                        print("InvalidStringData " + current_process().name + "@" + topic +": \n")
                        print e

            else:
                #print("Quit W2: %s" % self.name)
                self.quit.value = 1

        # we must make sure to clear the queue before exiting,
        # or the parent thread might deadlock otherwise
        #print("Quit W3: %s" % self.name)
        self.subscriber.unregister()
        self.subscriber = None
        while not self.queue.empty():
            t = self.queue.get_nowait()
        print("STOPPED: %s" % self.name)
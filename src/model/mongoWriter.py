import rostopic

from subprocessWorker import SubprocessWorker
from worker import WorkerProcess
from counter import Counter
from constants import NODE_NAME_TEMPLATE, PACKAGE_NAME, QUEUE_MAXSIZE, STATS_LOOPTIME, STATS_GRAPHTIME
from tf.msg import tfMessage
from tf2_msgs.msg import TFMessage
from roslib.packages import find_node
from sensor_msgs.msg import PointCloud, CompressedImage, Image
from designator_integration_msgs.msg import DesignatorRequest
from designator_integration_msgs.msg import DesignatorResponse
from designator_integration_msgs.msg import Designator
from datetime import datetime, timedelta
from threading import Thread, Timer
from time import sleep
import subprocess
from tempfile import mktemp

import os
import re
import rospy
import rosgraph
import rrdtool

class MongoWriter(object):
    def __init__(self, topics = [], graph_topics = False,
                 graph_dir = ".", graph_clear = False, graph_daemon = False,
                 all_topics = False, all_topics_interval = 5,
                 exclude_topics = [],
                 mongodb_host=None, mongodb_port=None, mongodb_name="roslog",
                 no_specific=False, nodename_prefix="", collection_name="/tf"):
        self.graph_dir = graph_dir
        self.graph_topics = graph_topics
        self.graph_clear = graph_clear
        self.graph_daemon = graph_daemon
        self.all_topics = all_topics
        self.all_topics_interval = all_topics_interval
        self.exclude_topics = exclude_topics
        self.mongodb_host = mongodb_host
        self.mongodb_port = mongodb_port
        self.mongodb_name = mongodb_name
        self.no_specific = no_specific
        self.nodename_prefix = nodename_prefix
        self.quit = False
        self.topics = set()
        self.sep = "\n" #'\033[2J\033[;H'
        self.in_counter = Counter()
        self.out_counter = Counter()
        self.drop_counter = Counter()
        self.workers = {}
        self.collection_name = str(collection_name)

        if self.graph_dir == ".": self.graph_dir = os.getcwd()
        if not os.path.exists(self.graph_dir): os.makedirs(self.graph_dir)

        self.exclude_regex = []
        for et in self.exclude_topics:
            self.exclude_regex.append(re.compile(et))
        self.exclude_already = []

        self.init_rrd()

        self.subscribe_topics(set(topics), self.collection_name)
        if self.all_topics:
            print("All topics")
            self.ros_master = rosgraph.masterapi.Master(NODE_NAME_TEMPLATE % self.nodename_prefix)
            self.update_topics(restart=False)
        rospy.init_node(NODE_NAME_TEMPLATE % self.nodename_prefix, anonymous=True)

        self.start_all_topics_timer()

    def subscribe_topics(self, topics, collection_name):
        for topic in topics:
            if topic and topic[-1] == '/':
                topic = topic[:-1]

            if topic in self.topics: continue
            if topic in self.exclude_already: continue

            do_continue = False
            for tre in self.exclude_regex:
                if tre.match(topic):
                    print("*** IGNORING topic %s due to exclusion rule" % topic)
                    do_continue = True
                    self.exclude_already.append(topic)
                    break
            if do_continue: continue

            # although the collections is not strictly necessary, since MongoDB could handle
            # pure topic names as collection names and we could then use mongodb[topic], we want
            # to have names that go easier with the query tools, even though there is the theoretical
            # possibility of name classes (hence the check)
            #collname = topic.replace("/", "_")[1:]
            collname = collection_name
            if collname in self.workers.keys():
                print("Two converted topic names clash: %s, ignoring topic %s"
                      % (collname, topic))
            else:
                print("Adding topic %s" % topic)
                self.workers[collname] = self.create_worker(len(self.workers), topic, collname)
                self.topics |= set([topic])

    def create_worker(self, idnum, topic, collname):
        msg_class, real_topic, msg_eval = rostopic.get_topic_class(topic, blocking=True)

        w = None
        node_path = None
        additional_parameters = [];

        if not self.no_specific and (msg_class == tfMessage) or (msg_class == TFMessage):
            print("DETECTED transform topic %s, using fast C++ logger" % topic)
            node_path = find_node(PACKAGE_NAME, "mongodb_log_tf")
            #additional_parameters = ["-a"]
            #additional_parameters = ["-k" "0.005" "-l" "0.005" "-g" "0"]
            #additional_parameters = ["-k" "0.025" "-l" "0.025" "-g" "0"]

            # Log only when the preceeding entry of that
            # transformation had at least 0.100 vectorial and radial
            # distance to its predecessor transformation, but at least
            # every second.
            additional_parameters = ["-k" "0.100" "-l" "0.100" "-g" "1"]
            if not node_path:
                print("FAILED to detect mongodb_log_tf, falling back to generic logger (did not build package?)")
        elif not self.no_specific and msg_class == PointCloud:
            print("DETECTED point cloud topic %s, using fast C++ logger" % topic)
            node_path = find_node(PACKAGE_NAME, "mongodb_log_pcl")
            if not node_path:
                print("FAILED to detect mongodb_log_pcl, falling back to generic logger (did not build package?)")
        elif not self.no_specific and msg_class == Image:
            print("DETECTED compressed image topic %s, using fast C++ logger" % topic)
            node_path = find_node(PACKAGE_NAME, "mongodb_log_img")
            if not node_path:
                print("FAILED to detect mongodb_log_img, falling back to generic logger (did not build package?)")
        elif not self.no_specific and msg_class == CompressedImage:
            print("DETECTED compressed image topic %s, using fast C++ logger" % topic)
            node_path = find_node(PACKAGE_NAME, "mongodb_log_cimg")
            if not node_path:
                print("FAILED to detect mongodb_log_cimg, falling back to generic logger (did not build package?)")
        elif not self.no_specific and msg_class == DesignatorRequest:
            print("DETECTED designator request topic %s, using fast C++ logger" % topic)
            node_path = find_node(PACKAGE_NAME, "mongodb_log_desig")
            additional_parameters = ["-d" "designator-request"]
            if not node_path:
                print("FAILED to detect mongodb_log_desig, falling back to generic logger (did not build package?)")
        elif not self.no_specific and msg_class == DesignatorResponse:
            print("DETECTED designator response topic %s, using fast C++ logger" % topic)
            node_path = find_node(PACKAGE_NAME, "mongodb_log_desig")
            additional_parameters = ["-d" "designator-response"]
            if not node_path:
                print("FAILED to detect mongodb_log_desig, falling back to generic logger (did not build package?)")
        elif not self.no_specific and msg_class == Designator:
            print("DETECTED designator topic %s, using fast C++ logger" % topic)
            node_path = find_node(PACKAGE_NAME, "mongodb_log_desig")
            additional_parameters = ["-d" "designator"]
            if not node_path:
                print("FAILED to detect mongodb_log_desig, falling back to generic logger (did not build package?)")
        """
        elif msg_class == TriangleMesh:
            print("DETECTED triangle mesh topic %s, using fast C++ logger" % topic)
            node_path = find_node(PACKAGE_NAME, "mongodb_log_trimesh")
            if not node_path:
                print("FAILED to detect mongodb_log_trimesh, falling back to generic logger (did not build package?)")
        """

        if node_path:
            w = SubprocessWorker(idnum, topic, collname,
                                 self.in_counter.count, self.out_counter.count,
                                 self.drop_counter.count, QUEUE_MAXSIZE,
                                 self.mongodb_host, self.mongodb_port, self.mongodb_name,
                                 self.nodename_prefix, node_path[0], additional_parameters)

        if not w:
            print("GENERIC Python logger used for topic %s" % topic)
            w = WorkerProcess(idnum, topic, collname,
                              self.in_counter.count, self.out_counter.count,
                              self.drop_counter.count, QUEUE_MAXSIZE,
                              self.mongodb_host, self.mongodb_port, self.mongodb_name,
                              self.nodename_prefix)

        if self.graph_topics: self.assert_worker_rrd(collname)

        return w


    def run(self):
        looping_threshold = timedelta(0, STATS_LOOPTIME,  0)

        self.graph_thread = Thread(name="RRDGrapherThread", target=self.graph_rrd_thread)
        self.graph_thread.daemon = True
        self.graph_thread.start()

        while not rospy.is_shutdown() and not self.quit:
            started = datetime.now()

            if self.graph_daemon and self.graph_process.poll() != None:
                print("WARNING: rrdcached died, falling back to non-cached version. Please investigate.")
                self.graph_daemon = False

            self.update_rrd()

            # the following code makes sure we run once per STATS_LOOPTIME, taking
            # varying run-times and interrupted sleeps into account
            td = datetime.now() - started
            while not rospy.is_shutdown() and not self.quit and td < looping_threshold:
                sleeptime = STATS_LOOPTIME - (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6
                if sleeptime > 0: sleep(sleeptime)
                td = datetime.now() - started


    def shutdown(self):
        self.quit = True
        if hasattr(self, "all_topics_timer"): self.all_topics_timer.cancel()
        for name, w in self.workers.items():
            #print("Shutdown %s" % name)
            w.shutdown()

        if self.graph_daemon:
            self.graph_process.kill()
            self.graph_process.wait()

    def start_all_topics_timer(self):
        if not self.all_topics or self.quit: return
        self.all_topics_timer = Timer(self.all_topics_interval, self.update_topics)
        self.all_topics_timer.start()


    def update_topics(self, restart=True):
        if not self.all_topics or self.quit: return
        ts = self.ros_master.getPublishedTopics("/")
        topics = set([t for t, t_type in ts if t != "/rosout" and t != "/rosout_agg"])
        new_topics = topics - self.topics
        self.subscribe_topics(new_topics, self.collection_name)
        if restart: self.start_all_topics_timer()

    def get_memory_usage_for_pid(self, pid):

        scale = {'kB': 1024, 'mB': 1024 * 1024,
                 'KB': 1024, 'MB': 1024 * 1024}
        try:
            f = open("/proc/%d/status" % pid)
            t = f.read()
            f.close()
        except:
            return (0, 0, 0)

        if t == "": return (0, 0, 0)

        try:
            tmp   = t[t.index("VmSize:"):].split(None, 3)
            size  = int(tmp[1]) * scale[tmp[2]]
            tmp   = t[t.index("VmRSS:"):].split(None, 3)
            rss   = int(tmp[1]) * scale[tmp[2]]
            tmp   = t[t.index("VmStk:"):].split(None, 3)
            stack = int(tmp[1]) * scale[tmp[2]]
            return (size, rss, stack)
        except ValueError:
            return (0, 0, 0)

    def get_memory_usage(self):
        size, rss, stack = 0, 0, 0
        for _, w in self.workers.items():
            pmem = self.get_memory_usage_for_pid(w.process.pid)
            size  += pmem[0]
            rss   += pmem[1]
            stack += pmem[2]
        #print("Size: %d  RSS: %s  Stack: %s" % (size, rss, stack))
        return (size, rss, stack)

    def assert_rrd(self, file, *data_sources):
        if not os.path.isfile(file) or self.graph_clear:
            rrdtool.create(file, "--step", "10", "--start", "0",
                           # remember that we always need to add the previous RRA time range
                           # hence number of rows is not directly calculated by desired time frame
                           "RRA:AVERAGE:0.5:1:720",    #  2 hours of 10 sec  averages
                           "RRA:AVERAGE:0.5:3:1680",   # 12 hours of 30 sec  averages
                           "RRA:AVERAGE:0.5:30:456",   #  1 day   of  5 min  averages
                           "RRA:AVERAGE:0.5:180:412",  #  7 days  of 30 min  averages
                           "RRA:AVERAGE:0.5:720:439",  #  4 weeks of  2 hour averages
                           "RRA:AVERAGE:0.5:8640:402", #  1 year  of  1 day averages
                           "RRA:MIN:0.5:1:720",
                           "RRA:MIN:0.5:3:1680",
                           "RRA:MIN:0.5:30:456",
                           "RRA:MIN:0.5:180:412",
                           "RRA:MIN:0.5:720:439",
                           "RRA:MIN:0.5:8640:402",
                           "RRA:MAX:0.5:1:720",
                           "RRA:MAX:0.5:3:1680",
                           "RRA:MAX:0.5:30:456",
                           "RRA:MAX:0.5:180:412",
                           "RRA:MAX:0.5:720:439",
                           "RRA:MAX:0.5:8640:402",
                           *data_sources)

    def graph_rrd_thread(self):
        graphing_threshold = timedelta(0, STATS_GRAPHTIME - STATS_GRAPHTIME*0.01, 0)
        first_run = True

        while not rospy.is_shutdown() and not self.quit:
            started = datetime.now()

            if not first_run: self.graph_rrd()
            else: first_run = False

            # the following code makes sure we run once per STATS_LOOPTIME, taking
            # varying run-times and interrupted sleeps into account
            td = datetime.now() - started
            while not rospy.is_shutdown() and not self.quit and td < graphing_threshold:
                sleeptime = STATS_GRAPHTIME - (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6
                if sleeptime > 0: sleep(sleeptime)
                td = datetime.now() - started

    def graph_rrd(self):
        #print("Generating graphs")
        time_started = datetime.now()
        rrdtool.graph(["%s/logstats.png" % self.graph_dir,
                       "--start=-600", "--end=-10",
                       "--disable-rrdtool-tag", "--width=560",
                       "--font", "LEGEND:10:", "--font", "UNIT:8:",
                       "--font", "TITLE:12:", "--font", "AXIS:8:",
                       "--title=MongoDB Logging Stats",
                       "--vertical-label=messages/sec",
                       "--slope-mode"]
                      + (self.graph_daemon and self.graph_daemon_args or []) +
                      ["DEF:qsize=%s/logstats.rrd:qsize:AVERAGE:step=10" % self.graph_dir,
                       "DEF:in=%s/logstats.rrd:in:AVERAGE:step=10" % self.graph_dir,
                       "DEF:out=%s/logstats.rrd:out:AVERAGE:step=10" % self.graph_dir,
                       "DEF:drop=%s/logstats.rrd:drop:AVERAGE:step=10" % self.graph_dir,
                       "LINE1:qsize#FF7200:Queue Size",
                       "GPRINT:qsize:LAST:Current\\:%8.2lf %s",
                       "GPRINT:qsize:AVERAGE:Average\\:%8.2lf %s",
                       "GPRINT:qsize:MAX:Maximum\\:%8.2lf %s\\n",
                       "LINE1:in#503001:In",
                       "GPRINT:in:LAST:        Current\\:%8.2lf %s",
                       "GPRINT:in:AVERAGE:Average\\:%8.2lf %s",
                       "GPRINT:in:MAX:Maximum\\:%8.2lf %s\\n",
                       "LINE1:out#EDAC00:Out",
                       "GPRINT:out:LAST:       Current\\:%8.2lf %s",
                       "GPRINT:out:AVERAGE:Average\\:%8.2lf %s",
                       "GPRINT:out:MAX:Maximum\\:%8.2lf %s\\n",
                       "LINE1:drop#506101:Dropped",
                       "GPRINT:drop:LAST:   Current\\:%8.2lf %s",
                       "GPRINT:drop:AVERAGE:Average\\:%8.2lf %s",
                       "GPRINT:drop:MAX:Maximum\\:%8.2lf %s\\n"])

        if self.graph_topics:
            for _, w in self.workers.items():
                #worker_time_started = datetime.now()
                rrdtool.graph(["%s/%s.png" % (self.graph_dir, w.collname),
                               "--start=-600", "--end=-10",
                               "--disable-rrdtool-tag", "--width=560",
                               "--font", "LEGEND:10:", "--font", "UNIT:8:",
                               "--font", "TITLE:12:", "--font", "AXIS:8:",
                               "--title=%s" % w.topic,
                               "--vertical-label=messages/sec",
                               "--slope-mode"]
                              + (self.graph_daemon and self.graph_daemon_args or []) +
                              ["DEF:qsize=%s/%s.rrd:qsize:AVERAGE:step=10" % (self.graph_dir, w.collname),
                               "DEF:in=%s/%s.rrd:in:AVERAGE:step=10" % (self.graph_dir, w.collname),
                               "DEF:out=%s/%s.rrd:out:AVERAGE:step=10" % (self.graph_dir, w.collname),
                               "DEF:drop=%s/%s.rrd:drop:AVERAGE:step=10" % (self.graph_dir, w.collname),
                               "LINE1:qsize#FF7200:Queue Size",
                               "GPRINT:qsize:LAST:Current\\:%8.2lf %s",
                               "GPRINT:qsize:AVERAGE:Average\\:%8.2lf %s",
                               "GPRINT:qsize:MAX:Maximum\\:%8.2lf %s\\n",
                               "LINE1:in#503001:In",
                               "GPRINT:in:LAST:        Current\\:%8.2lf %s",
                               "GPRINT:in:AVERAGE:Average\\:%8.2lf %s",
                               "GPRINT:in:MAX:Maximum\\:%8.2lf %s\\n",
                               "LINE1:out#EDAC00:Out",
                               "GPRINT:out:LAST:       Current\\:%8.2lf %s",
                               "GPRINT:out:AVERAGE:Average\\:%8.2lf %s",
                               "GPRINT:out:MAX:Maximum\\:%8.2lf %s\\n",
                               "LINE1:drop#506101:Dropped",
                               "GPRINT:drop:LAST:   Current\\:%8.2lf %s",
                               "GPRINT:drop:AVERAGE:Average\\:%8.2lf %s",
                               "GPRINT:drop:MAX:Maximum\\:%8.2lf %s\\n"])

                #worker_time_elapsed = datetime.now() - worker_time_started
                #print("Generated worker graph for %s, took %s" % (w.topic, worker_time_elapsed))


        rrdtool.graph(["%s/logmemory.png" % self.graph_dir,
                       "--start=-600", "--end=-10",
                       "--disable-rrdtool-tag", "--width=560",
                       "--font", "LEGEND:10:", "--font", "UNIT:8:",
                       "--font", "TITLE:12:", "--font", "AXIS:8:",
                       "--title=ROS MongoLog Memory Usage",
                       "--vertical-label=bytes",
                       "--slope-mode"]
                      + (self.graph_daemon and self.graph_daemon_args or []) +
                      ["DEF:size=%s/logmemory.rrd:size:AVERAGE:step=10" % self.graph_dir,
                       "DEF:rss=%s/logmemory.rrd:rss:AVERAGE:step=10" % self.graph_dir,
                       "AREA:size#FF7200:Total",
                       "GPRINT:size:LAST:   Current\\:%8.2lf %s",
                       "GPRINT:size:AVERAGE:Average\\:%8.2lf %s",
                       "GPRINT:size:MAX:Maximum\\:%8.2lf %s\\n",
                       "AREA:rss#503001:Resident",
                       "GPRINT:rss:LAST:Current\\:%8.2lf %s",
                       "GPRINT:rss:AVERAGE:Average\\:%8.2lf %s",
                       "GPRINT:rss:MAX:Maximum\\:%8.2lf %s\\n"])
        time_elapsed = datetime.now() - time_started
        print("Generated graphs, took %s" % time_elapsed)

    def init_rrd(self):
        self.assert_rrd("%s/logstats.rrd" % self.graph_dir,
                        "DS:qsize:GAUGE:30:0:U",
                        "DS:in:COUNTER:30:0:U",
                        "DS:out:COUNTER:30:0:U",
                        "DS:drop:COUNTER:30:0:U")

        self.assert_rrd("%s/logmemory.rrd" % self.graph_dir,
                        "DS:size:GAUGE:30:0:U",
                        "DS:rss:GAUGE:30:0:U",
                        "DS:stack:GAUGE:30:0:U")

        self.graph_args = []
        if self.graph_daemon:
            self.graph_sockfile = mktemp(prefix="rrd_", suffix=".sock")
            self.graph_pidfile  = mktemp(prefix="rrd_", suffix=".pid")
            print("Starting rrdcached -l unix:%s -p %s -b %s -g" %
                  (self.graph_sockfile,self.graph_pidfile, self.graph_dir))
            devnull = file('/dev/null', 'a+')
            self.graph_process = subprocess.Popen(["/usr/bin/rrdcached",
                                                   "-l", "unix:%s" % self.graph_sockfile,
                                                   "-p", self.graph_pidfile,
                                                   "-b", self.graph_dir,
                                                   "-g"], stderr=subprocess.STDOUT)
            self.graph_daemon_args = ["--daemon", "unix:%s" % self.graph_sockfile]

    def assert_worker_rrd(self, collname):
        self.assert_rrd("%s/%s.rrd" % (self.graph_dir, collname),
                        "DS:qsize:GAUGE:30:0:U",
                        "DS:in:COUNTER:30:0:U",
                        "DS:out:COUNTER:30:0:U",
                        "DS:drop:COUNTER:30:0:U")


    def update_rrd(self):
        # we do not lock here, we are not interested in super-precise
        # values for this, but we do care for high performance processing
        qsize = 0
        #print("Updating graphs")
        time_started = datetime.now()
        for _, w in self.workers.items():
            wqsize = w.queue.qsize()
            qsize += wqsize
            if wqsize > QUEUE_MAXSIZE/2: print("Excessive queue size %6d: %s" % (wqsize, w.name))

            if self.graph_topics:
                rrdtool.update(["%s/%s.rrd" % (self.graph_dir, w.collname)]
                               + (self.graph_daemon and self.graph_daemon_args or []) +
                               ["N:%d:%d:%d:%d" %
                                (wqsize, w.worker_in_counter.count.value,
                                 w.worker_out_counter.count.value, w.worker_drop_counter.count.value)])

        rrdtool.update(["%s/logstats.rrd" % self.graph_dir]
                       + (self.graph_daemon and self.graph_daemon_args or []) +
                       ["N:%d:%d:%d:%d" %
                        (qsize, self.in_counter.count.value, self.out_counter.count.value,
                         self.drop_counter.count.value)])

        rrdtool.update(["%s/logmemory.rrd" % self.graph_dir]
                       + (self.graph_daemon and self.graph_daemon_args or []) +
                       ["N:%d:%d:%d" % self.get_memory_usage()])

        time_elapsed = datetime.now() - time_started
        print("Updated graphs, total queue size %d, dropped %d, took %s" %
              (qsize, self.drop_counter.count.value, time_elapsed))

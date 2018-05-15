#!/usr/bin/python

###########################################################################
#  mongodb_log.py - Python based ROS to MongoDB logger (multi-process)
#
#  Created: Sun Dec 05 19:45:51 2010
#  Copyright  2010-2012  Tim Niemueller [www.niemueller.de]
#             2010-2011  Carnegie Mellon University
#             2010       Intel Labs Pittsburgh
###########################################################################

#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Library General Public License for more details.
#
#  Read the full text in the LICENSE.GPL file in the doc directory.

# make sure we aren't using floor division
from __future__ import division, with_statement
from model.constants import PACKAGE_NAME, NODE_NAME_TEMPLATE
from model.mongoWriter import MongoWriter
import roslib; roslib.load_manifest(PACKAGE_NAME)
import sys
import socket
from optparse import OptionParser
import rosgraph.masterapi




def main(argv):
    parser = OptionParser()
    parser.usage += " [TOPICs...]"
    parser.add_option("-c", "--coll", dest="collection_name", type="string", help="Name of the collection", default="")
    parser.add_option("--nodename-prefix", dest="nodename_prefix",
                      help="Prefix for worker node names", metavar="ROS_NODE_NAME",
                      default="")
    parser.add_option("--mongodb-host", dest="mongodb_host",
                      help="Hostname of MongoDB", metavar="HOST",
                      default="172.17.0.2")
    parser.add_option("--mongodb-port", dest="mongodb_port",
                      help="Hostname of MongoDB", type="int",
                      metavar="PORT", default=27017)
    parser.add_option("--mongodb-name", dest="mongodb_name",
                      help="Name of DB in which to store values",
                      metavar="NAME", default="roslog")
    parser.add_option("-a", "--all-topics", dest="all_topics", default=False,
                      action="store_true",
                      help="Log all existing topics (still excludes /rosout, /rosout_agg)")
    parser.add_option("--all-topics-interval", dest="all_topics_interval", default=5,
                      help="Time in seconds between checks for new topics", type="int")
    parser.add_option("-x", "--exclude", dest="exclude",
                      help="Exclude topics matching REGEX, may be given multiple times",
                      action="append", type="string", metavar="REGEX", default=[])
    parser.add_option("--graph-topics", dest="graph_topics", default=False,
                      action="store_true",
                      help="Write graphs per topic")
    parser.add_option("--graph-clear", dest="graph_clear", default=False,
                      action="store_true",
                      help="Remove existing RRD files.")
    parser.add_option("--graph-dir", dest="graph_dir", default=".",
                      help="Directory in which to create the graphs")
    parser.add_option("--graph-daemon", dest="graph_daemon", default=False,
                      action="store_true",
                      help="Use rrddaemon.")
    parser.add_option("--no-specific", dest="no_specific", default=False,
                      action="store_true", help="Disable specific loggers")

    (options, args) = parser.parse_args()
    print options.collection_name
    #if not options.all_topics and len(args) == 0:
#        parser.print_help()
#        return

    try:
        rosgraph.masterapi.Master(NODE_NAME_TEMPLATE % options.nodename_prefix).getPid()
    except socket.error:
        print("Failed to communicate with master")

    mongowriter = MongoWriter(topics=["/tf"], graph_topics = options.graph_topics,
                              graph_dir = options.graph_dir,
                              graph_clear = options.graph_clear,
                              graph_daemon = options.graph_daemon,
                              all_topics=options.all_topics,
                              all_topics_interval = options.all_topics_interval,
                              exclude_topics = options.exclude,
                              mongodb_host=options.mongodb_host,
                              mongodb_port=options.mongodb_port,
                              mongodb_name=options.mongodb_name,
                              no_specific=options.no_specific,
                              nodename_prefix=options.nodename_prefix,
                              collection_name=options.collection_name)

    mongowriter.run()
    mongowriter.shutdown()


if __name__ == "__main__":
    main(sys.argv)

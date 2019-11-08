#!/usr/bin/python3

import datetime
import pandra as cql
from cassandra.cluster import Cluster

cluster = Cluster([
                   '127.0.0.1'
                   ])

key_space_name = "pandas_test"
cassandra_session = cluster.connect()

cassandra_session.execute("CREATE KEYSPACE IF NOT EXISTS pandas_test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
cassandra_session.execute("USE " + key_space_name)

cassandra_session.execute("DROP TABLE test_table")

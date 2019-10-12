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

test = [
    {'a': 1, 'b': 2, 'timestamp': 'placeholder'},
    {'a': 4, 'b': 7, 'timestamp': str(datetime.datetime.now())},
]

cql_df = cql.CassandraDataFrame.from_dict(test)

column_types = {
    'a': cql.IntegerType('a', primary_key=True),
    'b': cql.IntegerType('b'),
    'timestamp': cql.TextType('timestamp'),
}

cql_df.set_key_space("pandas_test")

cql_df.to_cassandra(cassandra_session=cassandra_session,
                    table_name='test_table',
                    data_types=column_types,
                    debug=False,
                    create_table=True)

#############

cdf = cql.CassandraDataFrame.from_cassandra(cassandra_session, 'SELECT * FROM test_table')

print(cdf)

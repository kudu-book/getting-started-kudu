#!/usr/bin/env python

import kudu
from kudu.client import Partitioning
from datetime import datetime
import sys

def initKudu(kuduMaster):
    # Connect to Kudu master server
    print ("Connecting to kudu [Kudu Master: {}]".format(kuduMaster))
    client = kudu.connect(host=kuduMaster, port=7051)
    return client

def executeCommand(client, command, tableName):
    print ("Executing Command {} on table {}".format(command, tableName))

    if command == "create":
        # Creating a table requires just a few steps
        # - Define your schema
        # - Define your partitioning scheme
        # - Call the create_table API

        # Use the schema_builder to build your table's schema
        builder = kudu.schema_builder()


        # Lastname column
        builder.add_column('lastname').type(
            'string').default('doe').compression('snappy').encoding(
            'plain').nullable(False)

        # State/Province the person lives in
        # Leave all defaults except for the type and nullability
        builder.add_column('state_prov').type('string').nullable(False)

        builder.add_column('key').type(
            kudu.int64).nullable(False)

        # We prefer using dot notation, so let's add a few more columns
        # using that strategy
        #  - type : We specify the string representation of types
        #  - default: Default value if none specified
        #  - compression: Compression type
        #  - encoding: Encoding strategy
        #  - nullable: Nullability
        #  - block_size: Target block size, overriding server defaults
        builder.add_column('firstname').type(
            'string').default('jane').compression('zlib').encoding(
            'plain').nullable(False).block_size(20971520)

        # Use add_column list of parameters to specify properties
        # just as an example instead of dot notation.
        builder.add_column('ts_val',
                           type_=kudu.unixtime_micros,
                           nullable=False, compression='lz4')

        # Set our primary key column(s)
        builder.set_primary_keys(['lastname', 'state_prov', 'key'])

        # Build the schema
        schema = builder.build()

        # Define Hash partitioned column by the state/province
        # Its quite possible the data would then be skewed across partitions
        # so what we'll do here is add a the optional 3rd parameter to
        # help randomize the mapping of rows to hash buckets.
        partitioning = Partitioning().add_hash_partitions(
            column_names=['state_prov'], num_buckets=3, seed=13)

        # We've hash partitioned according to the state, now let's further
        # range partition our content by lastname. If we wanted to find all
        # the "Smith" families in the state of Oregon, we would very quickly
        # be able to isolate those rows with this type of schema.
        # Set the range partition columns - these columns MUST be part of
        # the primary key columns.
        partitioning.set_range_partition_columns('lastname')
        # Add range partitions
        partitioning.add_range_partition(['A'], ['E'])
        # By default, lower bound is inclusive while upper is exclusive
        partitioning.add_range_partition(['E'], ['Z'],
                                         upper_bound_type='inclusive')

        # Create new table passing in the table name, schema, partitioning
        # object and the optional parameter of number of replicas for this
        # table. If none specified, then it'll go by the Kudu server default
        # value for number of replicas.
        client.create_table(tableName, schema, partitioning, 1)
    elif command == "insert":
        # Open a table
        table = client.table(tableName)

        # Create a new session so that we can apply write operations
        session = client.new_session()

        # We have a few flush modes at our disposal, namely:
        # FLUSH_MANUAL, FLUSH_AUTO_SYNC and FLUSH_AUTO_BACKGROUND
        # The default is FLUSH_MANUAL, and we want to flush manually for
        # our examples below. Just providing example on how to change it
        # needed.
        session.set_flush_mode(kudu.FLUSH_MANUAL)

        # We can set a timeout value as well in milliseconds. Set ours to
        # 3 seconds.
        session.set_timeout_ms(3000)

        # Insert a row
        op = table.new_insert({'lastname'  : 'Smith',
                               'state_prov': 'ON',
                               'firstname' : 'Mike',
                               'key'       : 1,
                               'ts_val'    : datetime.utcnow()})
        session.apply(op)
        op = table.new_insert({'lastname'  : 'Smith',
                               'state_prov': 'ON',
                               'firstname' : 'Mike',
                               'key'       : 1,
                               'ts_val'    : datetime.utcnow()})
        session.apply(op)
        op = table.new_insert({'lastname'  : 'Smith',
                               'state_prov': 'ON',
                               'firstname' : 'Mike',
                               'key'       : 1,
                               'ts_val'    : datetime.utcnow()})
        session.apply(op)
        try:
            session.flush()
        except kudu.KuduBadStatus as e:
            (errorResult, overflowed) = session.get_pending_errors()
            print("Insert row failed: {} (more pending errors? {})".format(
                errorResult, overflowed))

        # Upsert a row
        #op = table.new_upsert({'key': 2, 'ts_val': "2016-01-01T00:00:00.000000"})
        #session.apply(op)
        #
        ## Updating a row
        #op = table.new_update({'key': 1, 'ts_val': ("2017-01-01", "%Y-%m-%d")})
        #session.apply(op)
        #
        ## Delete a row
        #op = table.new_delete({'key': 2})
        #session.apply(op)
        #
        ## Flush write operations, if failures occur, capture print them.
        #try:
        #    session.flush()
        #except kudu.KuduBadStatus as e:
        #    print(session.get_pending_errors())
        #
        ## Create a scanner and add a predicate
        #scanner = table.scanner()
        #scanner.add_predicate(table['ts_val'] == datetime(2017, 1, 1))
        #
        ## Open Scanner and read all tuples
        ## Note: This doesn't scale for large scans
        #result = scanner.open().read_all_tuples()

def printUsage(scriptName):
    print ("Usage: {} <kudu-master> <command> <tableName>".format(scriptName))
    print ("   where:")
    print ("   <command> : One of 'create', 'dml' (insert/update/delete/etc), 'delete'")
    sys.exit(1)

if __name__ == "__main__":
    scriptName = sys.argv[0]
    if (len(sys.argv) != 4):
        printUsage(scriptName)
    kuduMaster = sys.argv[1]
    command    = sys.argv[2]
    tableName  = sys.argv[3]
    kuduClient = initKudu(kuduMaster)
    executeCommand (kuduClient, command, tableName)
    sys.exit(0)

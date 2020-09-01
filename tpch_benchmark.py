import mysql_client as myclient
import time
import os
import random as rnd
import pandas as pd
import numpy as np
import multiprocessing
import threading
import math

class Benchmark:
    """
    Creates a new TPC-H benchmark instance. If necessary, it also makes use of DBGEN to generate TPC-H dummy data.

    Attributes
    ----------

    mysql_tpch : mysql_client
        A mysql_client object containing all the connection setup for the TPC-H database;
    sf : float
        SF values are related to the database size. For instance: A SF of 1 will generate 1GB of dummy data, while a SF of 0.1 will generate 100MB of dumy data.
    dbgen : boolean
        When this argument is True, dbgen is called to generate 'SF' GB of dummy data. 

    """
    __load_test_time = 0
    __power_test_time = 0
    __tables = ["customer", "orders", "lineitem", "nation", "partsupp", "part", "region", "supplier"]

    def __init__(self, mysql_tpch, sf = 1, dbgen=False):
        self.__sf = sf
        self.__connection = mysql_tpch

        r = self.__connection.connect_database(self.__connection.database_name)

        if dbgen:
            print("Generating {0}GB of dummy data...\n".format(sf))
            os.chdir("dbgen")
            os.system("sudo ./dbgen -s {0}".format(sf))
            os.chdir("../")

        if r == False:
            self.__connection.run_command("CREATE DATABASE {0}".format(self.__connection.database_name))
            self.__connection.run_command("USE {0}".format(self.__connection.database_name))
    
    def __create_tables(self):
        commands = [
            "CREATE TABLE NATION (N_NATIONKEY INTEGER NOT NULL, N_NAME CHAR(25) NOT NULL, N_REGIONKEY INTEGER NOT NULL, N_COMMENT VARCHAR(152))",
            "CREATE TABLE REGION (R_REGIONKEY INTEGER NOT NULL, R_NAME CHAR(25) NOT NULL, R_COMMENT VARCHAR(152))",
            "CREATE TABLE PART (P_PARTKEY INTEGER NOT NULL, P_NAME VARCHAR(55) NOT NULL, P_MFGR CHAR(25) NOT NULL, P_BRAND CHAR(10) NOT NULL, P_TYPE VARCHAR(25) NOT NULL, P_SIZE INTEGER NOT NULL, P_CONTAINER CHAR(10) NOT NULL, P_RETAILPRICE DECIMAL(15,2) NOT NULL, P_COMMENT VARCHAR(23) NOT NULL)",
            "CREATE TABLE SUPPLIER (S_SUPPKEY INTEGER NOT NULL, S_NAME CHAR(25) NOT NULL, S_ADDRESS VARCHAR(40) NOT NULL, S_NATIONKEY INTEGER NOT NULL, S_PHONE CHAR(15) NOT NULL, S_ACCTBAL DECIMAL(15,2) NOT NULL, S_COMMENT VARCHAR(101) NOT NULL)",
            "CREATE TABLE PARTSUPP (PS_PARTKEY INTEGER NOT NULL, PS_SUPPKEY INTEGER NOT NULL, PS_AVAILQTY INTEGER NOT NULL, PS_SUPPLYCOST DECIMAL(15,2) NOT NULL, PS_COMMENT VARCHAR(199) NOT NULL)",
            "CREATE TABLE CUSTOMER (C_CUSTKEY INTEGER NOT NULL, C_NAME VARCHAR(25) NOT NULL, C_ADDRESS VARCHAR(40) NOT NULL, C_NATIONKEY INTEGER NOT NULL, C_PHONE CHAR(15) NOT NULL, C_ACCTBAL DECIMAL(15,2) NOT NULL, C_MKTSEGMENT CHAR(10) NOT NULL, C_COMMENT VARCHAR(117) NOT NULL)",
            "CREATE TABLE ORDERS (O_ORDERKEY BIGINT UNSIGNED NOT NULL, O_CUSTKEY INTEGER NOT NULL, O_ORDERSTATUS CHAR(1) NOT NULL, O_TOTALPRICE DECIMAL(15,2) NOT NULL, O_ORDERDATE DATE NOT NULL, O_ORDERPRIORITY CHAR(15) NOT NULL, O_CLERK CHAR(15) NOT NULL, O_SHIPPRIORITY INTEGER NOT NULL, O_COMMENT VARCHAR(79) NOT NULL)",
            "CREATE TABLE LINEITEM (L_ORDERKEY BIGINT UNSIGNED NOT NULL, L_PARTKEY INTEGER NOT NULL, L_SUPPKEY INTEGER NOT NULL, L_LINENUMBER INTEGER NOT NULL, L_QUANTITY DECIMAL(15,2) NOT NULL, L_EXTENDEDPRICE DECIMAL(15,2) NOT NULL, L_DISCOUNT DECIMAL(15,2) NOT NULL, L_TAX DECIMAL(15,2) NOT NULL, L_RETURNFLAG CHAR(1) NOT NULL, L_LINESTATUS CHAR(1) NOT NULL, L_SHIPDATE DATE NOT NULL, L_COMMITDATE DATE NOT NULL, L_RECEIPTDATE DATE NOT NULL, L_SHIPINSTRUCT CHAR(25) NOT NULL, L_SHIPMODE CHAR(10) NOT NULL, L_COMMENT VARCHAR(44) NOT NULL)",
        ]
        try:
            for i in range(len(commands)):
                if i < 8: print("\nCreating table {0}".format(self.__tables[i]))
                self.__connection.run_command(commands[i])
            print("Tables created successfully!")

        except:
            print("An error occurred when creating tables.") 
        
    def __load_data(self):        
        try:           
            for t in self.__tables:
                print("\nLoading table {0}".format(t))
                s = "LOAD DATA LOCAL INFILE 'dbgen/{0}.tbl' INTO TABLE {1} FIELDS TERMINATED BY '|';".format(t, t.upper())
                print("Running {0}".format(s))
                self.__connection.run_command(s)
            print("Tables loaded successfully!\n")

        except Exception as e:
            print("An error occurred when loading tables:\n{0}".format(e))

    def __alter_tables(self):
        commands = [
            "CREATE PROCEDURE `refresh_function1`(sf float) BEGIN declare iterations1 int default 0; declare iterations2 int default 0; declare i int default 0; declare j int default 0; declare total decimal (15,2) default 20000.00; set iterations1 = floor(1500 * sf); START TRANSACTION; while i < iterations1 do insert into ORDERS (O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT) values (7801, 'O', total, '1996-12-06', '5-LOW', 'Clerk#000000616', 0, 'ly special requests'); insert into LINEITEM (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) values ((select O_ORDERKEY from ORDERS where O_CUSTKEY = 7801 AND O_ORDERSTATUS = 'O' AND O_TOTALPRICE = total AND O_ORDERDATE = '1996-12-06' AND O_ORDERPRIORITY = '5-LOW' AND O_CLERK = 'Clerk#000000616' AND O_SHIPPRIORITY = 0 AND O_COMMENT = 'ly special requests' LIMIT 1), 15519, 785, 1, 32.00, 12569.41, 0.08, 0.03, 'N', 'O', '1998-07-27', '1998-08-22', '1998-08-26', 'NONE', 'TRUCK', 'riously. regular, express dep'); set j = j + 1; set i = i + 1; set total = total + 0.01; end while; COMMIT; END",
            "CREATE PROCEDURE `refresh_function2`(sf float) BEGIN declare i int default 1; declare iterations int default floor(1500 * sf); declare o_key bigint default 0; START TRANSACTION; while i <= iterations do set o_key = (select O_ORDERKEY from ORDERS where O_CUSTKEY = 7801 AND O_ORDERSTATUS = 'O' AND O_ORDERDATE = '1996-12-06' AND O_ORDERPRIORITY = '5-LOW' AND O_CLERK = 'Clerk#000000616' AND O_SHIPPRIORITY = 0 AND O_COMMENT = 'ly special requests' LIMIT 1); delete from LINEITEM where L_ORDERKEY = o_key; delete from ORDERS where O_ORDERKEY = o_key; set i = i + 1; end while; COMMIT; END", 
            "ALTER TABLE REGION ADD PRIMARY KEY (R_REGIONKEY)",
            "ALTER TABLE NATION ADD PRIMARY KEY (N_NATIONKEY)",
            "ALTER TABLE ORDERS MODIFY O_ORDERKEY BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY",
            "ALTER TABLE NATION ADD FOREIGN KEY NATION_FK1 (N_REGIONKEY) references REGION(R_REGIONKEY) ON DELETE CASCADE ON UPDATE CASCADE",
            "ALTER TABLE PART ADD PRIMARY KEY (P_PARTKEY)",
            "ALTER TABLE SUPPLIER ADD PRIMARY KEY (S_SUPPKEY)",
            "ALTER TABLE SUPPLIER ADD FOREIGN KEY SUPPLIER_FK1 (S_NATIONKEY) references NATION(N_NATIONKEY) ON DELETE CASCADE ON UPDATE CASCADE",
            "ALTER TABLE PARTSUPP ADD PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY)",
            "ALTER TABLE CUSTOMER ADD PRIMARY KEY (C_CUSTKEY)",
            "ALTER TABLE CUSTOMER ADD FOREIGN KEY CUSTOMER_FK1 (C_NATIONKEY) references NATION(N_NATIONKEY) ON DELETE CASCADE ON UPDATE CASCADE",
            "ALTER TABLE LINEITEM ADD PRIMARY KEY (L_ORDERKEY,L_LINENUMBER)",
            "ALTER TABLE PARTSUPP ADD FOREIGN KEY PARTSUPP_FK1 (PS_SUPPKEY) references SUPPLIER(S_SUPPKEY) ON DELETE CASCADE ON UPDATE CASCADE",
            "ALTER TABLE PARTSUPP ADD FOREIGN KEY PARTSUPP_FK2 (PS_PARTKEY) references PART(P_PARTKEY) ON DELETE CASCADE ON UPDATE CASCADE",
            "ALTER TABLE ORDERS ADD FOREIGN KEY ORDERS_FK1 (O_CUSTKEY) references CUSTOMER(C_CUSTKEY) ON DELETE CASCADE ON UPDATE CASCADE",
            "ALTER TABLE LINEITEM ADD FOREIGN KEY LINEITEM_FK1 (L_ORDERKEY) references ORDERS(O_ORDERKEY) ON DELETE CASCADE ON UPDATE CASCADE",
            "ALTER TABLE LINEITEM ADD FOREIGN KEY LINEITEM_FK2 (L_PARTKEY,L_SUPPKEY) references PARTSUPP(PS_PARTKEY, PS_SUPPKEY) ON DELETE CASCADE ON UPDATE CASCADE",
        ]

        try: 
            for c in commands:
                print("\nRunning command {0}".format(c))
                self.__connection.run_command(c)
            print("Tables altered successfully!")
        except Exception as e:
            print("An error occurred when altering the tables:\n{0}".format(e))

    def __compute_power_size_metric(self):
        total_num_factors = len(self.__pwrtest_query_times) + len(self.__pwrtest_refresh_times)
        query_times_prod = np.prod(self.__pwrtest_query_times)
        refresh_times_prod = np.prod(self.__pwrtest_refresh_times)
        denominator = math.pow(query_times_prod * refresh_times_prod, 1/total_num_factors)

        return ((3600 * self.__sf) / denominator)

    def __compute_throughput_size_metric(self, num_query_streams, queries_per_stream, throughput_test_time):
        return ((num_query_streams * queries_per_stream) / throughput_test_time) * 3600 * self.__sf

    def load_benchmark(self):
        print("--- BEGINING OF LOAD TEST ---")
        self.__load_test_time = time.time()
        self.__create_tables()
        self.__load_data()
        self.__alter_tables()
        self.__load_test_time = time.time() - self.__load_test_time

    def power_benchmark(self):
        print("\n--- BEGINING OF POWER TEST ---")
        self.__pwrtest_query_times = []
        self.__pwrtest_refresh_times = []
        self.__df_queries_idxs = pd.read_csv("queries/queries_rnd_idxs.csv", header=None, delim_whitespace=True)
        idxs = self.__df_queries_idxs.loc[[0]] # The index '0' refers to 'Query Stream 00', according to TPC-H nomenclature.
        print(len(idxs.loc[0].values))
        
        i = 1
        running_time = time.time()
        self.__power_test_time = running_time

        self.__connection.run_command("call refresh_function1({0});".format(self.__sf))
        self.__pwrtest_refresh_times.append(time.time() - running_time)
        print("\nRefresh Function 2 finished after {0:.5} seconds.".format(self.__pwrtest_refresh_times[0]))

        # while i <= len(idxs.loc[0].values):
        while i <= 5:
            print("\nRUNNING QUERY {0}\n".format(idxs[i-1].values[0]))
            sql = open("queries/{0}.sql".format(idxs[i-1].values[0]), 'r').read().split(';')[0]
            
            running_time = time.time()
            self.__connection.run_command(sql)
            self.__pwrtest_query_times.append(time.time() - running_time)

            print("\nQuery {0} finished after {1:.5} seconds.".format(idxs[i-1].values[0], self.__pwrtest_query_times[i-1]))
            i = i + 1
            
        running_time = time.time()
        self.__connection.run_command("call refresh_function2({0});".format(self.__sf))
        self.__pwrtest_refresh_times.append(time.time() - running_time)

        print("\nRefresh Function 2 finished after {0:.5} seconds.".format(self.__pwrtest_refresh_times[1]))

        self.__power_test_time = time.time() - self.__power_test_time
        self.__connection.close()

    def throughput_benchmark(self):
        print("\n--- BEGINING OF THROUGHPUT TEST ---")
        num_query_streams = 0

        if   self.__sf < 10:                            num_query_streams = 2
        elif self.__sf >= 10 and self.__sf < 30:        num_query_streams = 3
        elif self.__sf >= 30 and self.__sf < 100:       num_query_streams = 4
        elif self.__sf >= 100 and self.__sf < 300:      num_query_streams = 5
        elif self.__sf >= 300 and self.__sf < 1000:     num_query_streams = 6
        elif self.__sf >= 1000 and self.__sf < 3000:    num_query_streams = 7
        elif self.__sf >= 3000 and self.__sf < 10000:   num_query_streams = 8
        elif self.__sf >= 10000 and self.__sf < 30000:  num_query_streams = 9
        elif self.__sf >= 30000 and self.__sf < 100000: num_query_streams = 10
        elif self.__sf >= 100000:                       num_query_streams = 11

        rnd_query_streams_idxs = rnd.sample(range(1, len(self.__df_queries_idxs)), num_query_streams)         
        processes = []

        self.__throughput_test_time = time.time()
        
        for i in range(len(rnd_query_streams_idxs)): # Runs all the Query Streams in parallel by using the helper function '__parallel_query_running'.
            p = multiprocessing.Process(target=self.__parallel_query_running, args=(rnd_query_streams_idxs[i],))
            p.start()
            processes.append(p)
        
        for p in processes:
            p.join()
        
        self.__throughput_test_time = time.time() - self.__throughput_test_time

        power_metric =      self.__compute_power_size_metric()
        throughput_metric = self.__compute_throughput_size_metric(num_query_streams, len(self.__pwrtest_query_times), self.__throughput_test_time)

        print("\n--- LOAD TEST TOTAL TIME: {0:.5} seconds. ---".format(self.__load_test_time))
        print("--- POWER TEST TOTAL TIME: {0:.5} seconds. ---".format(self.__power_test_time))
        print("--- POWER@SIZE METRIC: {0:.5} ---".format(power_metric))
        print("--- THROUGHPUT TEST TOTAL TIME: {0:.5} seconds. ---".format(self.__throughput_test_time))
        print("--- THROUGHPUT@SIZE METRIC: {0:.5} ---".format(throughput_metric))

        qphH = math.pow((power_metric * throughput_metric), 0.5)
        print("--- QphH@SIZE METRIC: {0:.5} ---\n".format(qphH))

    def __parallel_query_running(self, query_stream):
        indexes_per_query_stream = self.__df_queries_idxs.loc[query_stream, :len(self.__pwrtest_query_times)-1].values
        print("Running queries {0} of Query Stream {1}".format(indexes_per_query_stream, query_stream+1))

        con = self.__connection.getPoolConnection()
        cursor = con.cursor()   

        for i in range(len(indexes_per_query_stream)):
            sql = open("queries/{0}.sql".format(indexes_per_query_stream[i]), 'r').read().split(';')[0]

            running_time = time.time()
            self.__connection.run_command(sql, cursor)
            running_time = time.time() - running_time

            print("\n--- Query {0} of Query Stream {1} finished after {2:.5} seconds ---\n".format(indexes_per_query_stream[i], query_stream+1, running_time))

        print("\nRunning Refresh Function 1 of Query Stream {0}\n".format(query_stream+1))
        self.__connection.run_command("call refresh_function1({0})".format(self.__sf), cursor)
        print("\nRunning Refresh Function 2 of Query Stream {0}\n".format(query_stream+1))
        self.__connection.run_command("call refresh_function2({0})".format(self.__sf), cursor)        

        cursor.close()
        con.close()



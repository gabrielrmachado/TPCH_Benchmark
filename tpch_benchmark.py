import mysql_client as myclient
import time
from os import system
from os import chdir

class Benchmark:
    __load_test_time = 0
    __power_test_time = 0
    __tables = ["customer", "orders", "lineitem", "nation", "partsupp", "part", "region", "supplier"]

    def __init__(self, mysql_tpch, sf = 0):
        """
        Creates a new TPC-H benchmark instance. If necessary, it also makes use of DBGEN to generate TPC-H dummy data.

        Attributes
        ----------

        mysql_tpch : mysql_client
            A mysql_client object containing all the connection setup for the TPC-H database;
        sf : float
            If a value different from 0 is provided, DBGEN is summoned to generate dummy data.
            SF values are related to the database size. For instance: A SF of 1 will generate 1GB of dummy data, while a SF of 0.1 will generate 100MB of dumy data.

        """
        self.__sf = sf
        self.__database = mysql_tpch

        r = self.__database.connect_database(self.__database.database_name)

        if sf != 0:
            print("Generating {0}GB of dummy data...\n".format(sf))
            chdir("dbgen")
            system("sudo ./dbgen -s {0}".format(sf))

        if not r:
            self.__database.run_command("CREATE DATABASE {0}".format(self.__database.database_name))
            self.__database.run_command("USE {0}".format(self.__database.database_name))
    
    def __create_tables(self):
        commands = [
            "CREATE TABLE NATION (N_NATIONKEY INTEGER NOT NULL, N_NAME CHAR(25) NOT NULL, N_REGIONKEY INTEGER NOT NULL, N_COMMENT VARCHAR(152))",
            "CREATE TABLE REGION (R_REGIONKEY INTEGER NOT NULL, R_NAME CHAR(25) NOT NULL, R_COMMENT VARCHAR(152))",
            "CREATE TABLE PART (P_PARTKEY INTEGER NOT NULL, P_NAME VARCHAR(55) NOT NULL, P_MFGR CHAR(25) NOT NULL, P_BRAND CHAR(10) NOT NULL, P_TYPE VARCHAR(25) NOT NULL, P_SIZE INTEGER NOT NULL, P_CONTAINER CHAR(10) NOT NULL, P_RETAILPRICE DECIMAL(15,2) NOT NULL, P_COMMENT VARCHAR(23) NOT NULL)",
            "CREATE TABLE SUPPLIER (S_SUPPKEY INTEGER NOT NULL, S_NAME CHAR(25) NOT NULL, S_ADDRESS VARCHAR(40) NOT NULL, S_NATIONKEY INTEGER NOT NULL, S_PHONE CHAR(15) NOT NULL, S_ACCTBAL DECIMAL(15,2) NOT NULL, S_COMMENT VARCHAR(101) NOT NULL)",
            "CREATE TABLE PARTSUPP (PS_PARTKEY INTEGER NOT NULL, PS_SUPPKEY INTEGER NOT NULL, PS_AVAILQTY INTEGER NOT NULL, PS_SUPPLYCOST DECIMAL(15,2) NOT NULL, PS_COMMENT VARCHAR(199) NOT NULL)",
            "CREATE TABLE CUSTOMER (C_CUSTKEY INTEGER NOT NULL, C_NAME VARCHAR(25) NOT NULL, C_ADDRESS VARCHAR(40) NOT NULL, C_NATIONKEY INTEGER NOT NULL, C_PHONE CHAR(15) NOT NULL, C_ACCTBAL DECIMAL(15,2) NOT NULL, C_MKTSEGMENT CHAR(10) NOT NULL, C_COMMENT VARCHAR(117) NOT NULL)",
            "CREATE TABLE ORDERS (O_ORDERKEY INTEGER NOT NULL, O_CUSTKEY INTEGER NOT NULL, O_ORDERSTATUS CHAR(1) NOT NULL, O_TOTALPRICE DECIMAL(15,2) NOT NULL, O_ORDERDATE DATE NOT NULL, O_ORDERPRIORITY CHAR(15) NOT NULL, O_CLERK CHAR(15) NOT NULL, O_SHIPPRIORITY INTEGER NOT NULL, O_COMMENT VARCHAR(79) NOT NULL)",
            "CREATE TABLE LINEITEM (L_ORDERKEY INTEGER NOT NULL, L_PARTKEY INTEGER NOT NULL, L_SUPPKEY INTEGER NOT NULL, L_LINENUMBER INTEGER NOT NULL, L_QUANTITY DECIMAL(15,2) NOT NULL, L_EXTENDEDPRICE DECIMAL(15,2) NOT NULL, L_DISCOUNT DECIMAL(15,2) NOT NULL, L_TAX DECIMAL(15,2) NOT NULL, L_RETURNFLAG CHAR(1) NOT NULL, L_LINESTATUS CHAR(1) NOT NULL, L_SHIPDATE DATE NOT NULL, L_COMMITDATE DATE NOT NULL, L_RECEIPTDATE DATE NOT NULL, L_SHIPINSTRUCT CHAR(25) NOT NULL, L_SHIPMODE CHAR(10) NOT NULL, L_COMMENT VARCHAR(44) NOT NULL)",
        ]
        try:
            for i in range(len(commands)):
                if i < 8: print("Creating table {0}".format(self.__tables[i]))
                self.__database.run_command(commands[i])
            print("Tables created successfully!")

        except:
            print("An error occurred when creating tables.") 
        
    def __load_data(self):        
        try:           
            for t in self.__tables:
                print("Loading table {0}".format(t))
                s = "LOAD DATA LOCAL INFILE 'tpch_data/{0}.tbl' INTO TABLE {1} FIELDS TERMINATED BY '|';".format(t, t.upper())
                print("Running {0}".format(s))
                self.__database.run_command(s)
            print("Tables loaded successfully!\n")

        except Exception as e:
            print("An error occurred when loading tables:\n{0}".format(e))

    def __alter_tables(self):
        commands = [
            "CREATE PROCEDURE `refresh_function1` (sf float) BEGIN declare iterations1 int default 0; declare iterations2 int default 0; declare i int default 0; declare j int default 0; set iterations1 = floor(1500 * sf); while i < iterations1 do UPDATE ORDERS SET O_ORDERSTATUS = 'O', O_TOTALPRICE = 20000.00, O_ORDERDATE = '1996-12-06', O_ORDERPRIORITY = '5-LOW', O_CLERK = 'Clerk#000000616', O_SHIPPRIORITY = 0, O_COMMENT = 'ly special requests' WHERE O_ORDERKEY = FLOOR(RAND()* (140000-1)+1); set iterations2 = floor(RAND() * (7-1)+1); while j < iterations2 do UPDATE CUSTOMER SET C_NAME = 'Customer#00000000663', C_ADDRESS = 'IVhzIApeRb ot,c,E', C_NATIONKEY = 12, C_PHONE = '25-989-741-2988', C_ACCTBAL = 711.56, C_MKTSEGMENT = 'BUILDING', C_COMMENT = 'to the even, regular platelets. regular, ironic epitaphs nag e' where C_CUSTKEY = FLOOR(RAND()* (14000-1)+1); set j = j + 1; end while; set i = i + 1; end while; END",
            "CREATE PROCEDURE `refresh_function2`(sf float) BEGIN declare i int default 1; declare iterations int default floor(1500 * sf); while i < iterations do delete from LINEITEM where L_ORDERKEY = i; delete from ORDERS where O_ORDERKEY = i; set i = i + 1; end while; END;",
            "ALTER TABLE REGION ADD PRIMARY KEY (R_REGIONKEY)",
            "ALTER TABLE NATION ADD PRIMARY KEY (N_NATIONKEY)",
            "ALTER TABLE ORDERS ADD PRIMARY KEY (O_ORDERKEY)",
            "ALTER TABLE NATION ADD FOREIGN KEY NATION_FK1 (N_REGIONKEY) references REGION(R_REGIONKEY)",
            "ALTER TABLE PART ADD PRIMARY KEY (P_PARTKEY)",
            "ALTER TABLE SUPPLIER ADD PRIMARY KEY (S_SUPPKEY)",
            "ALTER TABLE SUPPLIER ADD FOREIGN KEY SUPPLIER_FK1 (S_NATIONKEY) references NATION(N_NATIONKEY)",
            "ALTER TABLE PARTSUPP ADD PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY)",
            "ALTER TABLE CUSTOMER ADD PRIMARY KEY (C_CUSTKEY)",
            "ALTER TABLE CUSTOMER ADD FOREIGN KEY CUSTOMER_FK1 (C_NATIONKEY) references NATION(N_NATIONKEY)",
            "ALTER TABLE LINEITEM ADD PRIMARY KEY (L_ORDERKEY,L_LINENUMBER)",
            "ALTER TABLE PARTSUPP ADD FOREIGN KEY PARTSUPP_FK1 (PS_SUPPKEY) references SUPPLIER(S_SUPPKEY)",
            "ALTER TABLE PARTSUPP ADD FOREIGN KEY PARTSUPP_FK2 (PS_PARTKEY) references PART(P_PARTKEY)",
            "ALTER TABLE ORDERS ADD FOREIGN KEY ORDERS_FK1 (O_CUSTKEY) references CUSTOMER(C_CUSTKEY)",
            "ALTER TABLE LINEITEM ADD FOREIGN KEY LINEITEM_FK1 (L_ORDERKEY)  references ORDERS(O_ORDERKEY)",
            "ALTER TABLE LINEITEM ADD FOREIGN KEY LINEITEM_FK2 (L_PARTKEY,L_SUPPKEY) references PARTSUPP(PS_PARTKEY, PS_SUPPKEY)",
        ]

        try: 
            for c in commands:
                print("Running command {0}".format(c))
                self.__database.run_command(c)
            print("Tables altered successfully!")
        except Exception as e:
            print("An error occurred when altering the tables:\n{0}".format(e))

    def load_benchmark(self):
        self.__load_test_time = time.time()
        self.__create_tables()
        self.__load_data()
        self.__alter_tables()
        self.__load_test_time = time.time() - self.__load_test_time
        print("\n--- Total Load Time: {0:5} seconds ---".format(self.__load_test_time))

    def power_benchmark(self, sf):
        self.__power_test_time = time.time()
        
        i = 1
        self.__database.run_command("call refresh_function1({0});".format(sf))

        while i <= 22:
            print("Running Query {0}\n".format(i))
            sql = open("queries/{0}.sql".format(i), 'r').read().split(';')[0]
            self.__database.run_command(sql)
            i = i + 1
            
        self.__database.run_command("call refresh_function2({0});".format(sf))

        self.__power_test_time = time.time() - self.__power_test_time
        print("\n--- Total Load Time: {0:5} seconds ---".format(self.__power_test_time))
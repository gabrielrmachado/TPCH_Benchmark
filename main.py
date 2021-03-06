import mysql_client as myclient
import tpch_benchmark as tpch
import random as rnd

if __name__ == "__main__":
    mysql = myclient.MySQL_TPCH("localhost", "user", "123456", "tpch100mb")
    benchmark = tpch.Benchmark(mysql, sf=0.1, dbgen=True)
    benchmark.load_benchmark()
    benchmark.power_benchmark()
    benchmark.throughput_benchmark()
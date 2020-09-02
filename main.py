import mysql_client as myclient
import tpch_benchmark as tpch
import random as rnd


if __name__ == "__main__":
    mysql = myclient.MySQL_TPCH("localhost", "user", "123456", "tpch100mb", 0.1)
    benchmark = tpch.Benchmark(mysql, dbgen=False)
    # benchmark.load_benchmark()
    benchmark.power_benchmark()
    benchmark.throughput_benchmark()
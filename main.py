import mysql_client as myclient
import tpch_benchmark as tpch

if __name__ == "__main__":
    mysql = myclient.MySQL_TPCH("localhost", "user", "123456")
    benchmark = tpch.Benchmark(mysql, "tpch100mb")
    # benchmark.load_benchmark()
    benchmark.power_benchmark()
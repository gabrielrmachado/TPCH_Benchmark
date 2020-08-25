import mysql_client as myclient
import tpch_benchmark as tpch


if __name__ == "__main__":
    mysql = myclient.MySQL_TPCH("localhost", "user", "123456", "tpch100mb")
    benchmark = tpch.Benchmark(mysql, 0.1)
    benchmark.load_benchmark()
    # benchmark.power_benchmark()
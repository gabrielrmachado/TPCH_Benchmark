# The TPC-H Benchmark for MySQL Database

*The full Python script for running the TPC-H benchmark on MySQL.*

## 1. Prerequisites

### 1.1. Create a new MySQL user.

In Linux terminal, run the commands below in order to create the test user.

```bash
sudo mysql -u root
CREATE USER 'user'@'localhost' IDENTIFIED BY '123456';
GRANT ALL PRIVILEGES ON * . * TO 'user'@'localhost';
```

### 1.2. Install dependencies.

Check whether the following libraries are already installed in you machine. If not, run the commands in Linux terminal in order to install them. In case of Python3 versions, run *pip3* instead of *pip*.

##### [MySQL connector for Python](https://dev.mysql.com/doc/connector-python/en/connector-python-installation-binary.html)
```bash
pip install mysql-connector-python
```
##### [Pandas](https://pandas.pydata.org)
```bash
pip install pandas
```
##### [NumPy](https://numpy.org)
```bash
pip install numpy
```
## Set the `sf` parameter for DBGEN generates dummy data

In `main.py`, make sure the parameter `dbgen` is set to `True` and assign a float value for `sf`, which indicates the amount of dummy data will be generated by DBGEN. By default, `sf = 0.1`, which means that DBGEN will generate 100MB of data. This is for sript testing purposes only. The oficial [TPC-H documentation](TPC-H documentation/tpc-h_v2.18.0.pdf) specifies a minimum data size of 1GB for running the benchmark.

### Running the script

After installing the dependecies, the script is ready to run. In the project directory, open the terminal and run the following command: `python main.py`.
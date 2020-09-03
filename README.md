# The TPC-H Benchmark for MySQL Database.

*A complete Python script for running the TPC-H benchmark on MySQL.*

## Prerequisites

1. Create a new MySQL user for running the benchmark

In Linux terminal, run the commands below in order to create the test user.

```bash
sudo mysql -u root
CREATE USER 'user'@'localhost' IDENTIFIED BY '123456';
GRANT ALL PRIVILEGES ON * . * TO 'user'@'localhost';
```

2. Install dependencies.

Check whether the following libraries are already installed in you machine. If not, run the commands in Linux terminal in order to install them. In case of Python3 versions, run *pip3* instead of *pip*.

### [MySQL connector for Python](https://dev.mysql.com/doc/connector-python/en/connector-python-installation-binary.html)
```bash
pip install mysql-connector-python
```

### [Pandas](https://pandas.pydata.org)
```bash
pip install pandas
```

### [NumPy](https://numpy.org)
```bash
pip install numpy
```

3. Running the script

After installing the dependecies, the script is ready to run. Open the terminal in the project directory and run the following command: `python main.py`.
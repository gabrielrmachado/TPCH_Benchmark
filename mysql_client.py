import mysql.connector as mysql
from mysql.connector import pooling
import time

class MySQL_TPCH:    
    def __init__(self, host, user, password, database_name):
      self.__connection = mysql.connect(host = host, user = user, password = password, allow_local_infile = True)
      self.database_name = database_name
      self.__host = host
      self.__user = user
      self.__password = password

    def connect_database(self, database_name = ""):
      if database_name == "": database_name = self.database_name
      self.__cursor = self.__connection.cursor()
      self.__cursor.execute("SHOW DATABASES LIKE '{0}'".format(database_name))
      self.__result = self.__cursor.fetchall()

      if len(self.__result) == 0:
          print("Database {0} does not exist.".format(database_name))
          # CREATE USER 'newuser'@'localhost' IDENTIFIED BY 'password';
          # GRANT ALL PRIVILEGES ON * . * TO 'newuser'@'localhost';
          return False
      else:
          self.__cursor.execute("USE {0}".format(database_name))
          print("Connected on database {0}.".format(database_name))        
          return True

    def run_command(self, command, cursor = None, sf=None):
      if cursor == None:
        cursor = self.__cursor

      try:
        for r in cursor.execute(command, multi=True):
            if r.with_rows:
              result = cursor.fetchall()
              print(result)

      except mysql.errors.InterfaceError:
        print("Command executed without fetchs.\n")
        return 0

      except mysql.errors.InternalError as e:
        if type(e).__name__ == "1213" or type(e).__name__ == "40001":
          time.sleep(0.5)
          cursor.execute(command, multi=True)
          return 1

      except mysql.errors.IntegrityError as e:
        if type(e).__name__ == "1062" or type(e).__name__ == "23000":
          print("\nDuplicated primary key. Restarting command...")
          time.sleep(0.5)
          cursor.execute("call refresh_function2({0})".format(sf))
          return 2

      return 0

    def getPoolConnection(self):
      return pooling.MySQLConnectionPool(pool_size=1, pool_name="mysqlpool", host=self.__host, database=self.database_name, user=self.__user, password=self.__password).get_connection()

    def close(self):
      if self.__connection.is_connected():
        self.__cursor.close()
        self.__connection.close()
        print("Connection to {0} database closed.\n".format(self.database_name))
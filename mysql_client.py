import mysql.connector as mysql
from mysql.connector import pooling

class MySQL_TPCH:    
    def __init__(self, host, user, password, database_name):
      self.database_name = database_name
      self.__db_conf = mysql.connect(host = host, user = user, password = password, allow_local_infile = True)
      self.connection_pool = pooling.MySQLConnectionPool(pool_size=32, pool_name="Throughput", host=host, database=database_name, user=user, password=password)

    def connect_database(self, database_name = ""):
      if database_name == "": database_name = self.database_name
      self.__cursor = self.__db_conf.cursor()
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

    def run_command(self, command, cursor = None):
      if cursor == None:
        cursor = self.__cursor
      
      cursor.execute(command)

      try:
        result = cursor.fetchall()
        if (len(result) > 0):
          for r in result:
            print(r)
      except mysql.errors.InterfaceError:
        print("Command executed without fetchs.\n")

    def get_connection_pool(self):
      return self.connection_pool.get_connection()

    def close(self):
      if self.__db_conf.is_connected():
        self.__cursor.close()
        self.__db_conf.close()
        print("Connection to {0} database closed.\n".format(self.database_name))
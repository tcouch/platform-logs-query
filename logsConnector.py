#establish a __conn to platform's SGE logs database
import pymysql
import sys
from secrets.sgelogs import Secrets

class logsConnection:
    Secrets=Secrets()
    def __init__(self,host=Secrets.host,
                      port=Secrets.port,
                      user=Secrets.user,
                      passwd=Secrets.passwd,
                      db=Secrets.db):
        try:
            self.__conn = pymysql.Connect(host=host,port=port,user=user,passwd=passwd,db=db,)
            #setup output as dictionary for easy access to data 
            self.__cursor = self.__conn.cursor(pymysql.cursors.DictCursor)
            self.__cursor.execute("SELECT VERSION()")
            row = self.__cursor.fetchone()
            print("server version:", row)
        except pymysql.Error as e:
            print('Got error {!r}, errno is {}'.format(e, e.args[0]))
            sys.exit(1)

    def __del__(self):
        self.__cursor.close()
        self.__conn.close()
        print("sge object destroyed")


    def query(self,SQLquery):
        """The query function returns a tuple of dictionaries containing all
         the rows and columns of the intended query"""
        try:
            self.__cursor.execute(SQLquery)
            return self.__cursor.fetchall()                 
        except pymysql.Error as e:
            print('Got error {!r}, errno is {}'.format(e, e.args[0]))
            sys.exit(1)
    
if __name__ == "__main__":
    sgelogs = logsConnection()
    
    rows = sgelogs.query("SELECT * FROM sgelogs2.accounting limit 3;")
    print(rows)
    # print sgelogs.cursor.description
    exit()


        

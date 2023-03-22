import mysql.connector
from mysql.connector import errorcode, Error, connect

class mysql_connector_one:
    def connection():
        """Connects to database.
        :return: the database connection
        :rtype: a connection object.
        """
        try: 
            cnx = mysql.connector.connect(option_files='connector.cnf')
            return cnx
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            cnx.close()

    def run(con, query):
        """ Run a query.
        :param con: a connection object
	    :type con: mysq.connector.connection.MysqlConnection
	    :param query: an SQL query
	    :type query: str
	    :return: the result set as Python list of tuples
	    :rtype: list
	    """
        cursor = con.cursor()
        cursor.execute( query )
        result = cursor.fetchall()
        cursor.close()

        return result
    
    def end_connection(cnx):
        """Ends the connection to the database.
        :param cnx: a connection object
        :type con: mysql.connector.connection.MysqlConnection
        """
        cnx.close()
import mysql.connector
from mysql.connector import Error
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import pandas as pd

# source venv/bin/activate


def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            auth_plugin="mysql_native_password",
        )

        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")


def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name, user=user_name, passwd=user_password, database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


def execute_query(connection, query):  # For changing dataBase
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


def read_query(connection, query):  # For reading info
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")


def execute_list_query(connection, sql, val):  # insert list(s) into table
    cursor = connection.cursor()
    try:
        cursor.executemany(sql, val)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


def putDataFrame_toSQL(dataBaseName, tableName, dataFrame, user, password):
    sqlEngine = create_engine(
        "mysql+pymysql://%s:%s@localhost/%s" % (user, password, dataBaseName),
        pool_recycle=3600,
    )
    dbConnection = sqlEngine.connect()
    try:
        frame = dataFrame.to_sql(tableName, dbConnection, if_exists="fail")

    except ValueError as vx:
        print(vx)

    except Exception as ex:
        print(ex)

    else:
        print("Table %s created successfully." % tableName)

    finally:
        dbConnection.close()


def replace_existing_table(dataBaseName, tableName, dataFrame, user, password):
    sqlEngine = create_engine(
        "mysql+pymysql://%s:%s@localhost/%s" % (user, password, dataBaseName),
        pool_recycle=3600,
    )
    dbConnection = sqlEngine.connect()
    try:
        frame = dataFrame.to_sql(tableName, dbConnection, if_exists="append")

    except ValueError as vx:
        print(vx)

    except Exception as ex:
        print(ex)

    else:
        print("Table %s created successfully." % tableName)

    finally:
        dbConnection.close()


if __name__ == "__main__":
    # creates and connects to database"
    pw = "MyNewPass"  # put your root password here
    connection = create_server_connection("localhost", "root", pw)  # pw is the password
    create_database_query = "CREATE DATABASE test"
    create_database(connection, create_database_query)

    # creates data Frame
    data = pd.read_csv("nasdaqtraded_230510141735.txt", sep="|")
    data2 = pd.read_csv("nasdaqtraded_230512105005.txt", sep="|")
    data3 = pd.read_csv("bxtraded_230510141735.txt", sep="|")
    # puts data frame into 'test' database with table name 'ex'
    putDataFrame_toSQL("test", "ex", data, "root", pw)
    replace_existing_table("test", "ex", data2, "root", pw)

    # example query with new table
    connection = create_db_connection("localhost", "root", pw, "test")
    q1 = """
SELECT *
FROM BX;
"""
    q2 = """SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'ex'
ORDER BY ORDINAL_POSITION"""
    dropTable_Query = "DROP TABLE BX;"
    replace_existing_table("test", "BX", data3, "root", pw)
    results = read_query(connection, q1)
    for result in results:
        print(result)

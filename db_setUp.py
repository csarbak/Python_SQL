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
        print("Table %s updated successfully." % tableName)

    finally:
        dbConnection.close()


if __name__ == "__main__":
    pw = "yupt pass"
    connection = create_db_connection("localhost", "root", pw, "test")
    create_table_nasdaq = """
CREATE TABLE nasdaq (
    `index` BIGINT UNSIGNED PRIMARY KEY,
    `Nasdaq Traded` VARCHAR(255) NULL,
    `Symbol` VARCHAR(255),
    `Security Name` VARCHAR(255),
    `Listing Exchange` VARCHAR(255),
    `Market Category` VARCHAR(255),
    ETF VARCHAR(255),
    `Round Lot Size` DOUBLE,
    `Test Issue` VARCHAR(255),
    `Financial Status` VARCHAR(255),
    `CQS Symbol` VARCHAR(255),
    `NASDAQ Symbol` VARCHAR(255),
    `NextShares` VARCHAR(255)
  );
 """
    create_table_PHLX = """
CREATE TABLE PHLX (
    `index` BIGINT UNSIGNED PRIMARY KEY,
    `PHLX Traded` VARCHAR(255) NULL,
    `Symbol` VARCHAR(255),
    `Security Name` VARCHAR(255),
    `Listing Exchange` VARCHAR(255),
    `Market Category` VARCHAR(255),
    ETF VARCHAR(255),
    `Round Lot Size` DOUBLE,
    `Test Issue` VARCHAR(255),
    `Financial Status` VARCHAR(255),
    `CQS Symbol` VARCHAR(255),
    `PHLX Symbol` VARCHAR(255),
    `NextShares` VARCHAR(255)
  );
 """

    create_table_BX = """
CREATE TABLE BX (
    `index` BIGINT UNSIGNED PRIMARY KEY,
    `BX Traded` VARCHAR(255) NULL,
    `Symbol` VARCHAR(255),
    `Security Name` VARCHAR(255),
    `Listing Exchange` VARCHAR(255),
    `Market Category` VARCHAR(255),
    ETF VARCHAR(255),
    `Round Lot Size` DOUBLE,
    `Test Issue` VARCHAR(255),
    `Financial Status` VARCHAR(255),
    `CQS Symbol` VARCHAR(255),
    `BX Symbol` VARCHAR(255),
    `NextShares` VARCHAR(255)
  );
 """
    execute_query(connection, create_table_nasdaq)
    execute_query(connection, create_table_PHLX)
    execute_query(connection, create_table_BX)

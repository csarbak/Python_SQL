import mysql.connector
from mysql.connector import Error
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import pandas as pd

# source venv/bin/activate


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

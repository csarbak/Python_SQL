import mysql.connector
from mysql.connector import Error
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import pandas as pd
import argparse
from sys import argv

EXISTING_DATABASE_NAME = "test"
MYSQL_USER = "root"
MYSQL_PASSWORD = "yourpass"


def replace_existing_table(dataBaseName, tableName, dataFrame, user, password):
    sqlEngine = create_engine(
        "mysql+pymysql://%s:%s@localhost/%s" % (user, password, dataBaseName),
        pool_recycle=3600,
    )
    try:
        dbConnection = sqlEngine.connect()
    except ValueError as vx:
        print(
            "Could not esatablish Connection to DataBase recheck correct user, password, and DataBase Name"
        )
        print(vx)

    except Exception as ex:
        print(
            "Could not esatablish Connection to DataBase recheck correct user, password, and DataBase Name"
        )
        print(ex)

    else:
        print("Connection created successfully.")
        try:
            frame = dataFrame.to_sql(tableName, dbConnection, if_exists="append")

        except ValueError as vx:
            print("Error creating frame")
            print(vx)

        except Exception as ex:
            print("Error creating frame")
            print(ex)

        else:
            print("Table %s updated successfully." % tableName)

        finally:
            dbConnection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""This is a very basic script program where it is assumed the tables and databases are already created"""
    )

    parser.add_argument(
        "tableName",
        type=str,
        help="This is the table name to be changed",
        action="store",
    )

    parser.add_argument(
        "newInfo",
        type=str,
        help="This is info goes in the updated table",
        action="store",
    )

    args = parser.parse_args(argv[1:])

    data = pd.read_csv(args.newInfo, sep="|")
    # puts data frame into 'test' database with table name 'ex'
    replace_existing_table(
        EXISTING_DATABASE_NAME, args.tableName, data, MYSQL_USER, MYSQL_PASSWORD
    )


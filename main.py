import mysql.connector
from pandas import read_sql, read_csv, DataFrame
from sqlalchemy import create_engine
import sys
import pymysql
pymysql.install_as_MySQLdb()

my_project_path = "/Users/petermyers/Desktop/high_quality_programs/14_gcp_sql/"
sys.path.append(my_project_path)
from connection_config import *

def get_query_from_file(sql_file_path):
    """
    Takes a path to a SQL file and returns the query in the file

    :param sql_file_path: str - the path to the SQL file like '/home/shared/ir_ds_etl/impala_etl/recommendations/sql/sql.sql'
    :return: str
    """
    file = open(sql_file_path)
    query = file.read()
    file.close()
    return query


table_name = 'test'
sql_path = f"{my_project_path}sql/{table_name}.sql"
query = get_query_from_file(sql_path)
parameters = {}
for key, value in parameters.items():
    query = query.replace("{" + key +"}", value)
try:
    connection = mysql.connector.connect(**connection_config)
    df = read_sql(query, connection)
except Exception as e:
    print("Something went wrong with a sql query")
    print(e)
    raise
finally:
    connection.close()


try:
    connection_string = f"mysql://peter2:a@35.236.113.37/front_room"
    engine = create_engine(connection_string, echo=False)
    df.to_sql('test_fact2', con=engine)
except Exception as e:
    print("Something went wrong with a sql query")
    print(e)
    raise
finally:
    engine.dispose()

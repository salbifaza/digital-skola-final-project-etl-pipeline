from modules.fetch_api import FetchApi
from modules.transform import Transform
from sqlalchemy import create_engine
import logging

def connect_mysql(user,password,host,db,port):
    engine = create_engine("mysql+mysqlconnector://{}:{}@{}:{}/{}".format(
        user,password,host,port,db
    ))
    return engine

from sqlalchemy import create_engine
def connect_postgres(user,password,host,db,port):
    engine = create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(
        user,password,host,port,db
    ))
    return engine

mysql_host     = '127.0.0.1' 
mysql_login    = 'mysql'
mysql_password = 'mysql'
mysql_schema   = 'mysql'
mysql_port     = 3307
engine_mysql = connect_mysql(
    host     = mysql_host,
    user     = mysql_login,
    password = mysql_password,
    db       = mysql_schema,
    port     = mysql_port
)


postgre_host      = '127.0.0.1'
postgre_login     = 'postgres'
postgre_password  = 'postgres'
postgre_schema    = 'dwh'
postgre_port      = 5435


fetch = FetchApi('http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab%22')
df    = fetch.get_data()

engine_mysql = connect_mysql(
    host     = mysql_host,
    user     = mysql_login,
    password = mysql_password,
    db       = mysql_schema,
    port     = mysql_port
)

engine_postgre = connect_postgres(
    host     = postgre_host,
    user     = postgre_login,
    password = postgre_password,
    db       = postgre_schema,
    port     = postgre_port
)

df.to_sql(con=engine_mysql, name='covid_data',if_exists='replace', index=False)

transformer = Transform(engine_mysql, engine_postgre)
transformer.create_dim_table('province')
transformer.create_dim_table('district')
transformer.create_dim_table('case')

transformer.insert_daily('district')
transformer.insert_daily('province')

# print(data.head())
# print(df[:1])
# print(df)
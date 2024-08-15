from pg8000.native import Connection, Error, DatabaseError, InterfaceError
from dotenv import load_dotenv
import os


load_dotenv()

def extract():

# temporary credentials for dev- will not have access when uploaded

    database = os.getenv('database')
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = os.getenv('port')


    try:
        db = Connection.run(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port
        )
    except DatabaseError as e:
        print(e)
    except InterfaceError as i:
        print(i)


    
import os
from dotenv import load_dotenv
# from configparser import ConfigParser

# def config(filename='dbsetting.ini',section='postgres'):
#     parser=ConfigParser()
#     parser.read(filename)
#     db={}
#     if parser.has_section(section):
#         db={k:v for k,v in parser.items(section)}
#     else:
#         raise Exception('{0} has no section header {1}'.format(filename,section))
#     return db
load_dotenv()


def config():
    db={
        'user':os.getenv('USER'),
        'host':os.getenv('HOST'),
        'dbname':os.getenv('DBNAME'),
        'password':os.getenv('PASSWORD'),
    }
    return db


    # print(db)
    # print("user",repr(os.environ.get('user')))
    # print("dbname",repr(os.environ.get('dbname')))
# config()

import os
from dotenv import load_dotenv
load_dotenv('backend\\.env.dbsetting')
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

def config():
    db={
        'user':os.getenv('user'),
        'host':os.getenv('host'),
        'database':os.getenv('database'),
        'password':os.getenv('password'),
    }
    # print(db)
    return db
# config()

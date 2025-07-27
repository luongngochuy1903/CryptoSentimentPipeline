import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))
#Gnews
KEY_GNEWS = parser.get('gnews', 'key')
#Minio
MINIO_ACCESS_KEY = parser.get('minio','user')
MINIO_SECRET_KEY = parser.get('minio','password')
#Postgres
POSTGRES_USERNAME = parser.get('postgres','user')
POSTGRES_PASSWORD = parser.get('postgres','password')
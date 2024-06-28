import pymysql
from sqlalchemy import create_engine


def get_database_connection():
    # Connect to the MySQL database
    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="root",
        database="eiis",
        port=3307,
        charset='utf8'
    )
    return conn


def get_database_engine():
    # Replace these with your actual credentials and database
    database_username = 'root'
    database_password = 'root'
    database_ip = 'localhost'
    database_name = 'eiis'
    database_port = '3307'  # Example port number, replace it with your actual port number

    # Create and return the SQLAlchemy engine with port number included
    engine = create_engine(
        f'mysql+pymysql://{database_username}:{database_password}@{database_ip}:{database_port}/{database_name}?charset=utf8'
    )
    return engine


def get_database_connection_e_eiis():
    # Connect to the MySQL database
    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="root",
        database="e_eiis",
        port=3307,
        charset='utf8'
    )
    return conn


def get_database_engine_e_eiis():
    # Replace these with your actual credentials and database
    database_username = 'root'
    database_password = 'root'
    database_ip = 'localhost'
    database_name = 'e_eiis'
    database_port = '3307'  # Example port number, replace it with your actual port number

    # Create and return the SQLAlchemy engine with port number included
    engine = create_engine(
        f'mysql+pymysql://{database_username}:{database_password}@{database_ip}:{database_port}/{database_name}?charset=utf8'
    )
    return engine

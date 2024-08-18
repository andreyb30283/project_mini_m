import logging
import mysql.connector
import yaml

with open('./configs_queries.yaml', 'r') as file:
    configs = yaml.safe_load(file)

db_config_read = configs['data_bases']['database_read']
db_config_write = configs['data_bases']['database_write']


class DatabaseConnector:
    connection = None
    query_r = ''
    query_w = ''
    logger = logging.getLogger(__name__)
    # logging.basicConfig(level=logging.INFO)
    @classmethod
    def connect(cls, config_c):
        try:
            cls.connection = mysql.connector.connect(**config_c)

            cls.logger.info("Connection successful")

        except Exception as e:
            cls.logger.error(f"Connection failed: {e}")
            cls.connection = None

    @classmethod
    def rw_to_db(cls, query_to_db, params, config_w):
        cls.connect(config_w)
        if cls.connection:
            try:
                cursor = cls.connection.cursor()
                cursor.execute(query_to_db, params)
                cls.logger.info("Data written successfully")
                result = cursor.fetchall()
                cls.connection.commit()
                cls.logger.info("Data connection.commit()")
                cursor.close()
                cls.close_connection()
                return result
            except Exception as e:
                cls.logger.error(f"Failed to write data: {e}")
        else:
            cls.logger.error("No connection available")

    @classmethod
    def close_connection(cls):
        if cls.connection:
            cls.connection.close()
            cls.logger.info("Connection closed")

import mysql.connector


class DatabaseConnector:
    connection = None

    @classmethod
    def connect(cls, config):
        try:
            cls.connection = mysql.connector.connect(**config)
        except:
            cls.connection = None

    @classmethod
    def rw_to_db(cls, query, params, config):
        cls.connect(config)
        if cls.connection:
            try:
                cursor = cls.connection.cursor()
                cursor.execute(query, params)
                result = cursor.fetchall()
                cls.connection.commit()
                cursor.close()
                cls.close_connection()
                return result
            except:
                pass

    @classmethod
    def close_connection(cls):
        if cls.connection:
            cls.connection.close()

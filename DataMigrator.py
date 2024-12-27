import psycopg2
import uuid
from pymongo import MongoClient

class PostgresConnection:
    def __init__(self, host, database, user, password):
        self.connection_params = {
            "host": host,
            "database": database,
            "user": user,
            "password": password
        }
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            self.cursor = self.conn.cursor()
        except psycopg2.Error as e:
            raise Exception(f"Failed to connect to PostgreSQL: {e}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

class MongoConnection:
    def __init__(self, connection_string, database_name):
        self.connection_string = connection_string
        self.database_name = database_name
        self.client = None
        self.db = None

    def connect(self):
        try:
            self.client = MongoClient(self.connection_string)
            self.db = self.client[self.database_name]
        except Exception as e:
            raise Exception(f"Failed to connect to MongoDB: {e}")

    def close(self):
        if self.client:
            self.client.close()

class DataMigrator:
    def __init__(self, postgres_conn, mongo_conn):
        self.postgres = postgres_conn
        self.mongo = mongo_conn

    def transform_row_to_document(self, row):
         # Converts a PostgreSQL row into a MongoDB document format
        return {
            "id": str(uuid.uuid4()).split("-")[0], # Creates a unique ID
            "msisdn": row[0],
            "name": row[1],
            "gender": row[2],
            "address": row[3],
            "age": row[4]
        }

    def migrate_data(self, source_table, target_collection):
        try:
            query = f"SELECT * FROM {source_table}"
            self.postgres.cursor.execute(query)
            rows = self.postgres.cursor.fetchall()
            
            collection = self.mongo.db[target_collection]
            
            for row in rows:
                document = self.transform_row_to_document(row)
                collection.insert_one(document)
                
        except Exception as e:
            raise Exception(f"Migration failed: {e}")


# Example usage:
if __name__ == "__main__":
    # Initialize connections
    pg_conn = PostgresConnection(
        host="localhost",
        database="postgres",
        user="postgres",
        password="postgres"
    )
    
    mongo_conn = MongoConnection(
        connection_string="mongodb://localhost:27017/",
        database_name="DB"
    )
    
    try:
        # Connect to both databases
        pg_conn.connect()
        mongo_conn.connect()
        
        # Initialize migrator and perform migration
        migrator = DataMigrator(pg_conn, mongo_conn)
        migrator.migrate_data("subscribers", "subscribers")
        
    finally:
        # Clean up connections
        pg_conn.close()
        mongo_conn.close() 
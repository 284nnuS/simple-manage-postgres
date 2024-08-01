from psycopg2._psycopg import cursor

from settings import settings
import psycopg2


class PostgresService:

    def __init__(self):
        self.conn = None
        self.cursor = None
        self.host=settings.POSTGRES_SERVER
        self.port=settings.POSTGRES_PORT

    def create_connection(
            self,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            dbname=settings.POSTGRES_DB):
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=user,
            password=password,
            dbname=dbname
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def close_connection(self):
        if self.conn is not None:
            self.conn.close()
            self.cursor.close()

    def create_user(self,user_name):
        self.cursor.execute(f"SELECT 1 FROM pg_roles WHERE rolname='{user_name}';")
        role_exists = self.cursor.fetchone()

        # Create role if it does not exist
        if not role_exists:
            self.cursor.execute(
                f"""CREATE ROLE {user_name} WITH
                    LOGIN
                    NOSUPERUSER
                    CREATEDB
                    NOCREATEROLE
                    NOINHERIT
                    NOREPLICATION
                    NOBYPASSRLS
                    CONNECTION LIMIT -1
                    PASSWORD '123';"""
            )

    def create_db(self,database_name):
        self.cursor.execute(
            f"""CREATE DATABASE {database_name} WITH
                OWNER = postgres
                ENCODING = 'UTF8'
                LOCALE_PROVIDER = 'libc'
                CONNECTION LIMIT = -1
                IS_TEMPLATE = False;"""
        )
    def init_roles(self,database_name):
        self.cursor.execute(f"REVOKE ALL PRIVILEGES ON DATABASE {database_name} FROM PUBLIC;")
        self.cursor.execute(f"GRANT ALL ON DATABASE {database_name} TO {database_name}_owner;")
        self.cursor.execute(f"GRANT CREATE ON SCHEMA public TO {database_name}_owner;")
        self.cursor.execute(f"GRANT USAGE ON SCHEMA public TO {database_name}_owner;")
        self.cursor.execute(f"GRANT SELECT,INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {database_name}_owner;")
        self.cursor.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT,INSERT, UPDATE, DELETE ON TABLES TO {database_name}_owner;")

        self.cursor.execute(f"GRANT CONNECT ON DATABASE {database_name} TO {database_name}_viewer;")
        self.cursor.execute(f"GRANT USAGE ON SCHEMA public TO {database_name}_viewer;")
        self.cursor.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {database_name}_viewer;")

    def alter_permission_view(self,database_name):
        self.cursor.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {database_name}_viewer;")


    def on_create_db(self, database_name):
        user_names = [database_name+"_owner",database_name+"_viewer"]
        # Init user and db
        self.create_connection()
        for user_name in user_names:
            self.create_user(user_name)
        self.create_db(database_name)
        self.close_connection()

        # Init role
        self.create_connection(dbname=database_name)
        self.init_roles(database_name)
        self.close_connection()

        # Alter permission for view
        self.create_connection(
            database_name + "_owner",
            "123",
            database_name)
        self.alter_permission_view(database_name)
        self.close_connection()

    def get_all_db(self):
        self.cursor.execute(f"SELECT datname FROM pg_database;")
        databases = self.cursor.fetchall()
        return databases

    def delete_db(self, database_name):
        self.cursor.execute(f"DROP DATABASE IF EXISTS {database_name};")
        print(f"Database '{database_name}' deleted successfully.")

        self.cursor.execute(f"DROP USER IF EXISTS {database_name}_owner;")
        print(f"User '{database_name}'_owner deleted successfully.")


        self.cursor.execute(f"DROP USER IF EXISTS {database_name}_viewer;")
        print(f"User '{database_name}'_viewer deleted successfully.")

if __name__ == "__main__":
    service = PostgresService()
    service.create_connection()
    print(service.conn)
    dbs = service.get_all_db()
    for db in dbs:
        print(db[0])
    service.close_connection()
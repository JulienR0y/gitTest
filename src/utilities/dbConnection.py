import pandas as pd
import psycopg2 as psg

class DbConstants:
    PSQL_HOST="PSQL_HOST"
    PSQL_USER="PSQL_USER"
    PSQL_PORT="PSQL_PORT"
    PSQL_DATABASE="PSQL_DATABASE"
    PSQL_PWD="PSQL_PWD"


class DatabaseConnection:
    def __init__(self, database_config):
        self.database_config = database_config


    def _connect(self):
        return psg.connect(**self.database_config, connect_timeout = 5)


    def execute_queries(self, queries, id_column="id"):
        connection = self._connect()
        cursor = connection.cursor()

        queries_result = []
        for query in queries:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            result = cursor.fetchall()

            df = pd.DataFrame(result, columns=columns)
            df = df.set_index(id_column)
            
            queries_result.append(df)

        cursor.close()
        connection.close()

        return queries_result

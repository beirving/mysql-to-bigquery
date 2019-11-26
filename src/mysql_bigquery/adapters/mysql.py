import datetime
from typing import Union
from mysql_bigquery import service_helpers
from mysql.connector import MySQLConnection, Error


class MySqlAdapter:
    def __init__(self, service: str):
        self.definitions = service_helpers.get_definitions(service)
        del(self.definitions['service'])
        del(self.definitions['data_set'])
        self.connection = None
        self.errors = {}

    def create_connection(self) -> bool:
        """
        create a usable connection in the self.connection attribute
        :return: bool of successfully creating a connection
        :raises RuntimeError
        """
        try:
            connection = MySQLConnection(**self.definitions)
            if connection.is_connected():
                self.connection = connection
                return True
        except Error as e:
            self.errors['create_connection'] = e
            RuntimeError('Failed to create connection')

    def fetch_results(self, query: str, **kwargs) -> Union[list, dict, bool]:
        """
        Run a MYSQL query and return results
        :param query: str
            valid MYSQL query
        :return: mysql.connector.cursor.MySQLCursor
        """
        if self.connection is None:
            self.create_connection()
        try:
            if kwargs:
                cursor = self.connection.cursor(**kwargs)
            else:
                cursor = self.connection.cursor(buffered=True)
            cursor.execute(query)
            rows = cursor.fetchall()

        except Error as e:
            self.errors['fetch_results'] = 'Error: {}'.format(e)
            return False

        finally:
            cursor.close()
            self.connection.close()
            self.connection = None
        return rows

    def fetch_all_table_names(self) -> list:
        """
        Using the database supplied in the config return a list of all table names
        :return: list
            list of tuples where the 0th index is the table name
                [{'table_name1',},{'table_name2',}]
        """
        if self.connection is None:
            self.create_connection()

        query = [
            "SELECT table_name",
            "FROM information_schema.tables",
            "WHERE table_type = 'base table'",
            f"and TABLE_SCHEMA = '{self.connection.database}'",
            "ORDER BY table_name"
        ]

        result = self.fetch_results(' '.join(query))

        return result

    def mysql_table_definition(self, table: str = None) -> list:
        """Using the database supplied in the config
                get COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT, COLUMN_DEFAULT, COLUMN_KEY, EXTRA
        :param table: str (optional)
            Specify the table to define
        :return: list
        """
        if self.connection is None:
            self.create_connection()

        query = [
            "SELECT ",
            "c.COLUMN_NAME as 'name',",
            "c.DATA_TYPE as 'type',",
            "c.COLUMN_COMMENT as 'comment',",
            "c.COLUMN_DEFAULT as 'default',",
            "c.COLUMN_KEY as 'key',",
            "c.EXTRA as 'extra',",
            "t.TABLE_NAME as 'table',",
            "c.ORDINAL_POSITION as 'position'",
            "FROM INFORMATION_SCHEMA.COLUMNS c",
            "INNER JOIN information_schema.tables t",
            "on c.TABLE_NAME = t.TABLE_NAME AND table_type = 'base table' AND t.TABLE_SCHEMA = c.TABLE_SCHEMA",
            f"WHERE c.TABLE_SCHEMA = '{self.connection.database}'"
        ]

        if table:
            query.append(f" AND t.TABLE_NAME = '{table}'")

        query.append('order by c.TABLE_NAME asc, c.ORDINAL_POSITION ASC')

        result = self.fetch_results(' '.join(query), dictionary=True)

        return result

    def count_items_to_sync(self, table: str, watched_column: str = None, last_run: str = None) -> Union[int, bool]:
        """Get the total number of rows
        :param table: str
            Subject table
        :param watched_column: str (optional)
            column to compare for getting a subset of the tables data
        :param last_run: str (optional)
            Date the compare against to get rows that occurred after given date
        :return: mysql.connector.cursor
        """
        query = [f"SELECT COUNT(*) AS count FROM `{table}`"]

        if last_run:
            query.append(f"WHERE {watched_column} > '{last_run}'")

        results = self.fetch_results(' '.join(query))
        if results:
            for result in results:
                return result[0]
        return results

    def get_random_records(
            self,
            table: str,
            limit: int,
            index: str,
            watched=None,
            last_updated: str = None
    ) -> list:
        """
        Get (limit) number of Random records from (table)
        :param table: str
            Name of the table to query
        :param limit: int
            number of records to return
        :param index: str
            primary key(s) of table
        :param watched:
            watched column to restrict the results with a where statement
        :param last_updated: str
            date on which to restrict the watched column on
        :return: list
            List of records returned from query
        """
        sub_query = [
            f"SELECT *",
            f"FROM {table}"
        ]

        if last_updated is not None:
            if type(last_updated) is int:
                sub_query.append(f"WHERE {watched} <= {last_updated}")
            else:
                sub_query.append(f"WHERE {watched} <= '{last_updated}'")

        sub_query.append("ORDER BY RAND()")
        sub_query.append(f"limit {limit}")

        query = [
            f'SELECT *',
            f"FROM ({' '.join(sub_query)}) as sample",
            'ORDER BY'
        ]

        order_by = []
        primary_ids = index.split(',')
        for key in primary_ids:
            order_by.append(f"{key} ASC")
        query.append(', '.join(order_by))

        result = self.fetch_results(' '.join(query), dictionary=True)

        return result

    def count_distinct(
            self,
            table: str,
            index: str,
            watched_column=None,
            last_updated: datetime.datetime = None
    ) -> Union[int, bool]:
        """
        Get the number of distinct rows of a given (table) and given (column)
        :param table: str
            Name of the table to query
        :param index: str
            column(s) to count distinct values
                composite column counts should be passed as a single string separated by a comma
                    EX: "column1,column2"
        :param watched_column: str
            time series column
        :param last_updated: str
            date for comparison on watched_column
        :note:
            Checking self.errors['fetch_results'] on False return
        :return: int, bool
            int: value is the count returned from the query
            bool: something went wrong with the query check self.errors['fetch_results']
        """
        query = [
            f"SELECT COUNT(DISTINCT {index})",
            f"FROM `{table}`"
        ]

        if watched_column is not None:
            if last_updated is not None:
                # get records before given date
                if type(last_updated) is int:
                    query.append(f"WHERE {watched_column} <= {last_updated}")
                else:
                    query.append(f"WHERE {watched_column} <= '{last_updated}'")

        results = self.fetch_results(' '.join(query))

        if results:
            for result in results:
                return result[0]
        return results

    def get_records(
            self,
            table: str,
            watched_column: str,
            last_run: str = None,
            limit: int = None,
            offset: int = None
    ) -> list:
        """Builds and executes a query for collecting sequential data from a table targeting the "watched" column
        :param table: str
            Subject Table
        :param watched_column: str
            Tracked Column for comparisons
        :param last_run: str
            Date of last run
        :param limit: int
            limit records returned
        :param offset: int
            offset records returned
        :return: mysql.connector.cursor
        """
        query = [f"SELECT * FROM `{table}`"]
        if last_run:
            query.append(f"WHERE `{watched_column}` > '{last_run}'")

        query.append(f"ORDER BY `{watched_column}` ASC")

        if limit is not None:
            query.append(f"limit {limit}")

        if offset is not None:
            query.append(f"offset {offset}")

        result = self.fetch_results(' '.join(query))

        return result

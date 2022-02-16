import unittest

from src.data.queryBuilder import QueryBuilder


@patch('os.system')
class TestQueryBuilder(unittest.TestCase):

    def test_build_query_all_conditions(self, mock_os):
        # Arrange
        query_builder = QueryBuilder()
        selects = ["statement_1", "statement_2"]
        table = "table_1"
        joins = [("table_join_1", "table_1.value = statement_1"), ("table_join_2", "table_2.value = statement_2")]
        constraints = ["constraint_1 = something", "constraint_2 = something_else"]
        # Act
        result = query_builder.build_query(selects, table, joins, constraints)
        expected_result_return = f"SELECT {selects[0]}, {selects[1]} FROM {table} JOIN {joins[0][0]} ON {joins[0][1]} JOIN {joins[1][0]} ON {joins[1][1]} WHERE {constraints[0]} AND {constraints[1]} ;"
        # Assert
        self.assertEqual(result, expected_result_return)

    # region SELECT
    def test_build_query_no_statement(self, mock_os):
        # Arrange
        query_builder = QueryBuilder()
        selects = None
        table = "table_1"
        joins = None
        constraints = None
        # Act
        result = query_builder.build_query(selects, table, joins, constraints)
        expected_result_return = f"SELECT * FROM {table} ;"
        # Assert
        self.assertEqual(result, expected_result_return)

    def test_build_query_with_single_statement(self):
        # Arrange
        query_builder = QueryBuilder()
        selects = ["statement_1"]
        table = "table_1"
        joins = None
        constraints = None
        # Act
        result = query_builder.build_query(selects, table, joins, constraints)
        expected_result_return = f"SELECT {selects[0]} FROM {table} ;"
        # Assert
        self.assertEqual(result, expected_result_return)

    def test_build_query_with_multiple_statement(self, mock_os):
        # Arrange
        query_builder = QueryBuilder()
        selects = ["statement_1", "statement_2"]
        table = "table_1"
        joins = None
        constraints = None
        # Act
        result = query_builder.build_query(selects, table, joins, constraints)
        expected_result_return = f"SELECT {selects[0]}, {selects[1]} FROM {table} ;"
        # Assert
        self.assertEqual(result, expected_result_return)

    # endregion

    # region FROM
    def test_build_query_no_table(self, mock_os):
        # Arrange
        query_builder = QueryBuilder()
        selects = None
        table = None
        joins = None
        constraints = None
        # Act
        result = query_builder.build_query(selects, table, joins, constraints)
        expected_result_return = f"SELECT * FROM None ;"
        # Assert
        self.assertEqual(result, expected_result_return)

    def test_build_query_with_table(self):
        # Arrange
        query_builder = QueryBuilder()
        selects = None
        table = "table_1"
        joins = None
        constraints = None
        # Act
        result = query_builder.build_query(selects, table, joins, constraints)
        expected_result_return = f"SELECT * FROM table_1 ;"
        # Assert
        self.assertEqual(result, expected_result_return)
    # endregion

    # region JOINS

    def test_build_query_no_joins(self, mock_os):
        # Arrange
        query_builder = QueryBuilder()
        selects = None
        table = "table_1"
        joins = None
        constraints = None
        # Act
        result = query_builder.build_query(selects, table, joins, constraints)
        expected_result_return = f"SELECT * FROM {table} ;"
        # Assert
        self.assertEqual(result, expected_result_return)

    def test_build_query_single_joins(self, mock_os):
        # Arrange
        query_builder = QueryBuilder()
        selects = None
        table = "table_1"
        joins = [("table_join_1", "table_1.value = statement_1")]
        constraints = None
        # Act
        result = query_builder.build_query(selects, table, joins, constraints)
        expected_result_return = f"SELECT * FROM {table} JOIN {joins[0][0]} ON {joins[0][1]} ;"
        # Assert
        self.assertEqual(result, expected_result_return)

    def test_build_query_multiple_joins(self, mock_os):
        # Arrange
        query_builder = QueryBuilder()
        selects = None
        table = "table_1"
        joins = [("table_join_1", "table_1.value = statement_1"), ("table_join_2", "table_2.value = statement_2")]
        constraints = None
        # Act
        result = query_builder.build_query(selects, table, joins, constraints)
        expected_result_return = f"SELECT * FROM {table} JOIN {joins[0][0]} ON {joins[0][1]} JOIN {joins[1][0]} ON {joins[1][1]} ;"
        # Assert
        self.assertEqual(result, expected_result_return)

    # endregion

    # region WITH
    def test_build_query_no_constraints(self, mock_os):
        # Arrange
        query_builder = QueryBuilder()
        selects = None
        table = "table_1"
        joins = None
        constraints = None
        # Act
        result = query_builder.build_query(selects, table, joins, constraints)
        expected_result_return = f"SELECT * FROM {table} ;"
        # Assert
        self.assertEqual(result, expected_result_return)

    def test_build_query_single_constraint(self, mock_os):
        # Arrange
        query_builder = QueryBuilder()
        selects = None
        table = "table_1"
        joins = None
        constraints = ["constraint_1 = something"]
        # Act
        result = query_builder.build_query(selects, table, joins, constraints)
        expected_result_return = f"SELECT * FROM {table} WHERE {constraints[0]} ;"
        # Assert
        self.assertEqual(result, expected_result_return)

    def test_build_query_multiple_constraint(self, mock_os):
        # Arrange
        query_builder = QueryBuilder()
        selects = None
        table = "table_1"
        joins = None
        constraints = ["constraint_1 = something", "constraint_2 = something_else"]
        # Act
        result = query_builder.build_query(selects, table, joins, constraints)
        expected_result_return = f"SELECT * FROM {table} WHERE {constraints[0]} AND {constraints[1]} ;"
        # Assert
        self.assertEqual(result, expected_result_return)

    # endregion

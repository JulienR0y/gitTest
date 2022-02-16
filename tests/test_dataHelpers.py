import unittest
import pandas as pd

from datetime import datetime
from inspect import getcallargs, signature
from unittest.mock import patch, Mock, call, ANY
with patch.dict(os.environ, {'key': 'mock-value'}):
    from src.data.dataHelpers import DataHelper


@patch('src.data.dataHelpers.os.system')
class TestDataHelpers(unittest.TestCase):
    def test_add_date_info(self, mock_os):
        # Arrange
        data_helper = DataHelper()

        data = {"col1": [1, 2, 3],
                "col_date": [datetime(2020, 1, 19), datetime(2020, 1, 18), datetime(2020, 1, 20)],
                "col2": [1, 2, 3]}
        amounts_df = pd.DataFrame(data=data, index=[0, 1, 2])

        # Act
        result = data_helper.add_date_info(amounts_df, "col_date")
        expected_df_data = {"col1": [1, 2, 3],
                            "col_date": [datetime(2020, 1, 19), datetime(2020, 1, 18), datetime(2020, 1, 20)],
                            "col2": [1, 2, 3],
                            "col_date_year": [2020, 2020, 2020],
                            "col_date_month": [1, 1, 1],
                            "col_date_day": [19, 18, 20],
                            "col_date_week_day": [6, 5, 0]}
        expected_df_break = pd.DataFrame()
        expected_df = pd.DataFrame(data=expected_df_data, index=[0, 1, 2])

        # Assert
        pd.testing.assert_frame_equal(result, expected_df)

    # @patch('data.dataHelpers.DataHelper.get_engagement_info')
    # @patch('data.dataHelpers.DataHelper._get_engagement_amounts')
    @patch('data.dataHelpers.QueryBuilder.build_query')
    @patch('data.dataHelpers.DatabaseConnection.execute_queries')
    def test_get_amount_table_engagements_with_engagements_ids_list(self, mock_os, mock_db_execute_queries, mock_qb_build_query):
        # Arrange
        data_helper = DataHelper()

        get_info_side_effect1 = pd.DataFrame([{'period_start': datetime(2019, 12, 1), 'period_end': datetime(2020, 11, 30)}])
        get_info_side_effect2 = pd.DataFrame([{'period_start': datetime(2000, 12, 1), 'period_end': datetime(2000, 11, 30)}])

        get_amount_side_effect3 = pd.DataFrame([{"col1": [1, 2, 3], "col2": ["A", "B", "C"]}])
        get_amount_side_effect4 = pd.DataFrame([{"col1": [4, 5, 6], "col2": ["D", "E", "F"]}])
        mock_db_execute_queries.side_effect = [[get_info_side_effect1], [get_info_side_effect1],
                                               [get_info_side_effect2], [get_info_side_effect2],
                                               [get_amount_side_effect3], [get_amount_side_effect4]]

        # Act
        result = data_helper.get_amount_table(tenant_id="tenant_id", engagement_ids=["a", "b"])

        expected_result_df = pd.concat([pd.DataFrame([{"col1": [1, 2, 3], "col2": ["A", "B", "C"]}]),
                                       pd.DataFrame([{"col1": [4, 5, 6], "col2": ["D", "E", "F"]}])])

        # Assert
        pd.testing.assert_frame_equal(result, expected_result_df)

    @patch('data.dataHelpers.QueryBuilder.build_query')
    @patch('data.dataHelpers.DatabaseConnection.execute_queries')
    def test_get_amount_table_with_no_engagement_ids(self, mock_os, mock_db_execute_queries, mock_qb_build_query):
        # Arrange
        data_helper = DataHelper()

        mock_db_execute_queries.return_value = [pd.DataFrame([{"data": {"value1", "value2"}}])]

        # Act
        result = data_helper.get_amount_table(tenant_id="tenant_id", engagement_ids=None)
        expected_result = pd.DataFrame([{"data": {"value1", "value2"}}])

        # Assert
        pd.testing.assert_frame_equal(result, expected_result)

    @ patch('data.dataHelpers.QueryBuilder.build_query')
    @ patch('data.dataHelpers.DatabaseConnection.execute_queries')
    def test_make_fsli_mappings(self, mock_os, mock_db_execute_queries, mock_querybuilder):
        # Arrange
        data_helper = DataHelper()

        query_mapping_data = {"id": ["id1", "id2", "id3"],
                              "account_id": ["account_id_1", "account_id_2", "account_id_3"],
                              "fsli_id": ["fsli_id_1", "fsli_id_2", "fsli_id_3"]}
        query_mapping_df = pd.DataFrame(data=query_mapping_data)

        query_fsli_data = {"id": ["fsli_id_1", "fsli_id_2", "fsli_id_3"],
                           "reverse_fsli_id": ["reverse_fsli_id_1", "reverse_fsli_id_2", "reverse_fsli_id_3"]}
        query_fsli_df = pd.DataFrame(data=query_fsli_data, index=query_fsli_data["id"])

        mock_db_execute_queries.side_effect = [[query_mapping_df], [query_fsli_df]]

        amounts_df_data = {"col1": [1, 2, 3],
                           "account_id": ["account_id_1", "account_id_2", "account_id_3"],
                           "col3": [3, 4, 5]}

        amounts_df = pd.DataFrame(data=amounts_df_data)

        # Act
        result = data_helper.make_fsli_mappings(amounts_df, "engagement_id")
        expected_amounts_df_date = {"col1": [1, 2, 3],
                                    "account_id": ["account_id_1", "account_id_2", "account_id_3"],
                                    "col3": [3, 4, 5],
                                    "fsli_id": ["fsli_id_1", "fsli_id_2", "fsli_id_3"]}
        expected_amounts_df = pd.DataFrame(expected_amounts_df_date)

        # Assert
        pd.testing.assert_frame_equal(result, expected_amounts_df)

    @ patch('data.dataHelpers.QueryBuilder.build_query')
    @ patch('data.dataHelpers.DatabaseConnection.execute_queries')
    def test_get_engagement_amount_query_with_no_engagement_dates(self, mock_os, mock_db_execute_queries, mock_qb_build_query):
        # Arrange
        data_helper = DataHelper()

        query = ['SELECT a  FROM b JOIN c ON d']
        mock_qb_build_query.side_effect = query

        execute_queries_data = {"col1": [1, 2, 3],
                                "col2": [3, 4, 5]}
        execute_queries_df = pd.DataFrame(data=execute_queries_data)
        mock_db_execute_queries.return_value = [execute_queries_df]

        calls = [call(query)]

        # Act
        result = data_helper._get_engagement_amounts(tenant_id="tenant_id", engagement_dates=None)
        expected_result_return = pd.DataFrame(data=execute_queries_data)

        # Assert
        mock_db_execute_queries.assert_has_calls(calls)
        pd.testing.assert_frame_equal(result, expected_result_return)

    @ patch('data.dataHelpers.QueryBuilder.build_query')
    @ patch('data.dataHelpers.DatabaseConnection.execute_queries')
    def test_get_engagement_amounts_query_with_engagment_dates_list(self, mock_os, mock_db_execute_queries, mock_qb_build_query):
        # Arrange
        data_helper = DataHelper()

        query = ["SELECT a  FROM b JOIN c ON d"]
        mock_qb_build_query.side_effect = query

        execute_queries_data = {"col1": [1, 2, 3],
                                "col2": [3, 4, 5]}
        execute_queries_df = pd.DataFrame(data=execute_queries_data)
        mock_db_execute_queries.return_value = [execute_queries_df]

        engagement_dates_list = ["engagement_id1", "engagement_id2"]

        # Act
        data_helper._get_engagement_amounts(tenant_id="tenant_id", engagement_dates=engagement_dates_list)

        # Assert
        mock_db_execute_queries.assert_called_with(['SELECT a  FROM b JOIN c ON d'])
        self.assertEqual(mock_qb_build_query.call_args_list, [call(
            ANY, ANY, ANY, ["accounting_amounts.tenant_id ='tenant_id'", "accounting_entries.date BETWEEN 'engagement_id1' AND 'engagement_id2'"])])

    @ patch('data.dataHelpers.QueryBuilder')
    @ patch('data.dataHelpers.DatabaseConnection.execute_queries')
    def test_get_flipping_id_amounts(self, mock_os, mock_db_execute_queries, mock_query_builder):
        # Arrange
        data_helper = DataHelper()

        query_mapping_data = {"id": ["id1", "id2", "id3"],
                              "account_id": ["account_id_1", "account_id_2", "account_id_3"],
                              "fsli_id": ["fsli_id_1", "fsli_id_2", "fsli_id_3"]}
        query_mapping_df = pd.DataFrame(data=query_mapping_data)

        query_fsli_data = {"id": ["fsli_id_1", "fsli_id_2", "fsli_id_3"],
                           "reverse_fsli_id": ["reverse_fsli_id_1", None, "reverse_fsli_id_3"]}
        query_fsli_df = pd.DataFrame(data=query_fsli_data, index=query_fsli_data["id"])

        mock_db_execute_queries.side_effect = [[query_mapping_df], [query_fsli_df]]

        amounts_df_data = {"col1": [1, 2, 3],
                           "account_id": ["account_id_1", "account_id_2", "account_id_3"]}
        amounts_df = pd.DataFrame(data=amounts_df_data)

        # Act
        result = data_helper.get_flipping_id_amounts(amounts_df, "engagement_id")

        expected_amounts_df_data = {"col1": [1, 3],
                                    "account_id": ["account_id_1", "account_id_3"]}
        expected_amounts_df_return = pd.DataFrame(data=expected_amounts_df_data, index=[0, 2])

        # Assert
        pd.testing.assert_frame_equal(result, expected_amounts_df_return)

    @ patch('data.dataHelpers.DatabaseConnection.execute_queries')
    def test_get_mapping_dict(self, mock_os, mock_connect):
        # Arrange
        data_helper = DataHelper()

        query_mapping_data = {"id": ["id1", "id2", "id3"],
                              "account_id": ["account_id_1", "account_id_2", "account_id_3"],
                              "fsli_id": ["fsli_id_1", "fsli_id_2", "fsli_id_3"]}
        query_mapping_df = pd.DataFrame(data=query_mapping_data)

        query_fsli_data = {"id": ["fsli_id_1", "fsli_id_2", "fsli_id_3"],
                           "reverse_fsli_id": ["reverse_fsli_id_1", "reverse_fsli_id_2", "reverse_fsli_id_3"]}
        query_fsli_df = pd.DataFrame(data=query_fsli_data, index=query_fsli_data["id"])

        mock_connect.side_effect = [[query_mapping_df], [query_fsli_df]]

        # Act
        result = data_helper._get_mapping_dict("engagement_id")
        expected_mapping_dict = {'account_id_1': {'fsli_id': 'fsli_id_1', 'reverse_fsli_id': 'reverse_fsli_id_1'},
                                 'account_id_2': {'fsli_id': 'fsli_id_2', 'reverse_fsli_id': 'reverse_fsli_id_2'},
                                 'account_id_3': {'fsli_id': 'fsli_id_3', 'reverse_fsli_id': 'reverse_fsli_id_3'}}

        # Assert
        self.assertEqual(result, expected_mapping_dict)

    @ patch('data.dataHelpers.DatabaseConnection.execute_queries')
    def test_get_engagement_info(self, mock_os, mock_connect):
        # Arrange
        data_helper = DataHelper()

        engagement_info_data = {"engagement_id": ["engagement_id_1"],
                                "col_2": ["col_2_value_1"],
                                "col_3": ["col_3_value_1"]}
        engagement_info_df = pd.DataFrame(data=engagement_info_data)

        mock_connect.return_value = [engagement_info_df]

        # Act
        result = data_helper.get_engagement_info("engagement_id_1")
        expected_dict_return = {"engagement_id": "engagement_id_1",
                                "col_2": "col_2_value_1",
                                "col_3": "col_3_value_1"}

        # Assert
        self.assertEqual(result, expected_dict_return)

    @ patch('data.dataHelpers.DatabaseConnection.execute_queries')
    def test_get_organization_info(self, mock_os, mock_connect):
        # Arrange
        data_helper = DataHelper()

        engagement_info_data = {"organization_id": ["organization_id_1"],
                                "col_2": ["col_2_value_1"],
                                "col_3": ["col_3_value_1"]}
        engagement_info_df = pd.DataFrame(data=engagement_info_data)

        mock_connect.return_value = [engagement_info_df]

        # Act
        result = data_helper.get_organization_info("organization_id_1")
        expected_dict_return = {"organization_id": "organization_id_1",
                                "col_2": "col_2_value_1",
                                "col_3": "col_3_value_1"}

        # Assert
        self.assertEqual(result, expected_dict_return)

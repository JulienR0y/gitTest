import os
from typing import List

import numpy as np
import pandas as pd
from data.queryBuilder import QueryBuilder
from stamped_ai.utilities.data.dbConnection import (DatabaseConnection,
                                                    DbConstants)


class DataHelper:
    def __init__(self):
        db_config = {
            "user": os.environ[DbConstants.PSQL_USER],
            "password": os.environ[DbConstants.PSQL_PWD],
            "host": os.environ[DbConstants.PSQL_HOST],
            "port": os.environ[DbConstants.PSQL_PORT],
            "database": os.environ[DbConstants.PSQL_DATABASE]
        }
        self.db_connection = DatabaseConnection()
        self.query_builder = QueryBuilder()

    def get_amount_table(self, tenant_id: str, engagement_ids: str = None) -> pd.DataFrame:
        if engagement_ids is None:
            return self._get_engagement_amounts(tenant_id, None)

        engagements_dates = [
            (self.get_engagement_info(engagement_id)["period_start"].strftime("%Y-%m-%d"),
             self.get_engagement_info(engagement_id)["period_end"].strftime("%Y-%m-%d"))
            for engagement_id in engagement_ids]
        amount_dataframes = [self._get_engagement_amounts(tenant_id, engagement_dates) for engagement_dates in engagements_dates]

        return pd.concat(amount_dataframes)

    def add_date_info(self, amounts_df: pd.DataFrame, date_column: str) -> pd.DataFrame:
        amounts_df[f'{date_column}_year'] = [date.year for date in amounts_df[date_column]]
        amounts_df[f'{date_column}_month'] = [date.month for date in amounts_df[date_column]]
        amounts_df[f'{date_column}_day'] = [date.day for date in amounts_df[date_column]]
        amounts_df[f'{date_column}_week_day'] = [date.weekday() for date in amounts_df[date_column]]

        return amounts_df

    def make_fsli_mappings(self, amounts_df: pd.DataFrame, engagement_id: str) -> pd.DataFrame:
        fsli_dict = self._get_mapping_dict(engagement_id)
        fsli_list = [fsli_dict[account_id]["fsli_id"] for account_id in amounts_df["account_id"]]
        amounts_df["fsli_id"] = fsli_list

        return amounts_df

    def _get_engagement_amounts(self, tenant_id: str, engagement_dates: List[str]) -> pd.DataFrame:
        selects = ["accounting_amounts.id",
                   "accounting_amounts.amount as amount",
                   "accounting_amounts.description as amount_description",
                   "accounting_amounts.type as amount_type",
                   "accounting_amounts.account_id as account_id",

                   "accounting_accounts.name as account_description",
                   "accounting_accounts.currency as account_currency",

                   "accounting_amounts.entry_id as transaction_id",
                   "accounting_entries.context as  transaction_context",
                   "accounting_entries.date as transaction_date",
                   "accounting_entries.external_created_at as transaction_external_date",
                   "accounting_entries.discarded_at as transaction_discarded_at"]

        table = "accounting_amounts"
        joins = [("accounting_accounts", "accounting_amounts.account_id = accounting_accounts.id"),
                 ("accounting_entries", "accounting_amounts.entry_id = accounting_entries.id")]

        constraints = [f"accounting_amounts.tenant_id ='{tenant_id}'"]

        if engagement_dates is not None:
            constraints.append(f"accounting_entries.date BETWEEN '{engagement_dates[0]}' AND '{engagement_dates[1]}'")

        query = self.query_builder.build_query(selects, table, joins, constraints)

        return self.db_connection.execute_queries([query])[0]

    def get_flipping_id_amounts(self, amounts_df: pd.DataFrame, engagement_id: str) -> pd.DataFrame:
        account_fsli_dict = self._get_mapping_dict(engagement_id)
        accounts_mapping_flips = []

        for account_id in account_fsli_dict:
            value = account_fsli_dict[account_id]
            if value["reverse_fsli_id"] is not None:

                accounts_mapping_flips.append({account_id: {"fsli_id": value["fsli_id"], "reverse_fsli_id": value["reverse_fsli_id"]}})

        accounts_id = [list(account.keys())[0] for account in accounts_mapping_flips]

        flip_amounts_table = amounts_df[amounts_df["account_id"].isin(pd.Series(accounts_id))]

        return flip_amounts_table

    def _get_mapping_dict(self, engagement_id: str) -> dict:
        mapping_dict = {}

        query_mapping = f"SELECT id, account_id, fsli_id FROM account_mappings WHERE engagement_id = '{engagement_id}';"
        account_mappings = self.db_connection.execute_queries([query_mapping])[0]

        query_fsli = "SELECT id ,reverse_fsli_id FROM accounting_fslis;"
        accounting_fslis = self.db_connection.execute_queries([query_fsli])[0]

        for account_id, fsli_id in zip(account_mappings["account_id"], account_mappings["fsli_id"]):
            reverse_fsli = accounting_fslis.loc[fsli_id].reverse_fsli_id
            mapping_dict[account_id] = {
                "fsli_id": fsli_id, "reverse_fsli_id": reverse_fsli}

        return mapping_dict

    def get_engagement_info(self, engagement_id: str) -> dict:
        engagement_info = self.db_connection.execute_queries(
            [f"SELECT id, period_start, period_end, type, organization_id, multi_currency, tax_services, materiality FROM engagements WHERE id = '{engagement_id}';"])[0]

        return engagement_info.to_dict("records")[0]

    def get_organization_info(self, organization_id: str) -> dict:
        organization_info = self.db_connection.execute_queries(
            [f"SELECT id, financial_year_end_day, financial_year_end_month, business_type FROM organizations WHERE id = '{organization_id}';"])[0]

        return organization_info.to_dict("records")[0]

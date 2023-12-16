# Copyright (c) Streamlit Inc. (2018-2022) Snowflake Inc. (2022)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import pandas as pd
from supabase import Client, create_client

from streamlit.connections import BaseConnection
from streamlit.errors import StreamlitAPIException
from streamlit.runtime.caching import cache_data


class SupabaseConnection(BaseConnection):

    """A connection to Supabase database using supabase python sdk. Initialize using
    ``st.experimental_connection("<name>", type="supabase", project_url="<database url>"),api_key="<api key>"``.
    """

    def __init__(self, connection_name: str, **kwargs) -> None:
        self.client = None
        super().__init__(connection_name, **kwargs)

    def _connect(self, **kwargs):
        try:
            if "project_url" in kwargs and "api_key" in kwargs:
                project_url = kwargs.get("project_url")
                api_key = kwargs.get("api_key")

                self.client = create_client(project_url, api_key)
            else:
                raise Exception("project_url, api_key not provided")
            return self.client
        except Exception as e:
            raise e

    def query(
        self,
        table_name: str,
        colume_list: str,
    ) -> pd.DataFrame:
        """Run a read-only SQL query.
        Parameters
        ----------
        sql : str
            The read-only SQL query to execute.

        Returns
        -------
        pd.DataFrame
            The result of running the query, formatted as a pandas DataFrame.
        Example
        -------
        >>> import streamlit as st
        >>> conn = st.experimental_connection(name="bigquery", type="bigquery",from_service_account_path="service_account.json")
        >>> df = conn.query("SELECT * FROM `bigquery-public-data.covid19_italy.data_by_province` LIMIT 20")

        """
        response = self.client.table(table_name).select(colume_list).execute()
        return pd.DataFrame.from_records(response.data)

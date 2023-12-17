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
        table_name : str
            The table name which needs to be retrieved
        colume_list : str
            List of columns , * for all columns

        Returns
        -------
        pd.DataFrame
            The result of running the query, formatted as a pandas DataFrame.
        Example
        -------
        >>> import streamlit as st
        >>> conn=st.connection(name='supabase',project_url=project_url,api_key=api_key)
        >>> df=conn.query(table_name="covidcases",colume_list="*")



        """
        response = self.client.table(table_name).select(colume_list).execute()
        return pd.DataFrame.from_records(response.data)

import base64
import dataclasses
import datetime
import html
import json
import os
import tempfile
import time
import typing

import pandas as pd
import PyPDF2
import requests
import xmltodict
from loguru import logger

from src.config import DATA_ROOT
from src.pipeline.dtx import dtx

def extract_data(queries : dict, start_date = "01/01/2025", end_date = None) -> dict[str, pd.DataFrame]:
    """"
    Extract data from the specified queries in the queries dictionary.
    Args:
        queries (dict): Dictionary with the queries to be executed.
        start_date (str): Start date of the query in the format "dd/mm/yyyy".
        end_date (str): End date of the query in the format "dd/mm/yyyy". If not provided, defaults to the current date.
    Returns:
        dict[str, pd.DataFrame]: Dictionary with the dataframes of the queries.
    """
    end_date = datetime.datetime.now().strftime("%d/%m/%Y") if not end_date else end_date
    client = dtx()
    dfs = {}
    for query_name in queries.keys():
        # create empty dataframe with the columns of the query
        dfs[query_name] = pd.DataFrame(columns=(list(queries[query_name]["mapping_columnas"].values()) + ["id_donant"]))
        resultado = client.download_data(queries[query_name]["id"], start_date, end_date, queries[query_name]["mapping_columnas"])
        if resultado is None:
            logger.error(f"La consulta {query_name} falló")
        else:
            logger.info(f"La consulta {query_name} se ha ejecutado correctamente")
        # append the result to the dataframe
        dfs[query_name] = pd.concat([dfs[query_name], resultado])
        dfs[query_name].drop(columns=["Codi UCIO"], inplace=True)
    return dfs


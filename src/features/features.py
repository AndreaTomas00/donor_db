import base64
import html
import json
import os
import tempfile
import time
import re
import pandas as pd
import PyPDF2
import requests
import typer
import xmltodict
from loguru import logger
from src.features.utils import load_params
import dataclasses


def extract_form_fields_from_base64(base64_document) -> dict:
    """
    Extract form fields like textboxes and radio buttons from the PDF using PyPDF2.
    """
    pdf_binary = base64.b64decode(base64_document)
    with tempfile.NamedTemporaryFile(delete=True, suffix=".pdf") as temp_pdf_file:
        temp_pdf_file.write(pdf_binary)
        temp_pdf_file.flush()
        with open(temp_pdf_file.name, "rb") as file:
            pdf_reader = PyPDF2.PdfReader(file)
            fields = pdf_reader.get_fields()
            form_data = {}
            if fields:
                for key, field in fields.items():
                    try:
                        field_value = field.value
                        if isinstance(field_value, str):
                            field_value = field_value.encode("latin1").decode("utf-8", "ignore").replace("\r", ".").replace("\n", ".")
                            form_data[key] = field_value
                    except Exception as field_error:
                        print(f"Error decoding field {key}: {field_error}")
                form_data = pd.DataFrame([form_data])
                form_data.rename(columns=replace_dict_ocatt, inplace=True)
                return form_data
            return None  # Return form fields, fallback to metadata if empty


def download_documents(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Download donor summary PDF documents from the RSA web service and return a list of dictionaries.
    :param start_date: Start date for the data extraction. str()
    :param end_date: End date for the data extraction. str()
    :return: DataFrame with the extracted form fields from the PDF documents.

    e.g.:
    download_documents("2021-01-01", "2021-01-31")
    """
    client = dtx()
    data = client.download_data("624505", start_date, end_date, columnas = {"1_1684":"document", "1_1515":"id_donant", "1_881":"Data de la donació"})
    documents_data = pd.DataFrame()
    # Iterate through each row in the DataFrame
    for index, row in data.iterrows():
        base64_document = row["1_1684"]
        if pd.isna(base64_document) or not base64_document.strip():
            print(f"Skipping row {index} due to missing or empty Base64 data.")
            continue
        try:
            form_data = extract_form_fields_from_base64(base64_document)
            form_data["id_donant"] = row["id_donant"]
            form_data["Data de la donació"] = row["Data de la donació"]
            documents_data = pd.concat([documents_data, form_data], ignore_index=True)
        except base64.binascii.Error as e:
            print(f"Error decoding Base64 for donor with Codi_UCIO {row['1_881']} and Data de la donació {row['1_1515']}: {e}")
    documents_data.to_csv("documents.csv", index=False)
    return documents_data
def conditional_update(df, index, column, new_value):
    """
    Updates a specific column in a DataFrame only if the current value is empty (NaN or "NULL").
    
    :param df: DataFrame being updated.
    :param index: Index of the row to update.
    :param column: Column name to update.
    :param new_value: New value to assign if the existing value is empty.
    """
    if pd.isna(df.loc[index, column]).all() or (df.loc[index, column] == "NULL").all():
        df.loc[index, column] = new_value

def ensure_row_exists(  df, codi_ucio, data_donacio):
    """
    Ensures that a row with the given `Codi UCIO` and `Data de la donació` exists in the DataFrame.
    If it does not exist, a new row is appended with default values.
    
    :param df: DataFrame being updated.
    :param codi_ucio: Value for `Codi UCIO`.
    :param data_donacio: Value for `Data de la donació`.
    """
    match_index = df[(df["Codi UCIO"] == codi_ucio) & (df["Data de la donació"] == data_donacio)].index

    if match_index.empty:
        new_row = {col: "NULL" for col in df.columns}
        new_row["Codi UCIO"] = codi_ucio
        new_row["Data de la donació"] = data_donacio
        df.loc[len(df)] = new_row
        return df.index[-1]  # Return index of the newly created row
    return match_index[0]  # Return existing row index

def update_dataframes_with_csv(dfs, csv_path):
    """
    Updates the DataFrames with the information from the CSV file only if the target cell is empty.
    
    :param dfs: Dictionary of DataFrames.
    :param csv_path: Path to the CSV file.
    :return: Updated dictionary of DataFrames.
    """
    csv_df = pd.read_csv(csv_path, sep=';')

    for index, row in csv_df.iterrows():
        codi_ucio = row["Codi UCIO"]
        data_donacio = row["Data de la donació"]
        donante = dfs["evolució"]
        match_index = donante[(donante["Codi UCIO"] == codi_ucio) & (donante["Data de la donació"] == data_donacio)].index

        if not match_index.empty:
            for table_name, df in dfs.items():
                row_index = ensure_row_exists(table_name, df, codi_ucio, data_donacio)
                if table_name in update_rules:
                    rules = update_rules[table_name]
                    for df_column, csv_mapping in rules.items():
                        if isinstance(csv_mapping, str):
                            conditional_update(df, row_index, df_column, row[csv_mapping])
                        elif isinstance(csv_mapping, tuple) and len(csv_mapping) == 2 and callable(csv_mapping[1]):
                            conditional_update(df, row_index, df_column, csv_mapping[1](row[csv_mapping[0]]))
                        elif isinstance(csv_mapping, list):
                            for condition in csv_mapping:
                                if isinstance(condition, tuple) and len(condition) == 3:
                                    if row[condition[0]] == condition[1]:
                                        conditional_update(df, row_index, df_column, condition[2])
                        elif callable(csv_mapping):
                            conditional_update(df, row_index, df_column, csv_mapping(row))
    return dfs  # Return the updated DataFrames dictionary


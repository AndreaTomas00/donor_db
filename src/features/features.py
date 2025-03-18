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
from src.pipeline.dtx import dtx
from src.features.utils import load_params
from src.config import DATA_ROOT
import dataclasses


def extract_form_fields_from_base64(base64_document) -> dict:
    """
    Extract form fields like textboxes and radio buttons from the PDF using PyPDF2.
    """
    replace_dict_ocatt = load_params("extract")["replace_dict_ocatt"]
    pdf_binary = base64.b64decode(base64_document)
    with tempfile.NamedTemporaryFile(delete=True, suffix=".pdf") as temp_pdf_file:
        temp_pdf_file.write(pdf_binary)
        temp_pdf_file.flush()
        
        # Suppress PyPDF2 warnings temporarily
        import warnings
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            
            with open(temp_pdf_file.name, "rb") as file:
                pdf_reader = PyPDF2.PdfReader(file, strict=False)
                try:
                    # Try to get fields, handling potential errors
                    try:
                        fields = pdf_reader.get_fields()
                    except Exception as e:
                        logger.warning(f"Could not get PDF fields: {str(e)}")
                        fields = {}
                    
                    form_data = {}
                    if fields:
                        for key, field in fields.items():
                            try:
                                # Only process keys that are strings
                                if not isinstance(key, str):
                                    continue
                                    
                                # Sanitize key by replacing problematic characters
                                sanitized_key = re.sub(r'[^\x00-\x7F]+', '_', key)
                                
                                field_value = field.value
                                if field_value is None:
                                    # Skip fields with no value
                                    continue
                                    
                                if isinstance(field_value, str):
                                    # Try various encodings to handle the string correctly
                                    try:
                                        field_value = field_value.encode('latin1').decode('utf-8', 'ignore')
                                    except (UnicodeEncodeError, UnicodeDecodeError):
                                        try:
                                            field_value = field_value.encode('utf-8').decode('utf-8', 'ignore')
                                        except (UnicodeEncodeError, UnicodeDecodeError):
                                            # If all else fails, just use the string as is
                                            pass
                                    
                                    field_value = field_value.replace("\r", ".").replace("\n", ".")
                                
                                form_data[sanitized_key] = field_value
                            except Exception as field_error:
                                # Skip problematic fields
                                continue
                        
                        form_data = pd.DataFrame([form_data])
                        form_data.rename(columns=replace_dict_ocatt, inplace=True)
                        return form_data
                    
                    return pd.DataFrame()  # Return empty DataFrame if no fields
                except Exception as e:
                    # Log the error but don't halt processing
                    logger.warning(f"Error processing PDF: {str(e)}")
                    return pd.DataFrame()  # Return empty DataFrame on error


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
    data = client.download_data("624505", start_date, end_date, columns = {"1_1684":"document", "1_1515":"Data de la donació", "1_881":"Codi UCIO"})
    print(f"Downloaded {len(data)} documents.")
    documents_data = pd.DataFrame()
    # Iterate through each row in the DataFrame
    for index, row in data.iterrows():
        base64_document = row["document"]
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
    os.makedirs(DATA_ROOT / "tmp/documents", exist_ok=True)
    documents_data.to_csv(DATA_ROOT/"tmp/documents/documents.csv", index=False)
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


if __name__ == "__main__":
    print(len(download_documents("01/01/2025", "15/01/2025")))
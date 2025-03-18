"""
Module for utility functions used in the project.
"""

import typer
import yaml
import datetime
from datetime import datetime, timedelta
import pandas as pd
from src.config import PROJ_ROOT
app = typer.Typer()

@app.command()
def load_params(stage: str) -> dict:
    """
    Load parameters from the params.yaml configuration file.

    Parameters
    ----------
    stage : str
        Stage of the pipeline (e.g., 'train', 'predict').

    Returns
    -------
    params : dict
        Dictionary of parameters for the specified stage.
    """
    params_path = PROJ_ROOT/"src/params.yaml"
    params = {}

    with open(params_path, "r", encoding="utf-8") as params_file:
        try:
            params = yaml.safe_load(params_file)
            params = params[stage]
        except yaml.YAMLError as exc:
            print(exc)

    return params

def generate_date_pairs(start_date: str, end_date: str):
    """
    Generates a list of date pairs separated by 7 days.

    :param start_date: The start date in the format "DD/MM/YYYY".
    :param end_date: The end date in the format "DD/MM/YYYY".
    :return: A list of tuples containing date pairs.
    """
    start = datetime.strptime(start_date, "%d/%m/%Y")
    end = datetime.strptime(end_date, "%d/%m/%Y")
    
    date_pairs = []
    
    while start + timedelta(days=7) <= end:
        next_date = start + timedelta(days=6)
        date_pairs.append((start.strftime("%d/%m/%Y"), next_date.strftime("%d/%m/%Y")))
        start = next_date + timedelta(days=1)
    
    return date_pairs


import pandas as pd
from loguru import logger

from src.config import DATA_ROOT
from src.features.utils import load_params


def transform_micro(df: pd.DataFrame):
    """
    Transforms the microbiology data from the given DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing microbiology data.

    Returns
    -------
    manteniment_micro : pd.DataFrame
        DataFrame containing common columns.
    cultius_micro : pd.DataFrame
        DataFrame containing transformed culture-specific data.
    """
    transform_params = load_params("transform")
    # Identifying common and culture-specific columns
    common_columns_micro = transform_params["common_columns_micro"]
    culture_mappings = transform_params["culture_mapping"]

    # Transforming the DataFrame
    transformed_rows = []
    manteniment_micro = df[common_columns_micro].copy()

    duplicate_counter = {}

    for _, row in df.iterrows():
        for culture in culture_mappings:
            if culture[2] is not None and pd.notna(row.get(culture[2])):  # If culture exists
                id_micro = row['ID_microbiologia']
                culture_type = culture[0]

                # Create a unique key for this combination
                key = (id_micro, culture_type)

                # Check if this combination already exists
                if key in duplicate_counter:
                    # Increment the counter
                    duplicate_counter[key] += 1
                    # Modify the culture type with a suffix
                    modified_culture_type = f"{culture_type}_{duplicate_counter[key]}"
                else:
                    # First occurrence
                    duplicate_counter[key] = 1
                    modified_culture_type = culture_type

                new_row = {
                    'id_microbiologia': id_micro,
                    'Tipus cultiu': modified_culture_type,
                    'Data/Hora cultiu': row.get(culture[1]) if culture[1] else None,
                    'Resultats_cultiu': row.get(culture[2]) if culture[2] else None,
                    'Resultats_cultiu_string': row.get(culture[3]) if culture[3] else None,
                    'Especifiqueu germens cultiu': row.get(culture[4]) if culture[4] else None,
                    'Tinció de Gram cultiu': row.get(culture[5]) if culture[5] else None,
                    'id_donant': row['id_donant'],
                }
                transformed_rows.append(new_row)

    # Creating the new transformed DataFrame
    cultius_micro = pd.DataFrame(transformed_rows)
    return manteniment_micro, cultius_micro

def transform_imatge(imatge: pd.DataFrame):
    # Columns to be transformed
    transform_params = load_params("transform")
    imatge_mapping = transform_params["imatge_mapping"]

    # Convert dataset from wide format to long format
    normalized_data = []

    for _, row in imatge.iterrows():
        id_donante = row["id_donant"]
        for test, conclusion_col in imatge_mapping.items():
            if test in row and pd.notna(row[test]) and row[test] != "NO REALITZAT":
                estat_prova = row[test]
                resultat = row[conclusion_col] if conclusion_col in row else None
                normalized_data.append([id_donante, test, estat_prova, resultat])

    df = pd.DataFrame(normalized_data, columns=["id_donant", "Prova", "Estat prova", "Resultat"])
    return df

def transform_me(df: pd.DataFrame):
    """
    Transforms the brain death (ME - Mort Encefàlica) data in the DataFrame:
    1. Combines 'Data de la mort ME' and 'Hora de la mort ME' into a new 'data_hora_mort_ME' column
    2. Creates a new 'dx_me' column listing the diagnostic tests that were performed (have 'Sí' value)

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing brain death data

    Returns
    -------
    pd.DataFrame
        Transformed DataFrame with combined datetime and diagnostic test summary
    """
    # Create a copy to avoid modifying the original dataframe
    transformed_df = df.copy()
    # 1. Combine date and time columns for brain death
    # First check if the columns exist
        # For rows where both date and time exist, combine them

    # Initialize the new column
    transformed_df['data_hora_mort_ME'] = None
    # For rows where both date and time exist, combine them
    mask = (~pd.isna(transformed_df['Data de la mort ME'])) & (~pd.isna(transformed_df['Hora de la mort ME']))
    for idx in transformed_df[mask].index:
        date = transformed_df.loc[idx, 'Data de la mort ME']
        time = transformed_df.loc[idx, 'Hora de la mort ME']
        transformed_df.loc[idx, 'data_hora_mort_ME'] = f"{date} {time}"

    # For rows where only date exists, use 00:00 as time
    mask_date_only = (~pd.isna(transformed_df['Data de la mort ME'])) & (pd.isna(transformed_df['Hora de la mort ME']))
    for idx in transformed_df[mask_date_only].index:
        date = transformed_df.loc[idx, 'Data de la mort ME']
        transformed_df.loc[idx, 'data_hora_mort_ME'] = f"{date} 00:00"

    # 2. Create dx_me column
    # List of diagnostic test columns
    diagnostic_columns = [
        "Exploració clínica",
        "Potencials evocats",
        "Electroencefalograma",
        "Doppler transcraneal",
        "Arteriografia cerebral",
        "Angiografia cerebral per TAC",
        "Angiogammagrafia cerebral"
    ]

    # Initialize dx_me as empty string
    transformed_df['dx_me'] = ""

    # For each row, check which diagnostic tests have 'SI' value
    for idx, row in transformed_df.iterrows():
        diagnostic_tests = []

        for col in diagnostic_columns:
            if col in row and row[col] == 'Sí':
                # Add the test name to the list
                test_name = col.split(' (')[0]  # Remove any parentheses if present
                diagnostic_tests.append(test_name)

        # Join all positive tests with semicolons
        transformed_df.loc[idx, 'dx_me'] = '; '.join(diagnostic_tests) if diagnostic_tests else None
    transformed_df.drop(columns=['Data de la mort ME', 'Hora de la mort ME'] + diagnostic_columns, inplace=True)
    return transformed_df

def transform_df(dfs) -> dict[str, pd.DataFrame]:
    """
    Transforms the dataframes into a dense schema format for analytics.

    This function takes a dictionary of dataframes and processes them to create
    a set of denormalized tables optimized for analytics queries.

    Args:
        dfs (dict): Dictionary containing the source dataframes to be transformed.

    Returns:
        dict: Dictionary containing the transformed dataframes in a dense schema
              format with the following tables:
              - donant: Main donor information with merged data from multiple sources
              - analitica: Laboratory test results
              - orina: Urine test results
              - gsa: Blood gas analysis
              - imatge: Imaging studies
              - micro: Microbiology culture results
              - oferta: Organ offer data

    Raises:
        KeyError: If a required dataframe is missing from the input dictionary
        ValueError: If critical columns are missing or data structure is unexpected
    """
    try:
        # Create a new dictionary for transformed dataframes
        db = {}

        # Load transformation parameters
        transform_params = load_params("transform")
        df_to_merge = transform_params["queries_merge"]

        # Log the process start
        logger.info("Starting dense schema transformation")

        # 1. Transform specialized dataframes first
        logger.info("Transforming specialized dataframes")

        # Handle nullable dataframes with safe processing
        if "Microbiologia" in dfs and not dfs["Microbiologia"].empty:
            dfs["Microbiologia_comu"], dfs["Microbiologia_cultius"] = transform_micro(dfs["Microbiologia"])
            logger.info("Microbiologia dataframe transformed")
        else:
            logger.warning("Microbiologia dataframe missing or empty")
            dfs["Microbiologia_comu"] = pd.DataFrame(columns=["id_donant", "Data de la donació"])
            dfs["Microbiologia_cultius"] = pd.DataFrame(columns=["id_donant"])

        if "Imatge" in dfs and not dfs["Imatge"].empty:
            dfs["Imatge"] = transform_imatge(dfs["Imatge"])
            logger.info("Imatge dataframe transformed")
        else:
            logger.warning("Imatge dataframe missing or empty")
            dfs["Imatge"] = pd.DataFrame(columns=["id_donant"])

        if "ME" in dfs and not dfs["ME"].empty:
            dfs["ME"] = transform_me(dfs["ME"])
            logger.info("ME dataframe transformed")
        else:
            logger.warning("ME dataframe missing or empty")
            dfs["ME"] = pd.DataFrame(columns=["id_donant", "Data de la donació"])

        # 2. Process the main donor dataframe
        logger.info("Creating main donor table")

        db["donant"] = dfs["Evolució"].copy()

        # Combine hospital fields
        if "Hospital de procedència" in db["donant"] and "Especifiqueu hospital" in db["donant"]:
            db["donant"]["Hospital de procedència"] = db["donant"].apply(
                lambda row: f"{row['Hospital de procedència']}, {row['Especifiqueu hospital']}"
                if pd.notna(row["Especifiqueu hospital"]) else row["Hospital de procedència"],
                axis=1
            )
            db["donant"].drop(columns=["Especifiqueu hospital"], inplace=True)

        # Process DAC if available
        if "DAC" in dfs and not dfs["DAC"].empty and "Diagnòstic mort en asistòlia" in dfs["DAC"]:
            dfs["DAC"]["Diagnòstic mort en asistòlia"] = dfs["DAC"].apply(
                lambda row: f"{row['Diagnòstic mort en asistòlia']}, {row['Diagnòstic mort en asistòlia 2']}"
                if pd.notna(row.get("Diagnòstic mort en asistòlia 2", None)) else row["Diagnòstic mort en asistòlia"],
                axis=1
            )
            dfs["DAC"].drop(columns=["Diagnòstic mort en asistòlia 2"], inplace=True, errors='ignore')

        # 3. Merge tables according to the configuration
        logger.info(f"Merging {len(df_to_merge)} dataframes into the donor table")
        for df_name in df_to_merge:
            if df_name in dfs and not dfs[df_name].empty:
                if "id_donant" in dfs[df_name] and "Data de la donació" in dfs[df_name]:
                    db["donant"] = db["donant"].merge(
                        dfs[df_name],
                        on=["id_donant", "Data de la donació"],
                        how="left",
                        suffixes=('', f'_{df_name}')
                    )
                    logger.info(f"Merged {df_name} into donor table")
                else:
                    logger.warning(f"Cannot merge {df_name}: Missing required join columns")
            else:
                logger.warning(f"DataFrame {df_name} not found or empty, skipping merge")

        # Ensure id_donant is the first column
        if 'id_donant' in db["donant"].columns:
            cols = ['id_donant'] + [col for col in db["donant"].columns if col != 'id_donant']
            db["donant"] = db["donant"][cols]

        # 4. Create secondary tables
        logger.info("Creating secondary tables")

        # Helper function to safely drop columns and handle missing dataframes
        def create_table(source_name, target_name=None, drop_cols=None):
            target_name = target_name or source_name.lower()
            if source_name in dfs and not dfs[source_name].empty:
                if drop_cols:
                    db[target_name] = dfs[source_name].drop(columns=drop_cols, errors='ignore')
                else:
                    db[target_name] = dfs[source_name].copy()
                logger.info(f"Created {target_name} table with {len(db[target_name])} rows")
            else:
                logger.warning(f"{source_name} dataframe missing or empty")
                db[target_name] = pd.DataFrame(columns=["id_donant"])

        # Create tables with simple transformations
        create_table("Analítica", "analitica", ["Data de la donació"])
        create_table("Orina", "orina", ["Data de la donació"])
        create_table("GSA", "gsa", ["Data de la donació"])
        create_table("Imatge", "imatge")
        create_table("Microbiologia_cultius", "micro")
        create_table("Oferta", "oferta", ["Data de la donació"])

        logger.info("Dense schema transformation completed successfully")
        return db

    except Exception as e:
        logger.error(f"Error in transform_df_dense: {str(e)}")
        # Return an empty dictionary or the partially completed one
        return db if 'db' in locals() else {}
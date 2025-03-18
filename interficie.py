import streamlit as st
import pandas as pd
import shutil
import os
import datetime
import time
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

from src.pipeline.extract import extract_data
from src.pipeline.transform import transform_df_dense, transform_df_star
from src.pipeline.load import insert_data
from src.features.utils import load_params
from src.config import DATA_ROOT

# Load parameters
extract_params = load_params("extract")
queries = extract_params["consultas"]

# Set up page configuration
st.set_page_config(
    page_title="Donor DB Clinic",
    page_icon="🏥",
    layout="wide"
)

# Create header with logo
st.markdown("""
<div style="background-color:#1E88E5;padding:10px;border-radius:10px">
    <h1 style="color:white;text-align:center">🏥 DONOR DB CLINIC</h1>
    <p style="color:white;text-align:center">Pipeline ETL para la gestión de datos de donantes</p>
</div>
""", unsafe_allow_html=True)

# Create tabs for different functions
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Extracción", 
    "🔄 Transformación", 
    "💾 Carga", 
    "📄 Documentos", 
    "🚀 Pipeline Completo"
])

def run_extract():
    st.header("Extracción de Datos")
    st.write("Extraer datos desde el web service de RSA.")
    
    # Date selection
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Fecha de inicio:",
            datetime.date(2025, 1, 1),
            key="extract_start_date"
        )
    with col2:
        end_date = st.date_input(
            "Fecha de fin:",
            datetime.date.today(),
            key="extract_end_date"
        )
    
    start_date_str = start_date.strftime("%d/%m/%Y")
    end_date_str = end_date.strftime("%d/%m/%Y")
    
    # Display available queries in a table
    st.subheader("Consultas Disponibles")
    
    # Create dataframe for display
    queries_df = pd.DataFrame({
        "Consulta": list(queries.keys()),
    })
    st.dataframe(queries_df, use_container_width=True)
    
    # Query selection
    selected_queries = st.multiselect(
        "Seleccione las consultas a extraer:",
        options=list(queries.keys()),
        default=list(queries.keys())
    )
    
    # Filter queries dictionary based on selected values
    filtered_queries = {k: queries[k] for k in selected_queries if k in queries}
    
    if st.button("Extraer Datos", key="extract_button"):
        if not filtered_queries:
            st.error("Por favor, seleccione al menos una consulta.")
            return
            
        progress_text = "Extrayendo datos..."
        my_bar = st.progress(0, text=progress_text)
        
        try:
            # Extract data for each query
            dfs = {}
            for i, (name, query) in enumerate(filtered_queries.items()):
                status_text = st.empty()
                status_text.info(f"Extrayendo {name}...")
                
                try:
                    result = extract_data(queries={name: query}, start_date=start_date_str, end_date=end_date_str)
                    dfs.update(result)
                    my_bar.progress((i+1)/len(filtered_queries), text=f"Procesado: {i+1}/{len(filtered_queries)}")
                except Exception as e:
                    st.error(f"Error al extraer datos de {name}: {str(e)}")
            
            # Save to session state for other tabs
            st.session_state.dfs = dfs
            
            # Display results
            if dfs:
                st.success("¡Datos extraídos correctamente!")
                
                # Create statistics
                stats = {name: len(df) for name, df in dfs.items()}
                
                # Create a bar chart
                fig, ax = plt.subplots(figsize=(10, 5))
                sns.barplot(x=list(stats.keys()), y=list(stats.values()), ax=ax)
                ax.set_title("Número de registros por consulta")
                ax.set_xlabel("Consulta")
                ax.set_ylabel("Registros")
                plt.xticks(rotation=45, ha="right")
                plt.tight_layout()
                st.pyplot(fig)
                
                # Show preview of each dataframe
                for name, df in dfs.items():
                    with st.expander(f"Vista previa: {name} ({len(df)} registros)"):
                        st.dataframe(df.head(), use_container_width=True)
                
                # Save dataframes to disk
                save_data = st.checkbox("Guardar datos extraídos", value=True)
                if save_data:
                    os.makedirs(DATA_ROOT / "queries", exist_ok=True)
                    for name, df in dfs.items():
                        df.to_csv(DATA_ROOT / "queries" / f"{name}.csv")
                    st.info("Datos guardados en ./queries/")
            else:
                st.warning("No se obtuvieron datos. Intente con otras consultas o fechas.")
                
        except Exception as e:
            st.error(f"Error inesperado: {str(e)}")

def run_transform():
    st.header("Transformación de Datos")
    
    # Check if data is available
    if 'dfs' not in st.session_state:
        st.warning("No hay datos para transformar. Por favor, primero extraiga los datos.")
        if st.button("Cargar datos de archivo"):
            try:
                dfs = {}
                os.makedirs(DATA_ROOT / "queries", exist_ok=True)
                files = list(Path(DATA_ROOT / "queries").glob("*.pkl"))
                if files:
                    for file in files:
                        name = file.stem
                        dfs[name] = pd.read_pickle(file)
                    st.session_state.dfs = dfs
                    st.success(f"Se cargaron {len(dfs)} archivos.")
                else:
                    st.error("No se encontraron archivos guardados.")
            except Exception as e:
                st.error(f"Error al cargar archivos: {str(e)}")
        return
    
    # Display data summary
    st.subheader("Datos disponibles para transformar")
    data_summary = {name: len(df) for name, df in st.session_state.dfs.items()}
    summary_df = pd.DataFrame(list(data_summary.items()), columns=["Consulta", "Registros"])
    st.dataframe(summary_df, use_container_width=True)
    
    # Transformation type selection
    transform_type = st.radio(
        "Seleccione el tipo de transformación:",
        ["Diseño Denso (tablas densas)", "Diseño Estrella (normalizado)"],
        key="transform_type"
    )
    
    if st.button("Transformar Datos", key="transform_button"):
        progress_text = "Transformando datos..."
        my_bar = st.progress(0, text=progress_text)
        
        try:
            # Transform data
            if transform_type == "Diseño Denso (tablas densas)":
                st.info("Aplicando transformación de tipo Dense...")
                transformed_dfs = transform_df_dense(st.session_state.dfs)
                transform_name = "dense"
            else:
                st.info("Aplicando transformación de tipo Star...")
                transformed_dfs = transform_df_star(st.session_state.dfs)
                transform_name = "star"
            
            my_bar.progress(80, text="Guardando resultados...")
            
            # Save to session state
            st.session_state.transformed_dfs = transformed_dfs
            st.session_state.transform_type = transform_name
            
            # Save transformed dataframes
            os.makedirs(DATA_ROOT / "transformed", exist_ok=True)
            for name, df in transformed_dfs.items():
                df.to_pickle(DATA_ROOT / "transformed" / f"{name}_{transform_name}.pkl")
                df.to_csv(DATA_ROOT / "transformed" / f"{name}_{transform_name}.csv")
            
            my_bar.progress(100, text="Transformación completada")
            st.success("¡Datos transformados correctamente!")
            
            # Show statistics in a table
            st.subheader("Resumen de datos transformados")
            
            # General metrics table
            metrics_data = {
                "Tipo de Transformación": transform_name,
                "Consultas procesadas": len(st.session_state.dfs),
                "Tablas creadas": len(transformed_dfs),
                "Total de registros": sum(len(df) for df in transformed_dfs.values())
            }
            metrics_df = pd.DataFrame(metrics_data.items(), columns=["Métrica", "Valor"])
            st.dataframe(metrics_df, use_container_width=True)
            
            # Detailed table
            details_data = []
            for name, df in transformed_dfs.items():
                details_data.append({
                    "Tabla": name,
                    "Registros": len(df),
                    "Columnas": len(df.columns)
                })
            details_df = pd.DataFrame(details_data)
            st.dataframe(details_df, use_container_width=True)
            
            # Show preview of each transformed DataFrame
            for name, df in transformed_dfs.items():
                with st.expander(f"Vista previa: {name} ({len(df)} registros)"):
                    st.dataframe(df.head(), use_container_width=True)
                    
        except Exception as e:
            st.error(f"Error durante la transformación: {str(e)}")

def run_load():
    st.header("Carga de Datos")
    
    # Check if transformed data is available
    if 'transformed_dfs' not in st.session_state:
        st.warning("No hay datos transformados para cargar. Por favor, primero transforme los datos.")
        try_load = st.checkbox("Intentar cargar datos transformados desde archivos")
        if try_load:
            transform_type = st.radio(
                "Tipo de transformación a cargar:",
                ["dense", "star"],
                key="load_transform_type"
            )
            if st.button("Cargar datos transformados", key="load_saved_data"):
                try:
                    transformed_dfs = {}
                    files = list(Path(DATA_ROOT / "transformed").glob(f"*_{transform_type}.pkl"))
                    if files:
                        for file in files:
                            name = file.stem.replace(f"_{transform_type}", "")
                            transformed_dfs[name] = pd.read_pickle(file)
                        st.session_state.transformed_dfs = transformed_dfs
                        st.session_state.transform_type = transform_type
                        st.success(f"Se cargaron {len(transformed_dfs)} tablas transformadas.")
                    else:
                        st.error(f"No se encontraron archivos transformados de tipo {transform_type}.")
                except Exception as e:
                    st.error(f"Error al cargar datos transformados: {str(e)}")
        return
    
    # Display transformation summary
    st.subheader(f"Datos Transformados ({st.session_state.transform_type})")
    transform_summary = {name: len(df) for name, df in st.session_state.transformed_dfs.items()}
    summary_df = pd.DataFrame(list(transform_summary.items()), columns=["Tabla", "Registros"])
    st.dataframe(summary_df, use_container_width=True)
    
    # Database configuration
    with st.expander("Configuración de Base de Datos", expanded=True):
        st.info("Utilizando configuración por defecto de la base de datos.")
    
    if st.button("Cargar Datos a Base de Datos", key="load_to_db_button"):
        progress_text = "Cargando datos a la base de datos..."
        my_bar = st.progress(0, text=progress_text)
        
        try:
            # Execute load process
            st.info("Creando tablas en la base de datos...")
            my_bar.progress(20, text="Creando tablas...")
            
            # Call insert_data which handles both table creation and data insertion
            insert_data()
            
            my_bar.progress(100, text="Carga completada")
            st.success("¡Datos cargados correctamente a la base de datos!")
            
        except Exception as e:
            st.error(f"Error al cargar datos: {str(e)}")

def run_download_documents():
    st.header("Descarga de Documentos")
    st.write("Descarga documentos desde el web service de RSA.")
    
    # Date selection
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Fecha de inicio:",
            datetime.date(2025, 1, 1),
            key="doc_start_date"
        )
    with col2:
        end_date = st.date_input(
            "Fecha de fin:",
            datetime.date.today(),
            key="doc_end_date"
        )
    
    start_date_str = start_date.strftime("%d/%m/%Y")
    end_date_str = end_date.strftime("%d/%m/%Y")
    
    if st.button("Descargar Documentos", key="download_docs_button"):
        progress_text = "Descargando documentos..."
        my_bar = st.progress(0, text=progress_text)
        
        try:
            # Import here to avoid circular imports
            from src.pipeline.dtx import dtx
            dtx_client = dtx()
            
            # Download documents
            dtx_client.download_documents(start_date_str, end_date_str)
            my_bar.progress(100, text="Descarga completada")
            
            # Check if documents were downloaded
            documents_dir = Path(DATA_ROOT) / "Documents"
            documents = list(documents_dir.glob("*.pdf"))
            
            if documents:
                st.success(f"¡{len(documents)} documentos descargados correctamente!")
                
                # Create table showing documents
                doc_data = []
                for doc in documents[:10]:  # Show first 10 documents
                    doc_data.append({
                        "Nombre": doc.name,
                        "Tamaño (KB)": f"{doc.stat().st_size / 1024:.1f}"
                    })
                
                if len(documents) > 10:
                    st.info(f"Mostrando 10 de {len(documents)} documentos")
                    
                doc_df = pd.DataFrame(doc_data)
                st.dataframe(doc_df, use_container_width=True)
                
            else:
                st.warning("No se encontraron documentos para descargar.")
                
        except Exception as e:
            st.error(f"Error al descargar documentos: {str(e)}")

def run_pipeline():
    st.header("Pipeline Completo")
    st.write("Ejecuta el pipeline completo de ETL.")
    
    # Pipeline configuration
    pipeline_type = st.radio(
        "Seleccione el tipo de pipeline:",
        ["dense", "star"],
        key="pipeline_type_radio"
    )
    
    # Date selection
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Fecha de inicio:",
            datetime.date(2025, 1, 1),
            key="pipeline_start_date"
        )
    with col2:
        end_date = st.date_input(
            "Fecha de fin:",
            datetime.date.today(),
            key="pipeline_end_date"
        )
    
    start_date_str = start_date.strftime("%d/%m/%Y")
    end_date_str = end_date.strftime("%d/%m/%Y")
    
    if st.button("Ejecutar Pipeline Completo", key="run_pipeline_button"):
        try:
            # Set up progress tracking
            progress_container = st.container()
            status_container = st.container()
            
            with progress_container:
                progress_text = "Iniciando pipeline..."
                overall_progress = st.progress(0, text=progress_text)
                step_progress = st.progress(0, text="")
            
            # Step 1: Extract data
            with status_container:
                st.info("Paso 1: Extrayendo datos...")
            
            step_progress.progress(0, text="Extrayendo datos...")
            
            try:
                dfs = {}
                for i, (name, query) in enumerate(queries.items()):
                    status_text = f"Extrayendo datos de {name}..."
                    step_progress.progress((i+1)/len(queries), text=status_text)
                    
                    try:
                        result = extract_data(queries={name: query}, start_date=start_date_str, end_date=end_date_str)
                        dfs.update(result)
                    except Exception as e:
                        with status_container:
                            st.error(f"Error al extraer datos de {name}: {str(e)}")
                
                # Save dataframes for future use
                os.makedirs(DATA_ROOT / "tmp", exist_ok=True)
                for name, df in dfs.items():
                    df.to_pickle(DATA_ROOT / "tmp" / f"{name}.pkl")
                
                with status_container:
                    st.success("✓ Datos extraídos correctamente")
                
                overall_progress.progress(30, text="Extracción completada")
                
            except Exception as e:
                with status_container:
                    st.error(f"Error en extracción: {str(e)}")
                return
            
            # Step 2: Transform data
            with status_container:
                st.info("Paso 2: Transformando datos...")
            
            step_progress.progress(0, text="Transformando datos...")
            
            try:
                # Apply transformation based on selected type
                step_progress.progress(30, text="Aplicando transformación...")
                
                if pipeline_type == "dense":
                    transformed_dfs = transform_df_dense(dfs)
                else:
                    transformed_dfs = transform_df_star(dfs)
                
                step_progress.progress(70, text="Guardando datos transformados...")
                
                # Save transformed dataframes
                os.makedirs(DATA_ROOT / "transformed", exist_ok=True)
                for name, df in transformed_dfs.items():
                    df.to_pickle(DATA_ROOT / "transformed" / f"{name}_{pipeline_type}.pkl")
                
                step_progress.progress(100, text="Transformación completada")
                with status_container:
                    st.success("✓ Datos transformados correctamente")
                
                overall_progress.progress(60, text="Transformación completada")
                
            except Exception as e:
                with status_container:
                    st.error(f"Error en transformación: {str(e)}")
                return
            
            # Step 3: Load data
            with status_container:
                st.info("Paso 3: Cargando datos...")
            
            step_progress.progress(0, text="Cargando datos a la base de datos...")
            
            try:
                # Insert data to database
                insert_data(transformed_dfs)
                
                step_progress.progress(100, text="Carga completada")
                with status_container:
                    st.success("✓ Datos cargados correctamente")
                
                overall_progress.progress(100, text="Pipeline completado")
                
            except Exception as e:
                with status_container:
                    st.error(f"Error al cargar datos: {str(e)}")
                return
            
            # Display pipeline results
            st.subheader("Resumen del Pipeline")
            
            # General metrics table
            metrics_data = {
                "Tipo de Pipeline": pipeline_type,
                "Consultas procesadas": len(dfs),
                "Tablas creadas": len(transformed_dfs),
                "Total de registros": sum(len(df) for df in transformed_dfs.values())
            }
            metrics_df = pd.DataFrame(metrics_data.items(), columns=["Métrica", "Valor"])
            st.dataframe(metrics_df, use_container_width=True)
            
            # Detailed table showing records per table
            details_data = []
            for name, df in transformed_dfs.items():
                details_data.append({
                    "Tabla": name,
                    "Registros": len(df),
                    "Columnas": len(df.columns)
                })
            details_df = pd.DataFrame(details_data)
            st.dataframe(details_df, use_container_width=True)
            shutil.rmtree(DATA_ROOT / "tmp", ignore_errors=True)
            shutil.rmtree(DATA_ROOT / "transformed", ignore_errors=True)
        except Exception as e:
            st.error(f"Error inesperado en el pipeline: {str(e)}")

# Run the appropriate function based on the selected tab
with tab1:
    run_extract()

with tab2:
    run_transform()

with tab3:
    run_load()

with tab4:
    run_download_documents()

with tab5:
    run_pipeline()
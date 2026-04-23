"""
====================================================================================================
SISTEMA GERENCIAL DE EFICIENCIA OPERATIVA (OEE) Y ANALÍTICA DE PRODUCCIÓN
Cliente / Área: Producción - Carga de Detonadores (Máquina 219)
Empresa: CAVA ROBOTICS
Versión: 7.1.1 (Build Filtros Granulares de Paradas - Módulo 2 Pareto Extendido + COD/Sistemas)

Módulos Integrados:
    1. CoreLogger: Trazabilidad, auditoría y manejo de excepciones silenciosas.
    2. DataProcessor & ETL: Escáner de Offset, Filtrado Clínico de Fechas y Autocorrección.
    3. QualityControl: Diagnóstico experto y generación de insights en lenguaje natural.
    4. BusinessLogic: Motor matemático de extracción directa de OEE (CAPS), Paradas y Volumetría.
    5. PlotlyEngine: Motor vectorial (Gauges, Pareto, Timeline, Tendencias, Matrices).
    6. PDFManager: Generador FPDF A4 Multisección (Portada, Resumen, Tablas, Gráficos de Alta Fidelidad).
    7. ExcelExporter: Exportador de data purificada para respaldos locales y auditorías.
    8. TelegramGateway: Capa de transmisión API segura con reintentos automáticos.
    9. DashboardUI: Orquestador UI con Pestañas (Tabs), Smart Defaults y Filtros de Categoría/Causa/COD/Sistemas.
====================================================================================================
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import os
import time
import logging
import warnings
from io import BytesIO
from datetime import datetime, timedelta, date

# --------------------------------------------------------------------------------------------------
# 1. SUPRESIÓN DE ADVERTENCIAS Y CONFIGURACIÓN DE ENTORNO
# --------------------------------------------------------------------------------------------------
# Mantenemos la terminal limpia de alertas generadas por librerías de terceros (openpyxl, dateutil)
# Esto garantiza una experiencia de usuario limpia y profesional sin ruido en la consola.
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
warnings.filterwarnings("ignore", message=".*Could not infer format.*")

try:
    from office365.sharepoint.client_context import ClientContext
    from office365.runtime.auth.user_credential import UserCredential
    from office365.sharepoint.files.file import File
    SHAREPOINT_AVAILABLE = True
except ImportError:
    SHAREPOINT_AVAILABLE = False

from fpdf import FPDF

# ==================================================================================================
# 2. CONFIGURACIÓN GLOBAL E INSTITUCIONAL DE LA PLATAFORMA (UI / UX)
# ==================================================================================================
st.set_page_config(
    page_title="Dashboard Gerencial OEE - Máquina 219",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inyección de CSS Avanzado: Estética gerencial, colores cálidos, tipografía institucional corporativa
# Se utiliza un sistema de variables CSS para mantener consistencia visual en toda la aplicación.
# Los colores institucionales de CAVA Robotics son el azul profundo (#0A2540) y el dorado/bronce (#C07F00).
st.markdown("""
    <style>
    /* Variables de Tema Institucional CAVA Robotics */
    :root {
        --primary-color: #0A2540;       /* Azul institucional corporativo (Profundo) */
        --secondary-color: #C07F00;     /* Dorado/Bronce cálido (Acentos) */
        --accent-warm: #8B4513;         /* Marrón cálido (SaddleBrown) para detalles */
        --bg-color: #F4F7F6;            /* Fondo general claro industrial */
        --card-bg: #FFFFFF;             /* Fondo de tarjetas de métricas */
        --text-main: #C07F00;           /* Texto principal oscuro */
        --text-muted: #7F8C8D;          /* Texto secundario / Leyendas */
        --success-color: #2E8B57;       /* Verde institucional para métricas óptimas */
        --danger-color: #C0392B;        /* Rojo para alertas y paradas críticas */
        --warning-color: #D35400;       /* Naranja cálido para advertencias */
        --border-color: #E2E8F0;        /* Color de bordes sutiles */
    }

    /* Reseteo y Fondos Globales */
    .main { 
        background-color: var(--bg-color); 
        font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; 
    }
    .stApp { background-color: var(--bg-color); }

    /* Tipografía Corporativa Jerárquica */
    h1, h2, h3, h4, h5, h6 { 
        color: var(--primary-color); 
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
        font-weight: 700;
        letter-spacing: -0.5px;
    }

    /* Contenedores de Tarjetas de Métricas (Cards) */
    .metric-container { 
        background-color: var(--card-bg); 
        border-top: 5px solid var(--secondary-color);
        border-radius: 10px; 
        padding: 25px 20px; 
        text-align: center; 
        box-shadow: 0 6px 15px rgba(0,0,0,0.05); 
        transition: transform 0.3s ease, box-shadow 0.3s ease;
        margin-bottom: 20px;
        position: relative;
        overflow: hidden;
    }
    .metric-container:hover {
        transform: translateY(-5px);
        box-shadow: 0 10px 20px rgba(0,0,0,0.1); 
    }
    .metric-title { 
        font-size: 1.15rem; 
        color: var(--text-muted); 
        font-weight: 600; 
        margin-bottom: 10px; 
        text-transform: uppercase;
        letter-spacing: 0.8px;
    }
    .metric-value { 
        font-size: 2.6rem; 
        color: var(--primary-color); 
        font-weight: 800; 
        line-height: 1.2;
    }
    .metric-subtitle {
        font-size: 0.95rem;
        color: var(--text-muted);
        margin-top: 5px;
        font-style: italic;
    }

    /* Panel de Información Relevante (Cabecera) */
    .info-box {
        background: linear-gradient(135deg, #FFFFFF 0%, #FFF8E1 100%);
        border-left: 8px solid var(--secondary-color);
        padding: 20px 25px;
        border-radius: 8px;
        margin-bottom: 30px;
        color: var(--text-main);
        box-shadow: 0 4px 10px rgba(0,0,0,0.04);
    }
    .info-box h4 {
        margin-top: 0;
        color: var(--accent-warm);
        display: flex;
        align-items: center;
        gap: 12px;
        font-size: 1.4rem;
        margin-bottom: 12px;
    }
    .info-box p { 
        margin: 5px 0; 
        font-size: 1.1rem; 
        display: flex;
        align-items: center;
        flex-wrap: wrap;
    }

    /* Botones Institucionales de Acción */
    .stButton>button { 
        background-color: var(--primary-color); 
        color: #FFFFFF; 
        font-weight: 700; 
        font-size: 1.1rem;
        border-radius: 6px; 
        width: 100%; 
        transition: all 0.3s ease;
        border: none;
        padding: 12px 24px;
        box-shadow: 0 4px 6px rgba(10, 37, 64, 0.2);
    }
    .stButton>button:hover { 
        background-color: var(--secondary-color); 
        color: #FFFFFF;
        box-shadow: 0 6px 12px rgba(192, 127, 0, 0.3);
        transform: translateY(-2px);
    }

    /* Tablas y DataFrames de Streamlit */
    .stDataFrame { 
        background-color: var(--card-bg); 
        border-radius: 8px; 
        border: 1px solid var(--border-color);
        box-shadow: 0 2px 8px rgba(0,0,0,0.03);
    }

    /* Separadores horizontales */
    hr { 
        border-top: 2px solid var(--border-color); 
        margin: 2.5rem 0; 
    }

    /* Personalización del Panel Lateral (Sidebar) */
    [data-testid="stSidebar"] {
        background-color: #FFFFFF;
        border-right: 1px solid var(--border-color);
        padding-top: 1rem;
    }
    [data-testid="stSidebar"] h2 {
        color: var(--primary-color);
        font-size: 1.3rem;
        border-bottom: 2px solid var(--secondary-color);
        padding-bottom: 5px;
        margin-bottom: 15px;
    }

    /* Tarjetas de auditoria e insights */
    .audit-card {
        background-color: #F8F9FA;
        border: 1px solid #E2E8F0;
        border-radius: 5px;
        padding: 12px 18px;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        font-size: 1.05rem;
        color: #2C3E50;
        margin-bottom: 8px;
        border-left: 4px solid var(--primary-color);
    }

    /* Pestañas de Streamlit (Tabs) Styling Corporativo */
    .stTabs [data-baseweb="tab-list"] { 
        gap: 8px; 
    }
    .stTabs [data-baseweb="tab"] { 
        height: 55px; 
        white-space: pre-wrap; 
        background-color: #F0F2F6; 
        border-radius: 6px 6px 0px 0px; 
        padding-top: 12px; 
        padding-bottom: 12px; 
        color: var(--text-main); 
        font-weight: 700;
        font-size: 1.1rem;
        transition: all 0.3s ease;
    }
    .stTabs [aria-selected="true"] { 
        background-color: var(--primary-color); 
        color: white; 
        border-bottom: 4px solid var(--secondary-color); 
    }

    /* Logo Institucional Nativo CSS (Evita links rotos) */
    .cava-logo-container {
        background: linear-gradient(135deg, var(--primary-color) 0%, #1A3A5A 100%);
        padding: 25px 20px; 
        text-align: center; 
        border-radius: 10px; 
        margin-bottom: 25px; 
        border-bottom: 5px solid var(--secondary-color);
        box-shadow: 0 4px 10px rgba(0,0,0,0.15);
    }
    .cava-logo-title { 
        color: var(--secondary-color); 
        margin: 0; 
        font-family: 'Arial Black', Impact, sans-serif; 
        font-size: 32px; 
        letter-spacing: 2px; 
        text-shadow: 1px 1px 2px rgba(0,0,0,0.3);
    }
    .cava-logo-subtitle { 
        color: #FFFFFF; 
        margin: 5px 0 0 0; 
        font-size: 12px; 
        letter-spacing: 4px; 
        font-family: 'Segoe UI', Tahoma, sans-serif; 
        opacity: 0.95; 
        font-weight: 600;
    }
    </style>
""", unsafe_allow_html=True)


# ==================================================================================================
# 3. CONSTANTES GLOBALES Y PARÁMETROS TÉCNICOS OPERATIVOS
# ==================================================================================================
class AppConfig:
    """Contenedor estático para configuraciones empresariales y credenciales del sistema."""
    # Credenciales API Telegram (Entorno de Producción Segura)
    # Estas credenciales son sensibles y deben manejarse con extrema precaución.
    # En un entorno de producción real, deberían almacenarse en variables de entorno.
    TELEGRAM_TOKEN = "8552261657:AAFdXG5ta6UUPyrSco2tqgvNFTTH_LGZw9M"
    TELEGRAM_CHAT_ID = "6153139566"

    # Especificaciones Técnicas Constantes - Máquina 219
    # Estos valores son críticos para el cálculo de la producción nominal y la capacidad teórica.
    MAQUINA_ID = "219"
    MAQUINA_NOMBRE = "Carga de Detonadores(219)"
    CAPACIDAD_PLACAS_HORA = 268
    DETONADORES_POR_PLACA = 40
    PRODUCCION_NOMINAL_HORA = CAPACIDAD_PLACAS_HORA * DETONADORES_POR_PLACA # 10,720 det/hora

    # Directorios del Sistema para Almacenamiento Temporal de Exportaciones
    # Se utilizan para almacenar reportes PDF, imágenes temporales y logs del sistema.
    TEMP_DIR = "temp_reports"
    LOGS_DIR = "system_logs"

    @staticmethod
    def initialize_environment():
        """Verifica y crea la arquitectura de directorios locales necesarios de forma robusta."""
        try:
            if not os.path.exists(AppConfig.TEMP_DIR):
                os.makedirs(AppConfig.TEMP_DIR)
            if not os.path.exists(AppConfig.LOGS_DIR):
                os.makedirs(AppConfig.LOGS_DIR)
        except Exception as e:
            st.error(f"Fallo crítico al crear directorios de sistema: {e}")

# Inicializar el entorno físico en el servidor local
# Esta llamada asegura que los directorios necesarios existan antes de cualquier operación de I/O.
AppConfig.initialize_environment()


# ==================================================================================================
# 4. MÓDULO DE LOGGING Y TRAZABILIDAD (CORE LOGGER)
# ==================================================================================================
class LogManager:
    """
    Sistema robusto de registro de eventos del sistema (Auditoría Gerencial).
    Escribe tanto en la terminal de ejecución como en un archivo físico de trazabilidad.
    El formato de log incluye timestamp, nivel de severidad y prefijo institucional CAVA_CORE.
    """
    log_file_path = os.path.join(AppConfig.LOGS_DIR, f"cava_core_{datetime.now().strftime('%Y%m')}.log")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - [%(levelname)s] - CAVA_CORE: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)

    @staticmethod
    def info(msg): LogManager.logger.info(msg)

    @staticmethod
    def warning(msg): LogManager.logger.warning(msg)

    @staticmethod
    def error(msg): LogManager.logger.error(msg)


# ==================================================================================================
# 5. MOTOR DE EXTRACCIÓN Y LIMPIEZA DE DATOS (ETL CLÍNICO AVANZADO)
# ==================================================================================================
class DataProcessor:
    """
    Clase de grado empresarial para el procesamiento, limpieza, validación 
    y estandarización clínica de DataFrames provenientes de archivos Excel industriales.
    Implementa algoritmos de búsqueda jerárquica y tolerancia a errores de formato.
    """

    @staticmethod
    def find_true_header_index(excel_file, sheet_name, keywords):
        """
        [MOTOR DE ESCANEO DE OFFSET]
        Analiza las primeras filas del Excel para identificar dónde comienzan realmente 
        las cabeceras, ignorando logos, espacios vacíos, metadatos o títulos de reporte.

        Args:
            excel_file: Objeto ExcelFile de pandas.
            sheet_name: Nombre de la hoja a analizar.
            keywords: Lista de palabras clave que deben aparecer en la fila de cabecera.

        Returns:
            int: Índice de la fila donde se encuentra la cabecera real.
        """
        try:
            # Leer las primeras 35 filas en crudo para ubicar la cabecera real
            df_raw = excel_file.parse(sheet_name, header=None, nrows=35)

            for idx, row in df_raw.iterrows():
                # Exigencia estricta: Debemos encontrar las palabras exactas en la fila
                match_count = 0
                for kw in keywords:
                    kw_upper = str(kw).upper()
                    for val in row.values:
                        # Conversión explícita a string para blindar el entorno de tipo float (NaN)
                        val_str = str(val).strip().upper()
                        # Búsqueda exacta y tolerante a espacios adyacentes
                        if kw_upper == val_str or f"{kw_upper} " in val_str or f" {kw_upper}" in val_str:
                            match_count += 1
                            break # No contar la misma palabra dos veces

                # Criterio de confirmación: Si encontramos coincidencias críticas, es la cabecera
                if match_count >= 1:
                    LogManager.info(f"Offset detectado en hoja '{sheet_name}'. Cabecera real en fila index {idx}.")
                    return idx

            LogManager.warning(f"No se detectó un patrón claro de cabecera en '{sheet_name}'. Asumiendo índice 0 por defecto.")
            return 0 
        except Exception as e:
            LogManager.error(f"Error en escáner de offset para '{sheet_name}': {e}. Se asume cabecera 0.")
            return 0

    @staticmethod
    def clean_column_names(df):
        """
        Purifica las cabeceras eliminando espacios en blanco invisibles al inicio y final,
        y elimina saltos de línea (\n, \r) originados por exportaciones crudas de SCADA.

        Args:
            df: DataFrame de pandas con nombres de columna potencialmente sucios.

        Returns:
            DataFrame: Mismo DataFrame con nombres de columna limpios.
        """
        if df is not None and not df.empty:
            df.columns = df.columns.astype(str).str.strip().str.replace('\n', ' ').str.replace('\r', '')
        return df

    @staticmethod
    def find_column_exact_or_partial(df, keywords_exact, keywords_partial=None):
        """
        Algoritmo de búsqueda jerárquica para ubicar columnas aunque cambien levemente de nombre:
        Fase 1: Busca coincidencia exacta (ignorando mayúsculas y espacios). Ej: "DATE"
        Fase 2: Si no encuentra exacta, busca coincidencia parcial. Ej: "FECHA DE CARGA"

        Args:
            df: DataFrame a inspeccionar.
            keywords_exact: Lista de palabras clave para búsqueda exacta.
            keywords_partial: Lista de palabras clave para búsqueda parcial (opcional).

        Returns:
            str or None: Nombre de la columna encontrada o None si no hay coincidencia.
        """
        if df is None or df.empty:
            return None

        # Fase 1: Búsqueda Exacta (Máxima precisión)
        for col in df.columns:
            col_norm = str(col).strip().upper()
            if col_norm in [k.upper() for k in keywords_exact]:
                return col

        # Fase 2: Búsqueda Parcial (Flexibilidad de respaldo para errores humanos de tipeo)
        if keywords_partial:
            for col in df.columns:
                col_norm = str(col).strip().upper()
                for kw in keywords_partial:
                    if kw.upper() in col_norm:
                        return col
        return None

    @staticmethod
    def process_dates(df, sheet_name):
        """
        [MOTOR CLÍNICO DE FECHAS]
        Identifica la columna de fecha con extrema precisión, soluciona vacíos de Excel
        mediante Forward Fill (ffill) para celdas combinadas, y realiza la conversión absoluta a Date.
        Esto garantiza que TODAS las filas se enlacen al día seleccionado en el panel.

        Args:
            df: DataFrame con datos potencialmente crudos.
            sheet_name: Nombre de la hoja para fines de logging.

        Returns:
            DataFrame: DataFrame con columnas FECHA_DATETIME, FECHA_STD, AÑO, MES, SEMANA.
        """
        df = DataProcessor.clean_column_names(df)

        # Palabras clave exactas estrictas (Evitamos "DAY" para no cruzar con días de la semana de texto)
        k_exact = ['DATE', 'FECHA']
        k_partial = ['FECHA', 'DATE'] 

        col_fecha = DataProcessor.find_column_exact_or_partial(df, k_exact, k_partial)

        if col_fecha:
            LogManager.info(f"Columna de fecha anclada exitosamente como '{col_fecha}' en '{sheet_name}'.")

            # 1. Aplicación de Forward Fill para rellenar vacíos por celdas combinadas en Excel
            df[col_fecha] = df[col_fecha].ffill()

            # 2. Coerción robusta: valores inválidos se vuelven NaT (Not a Time)
            df['FECHA_DATETIME'] = pd.to_datetime(df[col_fecha], errors='coerce')

            # 3. Limpieza de memoria y eliminación de filas basura al final del excel
            df = df.dropna(subset=['FECHA_DATETIME']).copy()

            # 4. Estandarización Absoluta a Objeto DATE (Hora truncada) para cruce preciso con el Dashboard
            df['FECHA_STD'] = df['FECHA_DATETIME'].dt.date

            # 5. Dimensiones temporales analíticas auxiliares
            df['AÑO'] = df['FECHA_DATETIME'].dt.year
            df['MES'] = df['FECHA_DATETIME'].dt.month
            df['SEMANA'] = df['FECHA_DATETIME'].dt.isocalendar().week
        else:
            LogManager.error(f"Fallo crítico: No se detectó columna de fecha válida en '{sheet_name}'. Estructura de libro inválida.")

        return df

    @staticmethod
    def extract_time_block(df):
        """
        [MOTOR PREPARADOR DE LÍNEA DE VIDA]
        Extrae y construye la 'Hora de Inicio' y 'Hora Final' para el gráfico de Línea de Vida.
        Si la columna "Hora Final" no existe, proyecta matemáticamente el bloque sumando 
        los minutos de parada al inicio.

        Args:
            df: DataFrame de detalle de paradas.

        Returns:
            DataFrame: DataFrame con columnas TIMELINE_START y TIMELINE_END.
        """
        if df.empty: return df

        c_inicio = DataProcessor.find_column_exact_or_partial(df, ['HORA INICIO', 'HORA', 'START TIME', 'TIEMPO INICIO'])
        c_fin = DataProcessor.find_column_exact_or_partial(df, ['HORA FINAL', 'HORA FIN', 'END TIME', 'TIEMPO FIN'])
        c_min = DataProcessor.find_column_exact_or_partial(df, ['PARADAS (MINUTOS)', 'MINUTOS', 'DURACION', 'TIEMPO PERDIDO'])

        if 'FECHA_DATETIME' not in df.columns:
            return df

        if c_inicio:
            def safe_combine(date_val, time_val):
                """Combina de forma segura un objeto Date con una celda que contiene una Hora."""
                try:
                    if isinstance(time_val, datetime): return time_val
                    t_str = str(time_val).strip()
                    t_obj = pd.to_datetime(t_str, errors='coerce')
                    if pd.notnull(t_obj):
                        return pd.Timestamp.combine(date_val.date(), t_obj.time())
                    return date_val
                except:
                    return date_val

            # Anclar hora de inicio
            df['TIMELINE_START'] = df.apply(lambda row: safe_combine(row['FECHA_DATETIME'], row[c_inicio]), axis=1)

            # Anclar o calcular hora de fin
            if c_fin:
                df['TIMELINE_END'] = df.apply(lambda row: safe_combine(row['FECHA_DATETIME'], row[c_fin]), axis=1)
            elif c_min:
                df[c_min] = DataProcessor.safe_numeric_conversion(df[c_min])
                df['TIMELINE_END'] = df.apply(lambda row: row['TIMELINE_START'] + timedelta(minutes=row[c_min]) if pd.notnull(row['TIMELINE_START']) else row['FECHA_DATETIME'], axis=1)
            else:
                # Failsafe: Bloque visual mínimo si todo lo demás falla
                df['TIMELINE_END'] = df['TIMELINE_START'] + timedelta(minutes=15)

            # Corrector de cruces de medianoche o errores manuales de tipeo
            mask_invertida = df['TIMELINE_END'] < df['TIMELINE_START']
            df.loc[mask_invertida, 'TIMELINE_END'] = df.loc[mask_invertida, 'TIMELINE_START'] + timedelta(minutes=1)

        return df

    @staticmethod
    def safe_numeric_conversion(series, fill_value=0.0):
        """Conversor aritmético seguro que blinda el sistema de textos ingresados por error humano."""
        return pd.to_numeric(series, errors='coerce').fillna(fill_value)


@st.cache_data(show_spinner=False, ttl=3600)
def load_and_parse_excel(uploaded_file):
    """
    Motor Maestro de Lectura de Excel. 
    1. Desencripta el archivo y mapea las hojas.
    2. Ejecuta el escaneo de Offsets buscando las cabeceras.
    3. Extrae, purifica y formatea los DataFrames específicos.

    Args:
        uploaded_file: Archivo subido por el usuario (st.file_uploader).

    Returns:
        dict: Diccionario con DataFrames procesados para 'CAPS', 'Produccion' y 'Detalle parada'.
    """
    try:
        excel_data = pd.ExcelFile(uploaded_file)
        hojas_disponibles = excel_data.sheet_names
        data_dict = {}

        # Diccionario de inyección: Define qué hojas se necesitan y sus palabras clave
        targets = {
            'CAPS': ['DATE', 'FECHA', 'OEE', 'MACHINE'], 
            'Produccion': ['DATE', 'FECHA', 'OPERADOR', 'CONFORME'],
            'Detalle parada': ['DATE', 'FECHA', 'MINUTOS', 'MOTIVO', 'ESPECIFICA']
        }

        for target_key, keywords in targets.items():
            # Búsqueda tolerante a tildes (Producción vs Produccion) y variaciones menores
            hoja_match = [h for h in hojas_disponibles if target_key.upper() in h.upper() or target_key.replace("ó", "o").upper() in h.upper()]

            if hoja_match:
                sheet_name = hoja_match[0]
                # Ubicar cabecera
                header_idx = DataProcessor.find_true_header_index(excel_data, sheet_name, keywords)

                # Extracción tabular
                df_raw = excel_data.parse(sheet_name, header=header_idx)

                # Profiling clínico de fechas
                df_processed = DataProcessor.process_dates(df_raw, target_key)

                # Tratamiento espacial especial para la Línea de Vida
                if target_key == 'Detalle parada':
                    df_processed = DataProcessor.extract_time_block(df_processed)

                data_dict[target_key] = df_processed
                LogManager.info(f"Hoja '{target_key}' parseada íntegramente. Registros de alta fidelidad: {len(df_processed)}")
            else:
                LogManager.error(f"Ausencia de matriz crítica: No se localizó '{target_key}'.")
                st.error(f"❌ Error Estructural: El libro no contiene la hoja esperada: '{target_key}'.")
                data_dict[target_key] = pd.DataFrame()

        return data_dict

    except Exception as e:
        LogManager.error(f"Colapso masivo en lectura binaria Excel: {e}")
        st.error(f"❌ Fallo crítico en el motor de lectura: {str(e)}")
        return None


# ==================================================================================================
# 6. MOTOR DE FILTRADO MULTI-DIMENSIONAL (MASTER FILTERS)
# ==================================================================================================
class FilterEngine:
    """Clase estática matemática que aplica máscaras condicionales temporales y lógicas."""

    @staticmethod
    def apply_master_filters(df, p_inicio, p_fin, p_tipo, p_ano, p_mes, p_sem, turnos_sel):
        """
        Aplica filtros maestros de tiempo y turno sobre un DataFrame.

        Args:
            df: DataFrame a filtrar.
            p_inicio: Fecha de inicio del rango.
            p_fin: Fecha de fin del rango.
            p_tipo: Tipo de filtro temporal seleccionado.
            p_ano: Año seleccionado (para filtros anuales/mensuales).
            p_mes: Mes seleccionado.
            p_sem: Semana ISO seleccionada.
            turnos_sel: Lista de turnos seleccionados.

        Returns:
            DataFrame: DataFrame filtrado según los criterios especificados.
        """
        if df is None or df.empty or 'FECHA_STD' not in df.columns:
            return df

        # Máscara Cronológica Base
        mask_time = (df['FECHA_STD'] >= p_inicio) & (df['FECHA_STD'] <= p_fin)

        # Sobreescritura jerárquica para selectores anuales o mensuales
        if p_tipo == "Año Anualizado" and 'AÑO' in df.columns:
            mask_time = df['AÑO'] == p_ano
        elif p_tipo == "Mes Fiscal" and 'MES' in df.columns and 'AÑO' in df.columns:
            mask_time = (df['AÑO'] == p_ano) & (df['MES'] == p_mes)
        elif p_tipo == "Semana ISO" and 'SEMANA' in df.columns and 'AÑO' in df.columns:
            mask_time = (df['AÑO'] == p_ano) & (df['SEMANA'] == p_sem)

        # Aplicación y clonado en memoria profunda
        df_filt = df.loc[mask_time].copy()

        # Máscara Operacional (Turnos)
        col_turno = DataProcessor.find_column_exact_or_partial(df_filt, ['TURNO', 'SHIFT'])
        if col_turno and turnos_sel:
            # Normalización robusta (" Día " -> "DÍA")
            df_filt[col_turno] = df_filt[col_turno].astype(str).str.strip().str.upper()
            turnos_norm = [str(t).strip().upper() for t in turnos_sel]
            df_filt = df_filt[df_filt[col_turno].isin(turnos_norm)]

        return df_filt


# ==================================================================================================
# 7. MOTOR ANALÍTICO Y DE REGLAS DE NEGOCIO (CÁLCULO EXACTO DE KPIs)
# ==================================================================================================
class BusinessLogic:
    """Ejecuta la consolidación directa desde la matriz Excel respetando sus cálculos de origen."""

    @staticmethod
    def calcular_metricas(df_caps, df_prod, df_paradas):
        """
        Garantiza que los datos se extraigan directamente de la hoja CAPS (Disponibilidad, Rendimiento, Calidad, OEE)
        y de la hoja Produccion para los volúmenes, logrando exactitud clínica del 100%.

        Args:
            df_caps: DataFrame de la hoja CAPS.
            df_prod: DataFrame de la hoja Produccion.
            df_paradas: DataFrame de la hoja Detalle parada.

        Returns:
            dict: Diccionario con métricas calculadas, DataFrames de soporte y datos de timeline.
        """
        resultados = {
            # Núcleo OEE Exacto (CAPS)
            "OEE": 0.0, "Disponibilidad": 0.0, "Rendimiento": 0.0, "Calidad": 0.0,
            # Núcleo Volumetría y RH
            "Prod_Conforme": 0, "Prod_No_Conforme": 0, "Muestras_Calidad": 0, "Operadores": [],
            "Data_Operadores": pd.DataFrame(),
            # Núcleo Eventos y Fallas
            "Top_Paradas": pd.DataFrame(),
            "Data_Pareto_Total": pd.DataFrame(),
            "Data_Timeline": df_paradas.copy()
        }

        # ---------------------------------------------------------
        # A. CÁLCULO DE EFICIENCIA EXACTA: HOJA 'CAPS'
        # ---------------------------------------------------------
        if not df_caps.empty:
            # Filtrado por equipo específico en matrices multi-máquina
            col_maq = DataProcessor.find_column_exact_or_partial(df_caps, ['MACHINE', 'MAQUINA', 'LÍNEA', 'LINEA', 'EQUIPO'])
            df_caps_219 = df_caps.copy()
            if col_maq:
                mask_219 = df_caps_219[col_maq].astype(str).str.contains('219|Carga de Detonadores', case=False, na=False)
                if mask_219.any():
                    df_caps_219 = df_caps_219[mask_219]

            # Mapeo exacto de indicadores pre-calculados por el área
            c_oee  = DataProcessor.find_column_exact_or_partial(df_caps_219, ['OEE'])
            c_disp = DataProcessor.find_column_exact_or_partial(df_caps_219, ['EQUIPMENT AVAILIBILITY', 'AVAILABILITY', 'DISPONIBILIDAD'])
            c_perf = DataProcessor.find_column_exact_or_partial(df_caps_219, ['PERFORMANCE', 'RENDIMIENTO'])
            c_qual = DataProcessor.find_column_exact_or_partial(df_caps_219, ['QUALITY', 'CALIDAD'])

            def extraer_promedio_clinico(df, col):
                """Obtiene el promedio válido de la columna elegida. Maneja auto-escalado si el Excel usa decimales."""
                if col and col in df.columns:
                    s = pd.to_numeric(df[col], errors='coerce').dropna()
                    if not s.empty:
                        val = s.mean()
                        # Auto-escala heurística a porcentaje: 0.85 -> 85.0%
                        return (val * 100) if val <= 1.5 else val
                return 0.0

            resultados['OEE'] = extraer_promedio_clinico(df_caps_219, c_oee)
            resultados['Disponibilidad'] = extraer_promedio_clinico(df_caps_219, c_disp)
            resultados['Rendimiento'] = extraer_promedio_clinico(df_caps_219, c_perf)
            resultados['Calidad'] = extraer_promedio_clinico(df_caps_219, c_qual)

        # ---------------------------------------------------------
        # B. VOLUMETRÍA Y TRAZABILIDAD: HOJA 'PRODUCCION'
        # ---------------------------------------------------------
        if not df_prod.empty:
            c_conf   = DataProcessor.find_column_exact_or_partial(df_prod, ['PRODUCCION CONFORME', 'PRODUCCIÓN CONFORME', 'CONFORME'])
            c_noconf = DataProcessor.find_column_exact_or_partial(df_prod, ['PRODUCCION NO CONFORME', 'PRODUCCIÓN NO CONFORME', 'RECHAZOS', 'NO CONFORME'])
            c_muest  = DataProcessor.find_column_exact_or_partial(df_prod, ['MUESTRAS DE CALIDAD', 'MUESTRA'])
            c_ope    = DataProcessor.find_column_exact_or_partial(df_prod, ['OPERADOR', 'OPERARIO', 'COLABORADOR', 'RESPONSABLE'])

            # Sumatoria neta de unidades
            if c_conf: resultados['Prod_Conforme'] = DataProcessor.safe_numeric_conversion(df_prod[c_conf]).sum()
            if c_noconf: resultados['Prod_No_Conforme'] = DataProcessor.safe_numeric_conversion(df_prod[c_noconf]).sum()
            if c_muest: resultados['Muestras_Calidad'] = DataProcessor.safe_numeric_conversion(df_prod[c_muest]).sum()

            # Matriz de Responsabilidad por Operador (Intacta)
            if c_ope: 
                resultados['Operadores'] = df_prod[c_ope].dropna().unique().tolist()
                if c_conf:
                    df_prod[c_conf] = DataProcessor.safe_numeric_conversion(df_prod[c_conf])
                    df_grouped_op = df_prod.groupby(c_ope)[c_conf].sum().reset_index()
                    resultados['Data_Operadores'] = df_grouped_op.sort_values(by=c_conf, ascending=False)

        # ---------------------------------------------------------
        # C. AUDITORÍA DE FALLAS: HOJA 'DETALLE PARADA'
        # ---------------------------------------------------------
        if not df_paradas.empty:
            c_min  = DataProcessor.find_column_exact_or_partial(df_paradas, ['PARADAS (MINUTOS)', 'MINUTOS'])
            c_desc = DataProcessor.find_column_exact_or_partial(df_paradas, ['DESCRIPCIÓN ESPECIFICA', 'DESCRIPCION ESPECIFICA', 'MOTIVO DE PARADA', 'FALLA'])
            c_cat = DataProcessor.find_column_exact_or_partial(df_paradas, ['CATEGORY', 'CATEGORIA', 'TIPO'])

            if c_min and c_desc:
                df_paradas[c_min] = DataProcessor.safe_numeric_conversion(df_paradas[c_min])

                # Consolidado Total para la Pestaña de Análisis Profundo
                df_all_par = df_paradas.groupby(c_desc)[c_min].sum().reset_index()
                df_all_par = df_all_par.sort_values(by=c_min, ascending=False)
                df_all_par.rename(columns={c_desc: 'Descripcion', c_min: 'Minutos'}, inplace=True)

                resultados['Data_Pareto_Total'] = df_all_par

                # Top 10 Detractores para el Dashboard Principal (Intacto)
                resultados['Top_Paradas'] = df_all_par.head(10)

                # Clasificación estándar para la línea de vida
                if c_cat:
                    df_paradas['CATEGORIA_STD'] = df_paradas[c_cat].fillna("Sin Categorizar")
                else:
                    df_paradas['CATEGORIA_STD'] = "Evento Registrado"

                resultados['Data_Timeline'] = df_paradas

        return resultados


# ==================================================================================================
# 8. MÓDULO EXPERTO DE DIAGNÓSTICO EJECUTIVO (QUALITY CONTROL)
# ==================================================================================================
class QualityControl:
    """Genera reportes de hallazgos (insights) basados en el performance extraído frente a las metas."""

    @staticmethod
    def generate_insights(metrics, target_oee):
        """
        Genera una lista de insights ejecutivos basados en las métricas calculadas.

        Args:
            metrics: Diccionario de métricas calculadas por BusinessLogic.
            target_oee: Meta de OEE configurada por el usuario.

        Returns:
            list: Lista de strings con insights formateados en Markdown.
        """
        insights = []

        # Auditoría de Meta Global
        if metrics['OEE'] >= target_oee:
            insights.append(f"🟢 **Comportamiento Óptimo:** El Índice OEE ({metrics['OEE']:.1f}%) superó la meta gerencial ({target_oee}%).")
        else:
            brecha = target_oee - metrics['OEE']
            insights.append(f"🔴 **Desviación de Meta:** Existe una oportunidad de recuperación del {brecha:.1f}% en el OEE Global respecto a la cuota.")

        # Auditoría de Variables TPM (Cuellos de botella)
        lowest_factor = min(metrics['Disponibilidad'], metrics['Rendimiento'], metrics['Calidad'])
        if lowest_factor == metrics['Disponibilidad'] and metrics['Disponibilidad'] > 0:
            insights.append("⚠️ **Diagnóstico de Limite:** La 'Disponibilidad' es el cuello de botella actual (Alto impacto por averías/setup prolongado).")
        elif lowest_factor == metrics['Rendimiento'] and metrics['Rendimiento'] > 0:
            insights.append("⚠️ **Diagnóstico de Limite:** El 'Rendimiento' está degradado (Presencia de microparadas o velocidad reducida en la línea).")

        # Auditoría Volumétrica Físico-Química
        tasa_rechazo = 0
        total_prod = metrics['Prod_Conforme'] + metrics['Prod_No_Conforme']
        if total_prod > 0:
            tasa_rechazo = (metrics['Prod_No_Conforme'] / total_prod) * 100
            if tasa_rechazo > 1.0:
                insights.append(f"🔴 **Alerta de Merma Física:** Tasa de chatarra/rechazo elevada ({tasa_rechazo:.2f}%). Requierese inspección en tolerancias.")

        return insights


# ==================================================================================================
# 9. MOTORES DE VISUALIZACIÓN VECTORIAL AVANZADA (PLOTLY DASHBOARDS & GANTT)
# ==================================================================================================
class PlotlyEngine:
    """Núcleo responsable de la creación de arte visual, gráficos de alta resolución y mapas térmicos."""

    @staticmethod
    def create_gauge(value, title, target, color_theme):
        """Gráfico Tipo Manómetro de Precisión Institucional para las métricas base del OEE."""
        val = max(0, min(value, 100))
        fig = go.Figure(go.Indicator(
            mode = "gauge+number+delta",
            value = val,
            title = {'text': title, 'font': {'size': 18, 'color': '#0A2540', 'family': 'Segoe UI'}},
            number = {'suffix': "%", 'font': {'size': 36, 'color': '#0A2540', 'weight': 'bold'}},
            delta = {'reference': target, 'increasing': {'color': "#2E8B57"}, 'decreasing': {'color': "#C0392B"}},
            gauge = {
                'axis': {'range': [None, 100], 'tickwidth': 1, 'tickcolor': "#0A2540", 'tickfont': {'size': 14}},
                'bar': {'color': color_theme, 'thickness': 0.8},
                'bgcolor': "white",
                'borderwidth': 2,
                'bordercolor': "#E2E8F0",
                'steps': [
                    {'range': [0, 60], 'color': '#FFEBEE'},            
                    {'range': [60, target], 'color': '#FFF8E1'},       
                    {'range': [target, 100], 'color': '#E8F5E9'}       
                ],
                'threshold': {'line': {'color': "#C0392B", 'width': 3}, 'thickness': 0.8, 'value': target}
            }
        ))
        fig.update_layout(height=280, margin=dict(l=20, r=20, t=40, b=20), paper_bgcolor='rgba(0,0,0,0)')
        return fig

    @staticmethod
    def create_pareto_bar(df_top, title="Análisis Crítico: Top Fallas (Minutos)"):
        """Gráfico de barras horizontales optimizado para alojar la descripción del error técnico."""
        if df_top.empty:
            return go.Figure().update_layout(title="Sin incidentes físicos en el periodo. Operación Impecable.", template="simple_white")

        # Ordenamiento ascendente (Plotly apila de abajo hacia arriba en modo 'h')
        df_sorted = df_top.sort_values(by='Minutos', ascending=True)

        fig = px.bar(
            df_sorted, 
            x='Minutos', y='Descripcion', 
            orientation='h',
            text='Minutos', color='Minutos',
            color_continuous_scale=['#FADBD8', '#C0392B'] 
        )

        fig.update_traces(
            texttemplate='%{text:.1f} min', textposition='outside', 
            marker_line_color='#0A2540', marker_line_width=1, textfont_size=12
        )

        fig.update_layout(
            title={'text': title, 'font': {'size': 18, 'color': '#0A2540'}},
            xaxis_title="Minutos de Impacto", yaxis_title="",
            template="simple_white", height=450,
            margin=dict(l=10, r=40, t=60, b=10),
            coloraxis_showscale=False, yaxis=dict(tickfont=dict(size=11))
        )
        return fig

    @staticmethod
    def create_timeline_gantt(df_timeline):
        """
        [LÍNEA DE VIDA CRONOLÓGICA] 
        Genera un diagrama de Gantt que evidencia visualmente las caídas del equipo durante las 24/12H.
        Permite observar el comportamiento de la máquina a nivel microscópico.
        """
        if df_timeline.empty or 'TIMELINE_START' not in df_timeline.columns or 'TIMELINE_END' not in df_timeline.columns:
            return go.Figure().update_layout(title="Data cronológica insuficiente para trazar la Línea de Vida Operacional", template="simple_white")

        df_g = df_timeline.dropna(subset=['TIMELINE_START', 'TIMELINE_END']).copy()
        if df_g.empty:
            return go.Figure().update_layout(title="Formatos de hora (Inicio/Fin) no compatibles para la proyección gráfica", template="simple_white")

        # Asegurar columna descriptiva para el hover del mouse
        c_desc = DataProcessor.find_column_exact_or_partial(df_g, ['DESCRIPCIÓN ESPECIFICA', 'DESCRIPCION ESPECIFICA', 'MOTIVO'])
        desc_col = c_desc if c_desc else 'Falla Registrada'

        # Categoría para agrupar y colorear (Eje Y)
        cat_col = 'CATEGORIA_STD'

        # Instancia de Timeline Avanzado
        fig = px.timeline(
            df_g, 
            x_start="TIMELINE_START", 
            x_end="TIMELINE_END", 
            y=cat_col, 
            color=cat_col,
            hover_name=desc_col,
            title="Línea de Vida Operacional: Trazo Cronológico de Fallas del Equipo",
            color_discrete_sequence=px.colors.qualitative.Dark24
        )

        # Invertir eje Y para estética
        fig.update_yaxes(autorange="reversed")

        # Configurar Eje X para mostrar horas detalladas del turno
        fig.update_layout(
            xaxis=dict(
                title="Horario del Turno Analizado",
                tickformat="%H:%M", # Formato de hora estándar
                showgrid=True,
                gridcolor='#E2E8F0',
                tickangle=-45
            ),
            yaxis_title="Agrupación de Eventos",
            template="simple_white",
            height=400,
            showlegend=False, 
            margin=dict(t=60, b=60, l=20, r=20)
        )
        return fig

    @staticmethod
    def create_operator_pie(df_op):
        """Gráfico tipo Dona que ilustra el reparto porcentual exacto de responsabilidad productiva."""
        if df_op is None or df_op.empty: return go.Figure()

        col_ope = df_op.columns[0]
        col_val = df_op.columns[1]

        fig = px.pie(
            df_op, 
            values=col_val, names=col_ope, 
            hole=0.45, 
            title="Distribución Neta por Colaborador",
            color_discrete_sequence=['#0A2540', '#C07F00', '#8B4513', '#2E8B57', '#7F8C8D']
        )
        fig.update_traces(
            textposition='inside', textinfo='percent+label',
            marker=dict(line=dict(color='#FFFFFF', width=2))
        )
        fig.update_layout(height=450, showlegend=False, margin=dict(t=50, b=20, l=10, r=10))
        return fig

    @staticmethod
    def create_pareto_advanced(df_full):
        """
        [PANTALLA DE ANÁLISIS PROFUNDO]
        Gráfica Pareto Corporativa combinada (Barras + Línea Acumulada 80/20) para el nuevo Tab analítico.
        """
        if df_full.empty: return go.Figure()

        df = df_full.copy()
        df['Acumulado %'] = (df['Minutos'].cumsum() / df['Minutos'].sum()) * 100

        fig = go.Figure()
        # Capa 1: Barras Físicas
        fig.add_trace(go.Bar(
            x=df['Descripcion'], y=df['Minutos'], 
            name='Impacto (Minutos)', marker_color='#C07F00',
            text=df['Minutos'].round(1), textposition='outside'
        ))
        # Capa 2: Línea Acumulada Porcentual (Principio de Pareto)
        fig.add_trace(go.Scatter(
            x=df['Descripcion'], y=df['Acumulado %'], 
            name='Acumulado %', yaxis='y2', 
            mode='lines+markers', marker=dict(color='#0A2540', size=8), line=dict(width=3)
        ))

        fig.update_layout(
            title="Análisis Espectral de Fallas (Curva de Pareto 80/20 Acumulada)",
            template="simple_white", height=550,
            yaxis=dict(title='Minutos Netos de Impacto', showgrid=True, gridcolor='#F0F0F0'),
            yaxis2=dict(title='Acumulado Frecuencial (%)', overlaying='y', side='right', range=[0, 105], showgrid=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1),
            xaxis_tickangle=-45, margin=dict(b=140) # Espacio generoso inferior para leer las fallas
        )

        # Marcador de alerta roja al 80%
        fig.add_hline(y=80, yref='y2', line_dash="dash", line_color="#C0392B", annotation_text="Frontera de Foco Crítico (80%)", annotation_position="bottom right")
        return fig


# ==================================================================================================
# 10. MOTOR FPDF AVANZADO PARA REPORTES EJECUTIVOS AUTOMATIZADOS (FORMATO A4 SEGURO)
# ==================================================================================================
class ReportGenerator(FPDF):
    """
    Motor vectorial avanzado para emitir reportes A4 a la gerencia.
    Arquitectura extendida para asegurar márgenes, evitar traslapes de gráficos y aplicar firmas.
    """
    def __init__(self, ctx_date, turno_str):
        super().__init__(orientation='P', unit='mm', format='A4')
        self.ctx_date = ctx_date
        self.turno_str = turno_str
        self.set_auto_page_break(auto=True, margin=20)
        self.set_margins(15, 15, 15)

    def header(self):
        """Bloque Header corporativo estricto y persistente."""
        if self.page_no() > 1: 
            self.set_fill_color(10, 37, 64) 
            self.rect(0, 0, 210, 26, 'F')

            self.set_y(8)
            self.set_font('Arial', 'B', 15)
            self.set_text_color(255, 255, 255)
            self.cell(0, 6, 'INFORME EJECUTIVO DE DESEMPENO (OEE) Y EVENTOS', 0, 1, 'C')

            self.set_font('Arial', '', 10)
            self.set_text_color(220, 220, 220)
            self.cell(0, 6, f'Unidad: Planta Lurin | Maquina {AppConfig.MAQUINA_ID} ({AppConfig.MAQUINA_NOMBRE})', 0, 1, 'C')
            self.ln(12)

    def footer(self):
        """Bloque Footer con control de páginas y sellado de tiempo."""
        if self.page_no() > 1:
            self.set_y(-18)
            self.set_font('Arial', 'I', 8)
            self.set_text_color(150, 150, 150)
            self.line(15, 278, 195, 278)
            self.cell(90, 10, f'Framework Analitico CAVA | Filtro de Emision: {self.ctx_date}', 0, 0, 'L')
            self.cell(90, 10, f'Pagina {self.page_no()} | Timestamp: {datetime.now().strftime("%Y-%m-%d %H:%M")}', 0, 0, 'R')

    def add_cover_page(self):
        """Renderizado de portada de alto impacto visual."""
        self.add_page()
        self.set_fill_color(248, 249, 250)
        self.rect(0, 0, 210, 297, 'F') 

        # Banda lateral identificativa dorada
        self.set_fill_color(192, 127, 0) 
        self.rect(0, 0, 8, 297, 'F')

        self.ln(50)
        self.set_font('Arial', 'B', 26)
        self.set_text_color(10, 37, 64)
        self.cell(10)
        self.cell(0, 15, 'REPORTE GERENCIAL CONSOLIDADO', 0, 1, 'L')
        self.cell(10)
        self.cell(0, 15, 'DE OEE Y VOLUMETRIA', 0, 1, 'L')

        self.ln(10)
        self.set_font('Arial', '', 16)
        self.set_text_color(120, 120, 120)
        self.cell(10); self.cell(0, 10, f'Activo Estrategico: Carga de Detonadores', 0, 1, 'L')
        self.cell(10); self.cell(0, 10, f'Identificador Numerico: Maquina {AppConfig.MAQUINA_ID}', 0, 1, 'L')

        self.ln(35)

        # Inyección de Metadata
        meta_info = [
            ('Ventana de Analisis:', self.ctx_date), 
            ('Turnos Integrados:', self.turno_str), 
            ('Capa de Extraccion:', 'CAVA Robotics Core (Automated System)')
        ]

        for title, val in meta_info:
            self.set_font('Arial', 'B', 12)
            self.set_text_color(10, 37, 64)
            self.cell(10)
            self.cell(45, 8, title, 0, 0)
            self.set_font('Arial', '', 12)
            self.cell(0, 8, val, 0, 1)

    def draw_section_header(self, title):
        """Pintado de divisores estilizados de sección lógicas en A4."""
        self.ln(5)
        self.set_font('Arial', 'B', 12)
        self.set_text_color(255, 255, 255)
        self.set_fill_color(192, 127, 0) 
        self.cell(0, 9, f"   {title.upper()}", 0, 1, 'L', fill=True)
        self.ln(4)

    def build_executive_body(self, met, imgs_paths, insights):
        """Maquetado del contenido real. Cuidado extremo en los márgenes de las gráficas."""
        self.add_page()

        # -------------------------------------------------------------
        # SECCIÓN 1: INSIGHTS E INTELIGENCIA
        # -------------------------------------------------------------
        self.draw_section_header('1. Diagnostico Ejecutivo y Hallazgos Relevantes')
        self.set_font('Arial', '', 11)
        self.set_text_color(0, 0, 0)
        for insight in insights:
            # Purificación del texto Markdown para que FPDF no explote
            clean_text = insight.replace('**', '').encode('latin-1', 'ignore').decode('latin-1')
            self.multi_cell(0, 6, f"  - {clean_text}")
        self.ln(3)

        # -------------------------------------------------------------
        # SECCIÓN 2: OEE MATRIZ
        # -------------------------------------------------------------
        self.draw_section_header('2. Analisis Factorial de Eficiencia de Maquina (OEE)')

        self.set_font('Arial', 'B', 10)
        self.set_fill_color(10, 37, 64)
        self.set_text_color(255, 255, 255)
        for col in ['OEE Global', 'Factor Disponibilidad', 'Factor Rendimiento', 'Factor Calidad']: 
            self.cell(45, 8, col, 1, 0, 'C', fill=True)
        self.ln()

        self.set_font('Arial', 'B', 12)
        self.set_text_color(10, 37, 64)
        self.set_fill_color(245, 245, 245)
        for val in [met['OEE'], met['Disponibilidad'], met['Rendimiento'], met['Calidad']]: 
            self.cell(45, 10, f"{val:.1f}%", 1, 0, 'C', fill=True)
        self.ln(12)

        # -------------------------------------------------------------
        # SECCIÓN 3: VOLUMETRÍA NET
        # -------------------------------------------------------------
        self.draw_section_header('3. Trazabilidad Volumetrica de Produccion')

        self.set_font('Arial', '', 11); self.set_text_color(0, 0, 0)
        self.cell(100, 9, "Volumen Neto de Produccion Conforme (Liberado):", border='B')
        self.set_font('Arial', 'B', 11)
        self.cell(80, 9, f"{met['Prod_Conforme']:,.0f} unidades", border='B', ln=1, align='R')

        self.set_font('Arial', '', 11); self.set_text_color(0, 0, 0)
        self.cell(100, 9, "Volumen Neto de Produccion No Conforme (Rechazo):", border='B')
        self.set_font('Arial', 'B', 11); self.set_text_color(192, 57, 43)
        self.cell(80, 9, f"{met['Prod_No_Conforme']:,.0f} unidades", border='B', ln=1, align='R')
        self.set_text_color(0, 0, 0)

        self.set_font('Arial', '', 11)
        self.cell(100, 9, "Muestreos Extraidos para QA/Laboratorio:", border='B')
        self.set_font('Arial', 'B', 11)
        self.cell(80, 9, f"{met['Muestras_Calidad']:,.0f} unidades", border='B', ln=1, align='R')

        # -------------------------------------------------------------
        # INCORPORACIÓN DE GRÁFICOS (Página actual y siguientes)
        # -------------------------------------------------------------
        if 'gauges' in imgs_paths:
            self.ln(8)
            self.image(imgs_paths['gauges'], x=15, w=180) # Dimensionado perfecto al centro A4

        # NUEVA PÁGINA OBLIGATORIA PARA LAS MACRO-GRÁFICAS
        self.add_page()

        self.draw_section_header('4. Linea de Vida Cronologica (Comportamiento Operativo)')
        if 'timeline' in imgs_paths:
            self.image(imgs_paths['timeline'], x=15, w=180)
            self.ln(85) # Resguardo de margen vital posterior a la foto

        self.draw_section_header('5. Auditoria Cientifica de Fallas Criticas (Analisis de Pareto)')
        if 'pareto_adv' in imgs_paths:
            # Si el usuario exporta desde la pestaña de Análisis Profundo
            self.image(imgs_paths['pareto_adv'], x=15, w=180)
            self.ln(100)
        elif 'bar_paradas' in imgs_paths:
            # Fallback a gráfica de barras simple
            self.image(imgs_paths['bar_paradas'], x=15, w=180)
            self.ln(90)

        # -------------------------------------------------------------
        # BLOQUE DE VALIDACIÓN Y FIRMAS AUTÓGRAFAS
        # -------------------------------------------------------------
        # Si no hay espacio, añadir nueva página para las firmas
        if self.get_y() > 240:
            self.add_page()
            self.ln(40)

        self.ln(25)
        self.set_font('Arial', '', 10)
        # Dibujado de líneas de firma equilibradas
        self.line(30, self.get_y(), 85, self.get_y())
        self.line(125, self.get_y(), 180, self.get_y())

        self.ln(2)
        self.cell(95, 5, 'Vobo. Superintendencia Mantenimiento', 0, 0, 'C')
        self.cell(90, 5, 'Vobo. Jefatura/Gerencia de Planta', 0, 1, 'C')


# ==================================================================================================
# 11. SISTEMA DE TRANSMISIÓN DE DATOS (TELEGRAM API GATEWAY)
# ==================================================================================================
class TelegramGateway:
    """Encapsula los métodos HTTP para el túnel de encriptación y envío de PDF al corporativo."""

    @staticmethod
    def dispatch_report(pdf_path, metrics, ctx_date):
        """
        Envía el reporte PDF generado a través de la API de Telegram.

        Args:
            pdf_path: Ruta física del archivo PDF a enviar.
            metrics: Diccionario de métricas para incluir en el mensaje.
            ctx_date: Fecha del contexto analizado.

        Returns:
            bool: True si el envío fue exitoso, False en caso contrario.
        """
        url = f"https://api.telegram.org/bot{AppConfig.TELEGRAM_TOKEN}/sendDocument"

        top_falla = metrics['Top_Paradas'].iloc[0]['Descripcion'] if not metrics['Top_Paradas'].empty else 'Operatividad Impecable'
        top_min = metrics['Top_Paradas'].iloc[0]['Minutos'] if not metrics['Top_Paradas'].empty else 0.0

        # Mensaje estético y gerencial para WhatsApp/Telegram
        msg_caption = (
            f"📊 *Reporte Gerencial Consolidado - Maq. {AppConfig.MAQUINA_ID}*\n"
            f"📅 *Ventana Analizada:* {ctx_date}\n"
            f"⚙️ *Tasa OEE Cierre:* {metrics['OEE']:.1f}%\n"
            f"📦 *Volumen QA Pass:* {metrics['Prod_Conforme']:,.0f} unds\n"
            f"⚠️ *Alerta Falla Mayor:* {top_falla} ({top_min:.1f} min)\n\n"
            f"_Operación despachada automáticamente desde CAVA Analytics_"
        )

        try:
            with open(pdf_path, 'rb') as file_binary:
                payload_files = {'document': file_binary}
                payload_data = {'chat_id': AppConfig.TELEGRAM_CHAT_ID, 'caption': msg_caption, 'parse_mode': 'Markdown'}

                LogManager.info("Ejecutando Handshake de Red con API de Telegram...")
                response = requests.post(url, files=payload_files, data=payload_data, timeout=20)

            if response.status_code == 200:
                LogManager.info("Carga de Blob y transmisión de Reporte completada al 100%.")
                return True
            else:
                LogManager.error(f"Fallo HTTP POST Telegram. Status Code: {response.status_code} - Log: {response.text}")
                return False
        except requests.exceptions.RequestException as e:
            LogManager.error(f"Interrupción de TimeOut o DNS en TelegramGateway: {e}")
            return False


# ==================================================================================================
# 12. ORQUESTADOR PRINCIPAL UI: TABLERO INTERACTIVO EN STREAMLIT
# ==================================================================================================
class DashboardUI:
    """Clase maestra que gobierna el ciclo de vida de la aplicación, interacción y rendering HTML."""

    def __init__(self):
        self.data_dict = None
        self.metricas = None
        self.ctx_str = ""
        self.str_turnos = ""
        # =============================================================================
        # NUEVOS ATRIBUTOS: Soportes para filtros granulares de Categoría y Causa (Módulo 2)
        # =============================================================================
        self.df_paradas_master = pd.DataFrame()
        self.filtro_categorias = []
        self.filtro_causas = []
        # =============================================================================
        # ATRIBUTOS ADICIONALES: Filtros COD y Sistemas para Módulo 2
        # =============================================================================
        self.filtro_cod = []
        self.filtro_sistemas = []

    def render_cava_logo_native(self):
        """
        Renderiza el logotipo corporativo de CAVA en formato CSS. 
        Evita el uso de URLs de imágenes externas rotas y garantiza carga instantánea.
        """
        st.sidebar.markdown("""
        <div class="cava-logo-container">
            <h2 class="cava-logo-title">CAVA</h2>
            <p class="cava-logo-subtitle">ROBOTICS & AUTOMATION</p>
        </div>
        """, unsafe_allow_html=True)

    def calculate_smart_default_dates(self, df_caps):
        """
        [MEJORA CODIFICADA: Carga Inteligente al Día de Hoy]
        Detecta el día exacto en que se encuentra el servidor y carga los datos 
        del último turno operativo automáticamente.

        Args:
            df_caps: DataFrame de CAPS para inferir el rango de fechas disponible.

        Returns:
            tuple: (fecha_mínima, fecha_máxima, fecha_objetivo_inteligente)
        """
        today_date = datetime.now().date()
        min_date, max_date = today_date, today_date

        if not df_caps.empty and 'FECHA_STD' in df_caps.columns:
            min_date = df_caps['FECHA_STD'].min()
            max_date = df_caps['FECHA_STD'].max()

        # Logica Smart: Si hoy no hay producción, vete al último día donde sí hubo (max_date)
        smart_target_date = today_date if min_date <= today_date <= max_date else max_date
        return min_date, max_date, smart_target_date

    def render_sidebar_ingestion(self):
        """Módulo físico/cloud de entrada de archivos Excel."""
        self.render_cava_logo_native()
        st.sidebar.markdown("## 📥 1. Ingesta de Datos Raw (Brutos)")

        data_source = st.sidebar.radio("Metodología de Ingesta:", ["Carga Directa Matriz (.xlsx)", "Integración Cloud SharePoint"])

        if data_source == "Carga Directa Matriz (.xlsx)":
            uploaded_file = st.sidebar.file_uploader("Arrastre Matriz OEE de Planta:", type=["xlsx", "xlsm", "xls"])
            if uploaded_file:
                with st.spinner("Decodificando, aplicando Offsets de Red y Limpieza Clínica de Fechas..."):
                    self.data_dict = load_and_parse_excel(uploaded_file)
        else:
            with st.sidebar.form("sp_auth_form"):
                st.info("Autenticación Corporativa Microsoft 365")
                sp_url = st.text_input("URL Site Root (Site Collection)")
                sp_user = st.text_input("Credencial Administrativa")
                sp_pass = st.text_input("Clave de Bóveda", type="password")
                sp_path = st.text_input("Directorio Relativo Fichero")
                if st.form_submit_button("Sincronizar Azure/Sharepoint"):
                    if SHAREPOINT_AVAILABLE:
                        st.info("Modo de desarrollo local. Requiere bypass de Proxy Corporativo.")
                    else:
                        st.error("Dependencias físicas de SharePoint no habilitadas en el Kernel.")

    def render_sidebar_filters(self):
        """Controlador de flujo de Fechas, Turnos, Targets y Filtros de Paradas."""
        if not self.data_dict: return None

        df_caps = self.data_dict.get('CAPS', pd.DataFrame())
        df_prod = self.data_dict.get('Produccion', pd.DataFrame())
        df_par = self.data_dict.get('Detalle parada', pd.DataFrame())

        st.sidebar.markdown("---")
        st.sidebar.markdown("## 📅 2. Matriz Cronológica Exacta")

        # Inferencia Inteligente
        min_date, max_date, smart_target = self.calculate_smart_default_dates(df_caps)

        # Segmentación
        filtro_tipo = st.sidebar.selectbox("Lente Temporal:", ["Turno de Hoy (Smart)", "Día Exacto", "Semana ISO", "Mes Fiscal", "Año Anualizado", "Rango de Vectores"])

        f_inicio, f_fin = min_date, max_date
        p_ano, p_mes, p_sem = None, None, None

        if filtro_tipo == "Turno de Hoy (Smart)":
            # Forzamos el backend a que fije la fecha en el target inteligente
            f_inicio, f_fin = smart_target, smart_target
            st.sidebar.success(f"📌 Auto-enrutado al último turno: {smart_target}")

        elif filtro_tipo == "Día Exacto":
            sel_date = st.sidebar.date_input("Día de Inspección:", value=smart_target, min_value=min_date, max_value=max_date)
            f_inicio, f_fin = sel_date, sel_date

        elif filtro_tipo == "Rango de Vectores":
            rango = st.sidebar.date_input("Espacio Continuo de Tiempo:", [min_date, max_date], min_value=min_date, max_value=max_date)
            if len(rango) == 2: f_inicio, f_fin = rango[0], rango[1]

        elif filtro_tipo == "Año Anualizado" and 'AÑO' in df_caps.columns:
            p_ano = st.sidebar.selectbox("Seleccionar Año Fiscal", sorted(df_caps['AÑO'].dropna().unique(), reverse=True))

        elif filtro_tipo == "Mes Fiscal" and 'AÑO' in df_caps.columns and 'MES' in df_caps.columns:
            p_ano = st.sidebar.selectbox("Año Base", sorted(df_caps['AÑO'].dropna().unique(), reverse=True))
            p_mes = st.sidebar.selectbox("Mes Correlativo", sorted(df_caps[df_caps['AÑO']==p_ano]['MES'].dropna().unique()))

        elif filtro_tipo == "Semana ISO" and 'AÑO' in df_caps.columns and 'SEMANA' in df_caps.columns:
            p_ano = st.sidebar.selectbox("Año Base", sorted(df_caps['AÑO'].dropna().unique(), reverse=True))
            p_sem = st.sidebar.selectbox("Semana Productiva ISO", sorted(df_caps[df_caps['AÑO']==p_ano]['SEMANA'].dropna().unique()))

        # Módulo de Turnos
        turnos_disponibles = []
        col_t_caps = DataProcessor.find_column_exact_or_partial(df_caps, ['TURNO', 'SHIFT'])
        if col_t_caps: turnos_disponibles = df_caps[col_t_caps].dropna().unique().tolist()

        turnos_sel = turnos_disponibles
        if turnos_disponibles and filtro_tipo in ["Turno de Hoy (Smart)", "Día Exacto"]:
            turnos_sel = st.sidebar.multiselect("Asignar a Turno(s)", turnos_disponibles, default=turnos_disponibles)
        elif turnos_disponibles:
            with st.sidebar.expander("Control de Turnos Complejo"):
                turnos_sel = st.multiselect("Regla de Modificación Macro", turnos_disponibles, default=turnos_disponibles)

        st.sidebar.markdown("---")
        st.sidebar.markdown("## 🎯 3. Control de Objetivos")
        target_oee = st.sidebar.number_input("Benchmark OEE Tasa (%)", value=85.0, step=0.5)

        # Ejecución del Pipeline Matemático en el backend
        df_caps_f = FilterEngine.apply_master_filters(df_caps, f_inicio, f_fin, filtro_tipo, p_ano, p_mes, p_sem, turnos_sel)
        df_prod_f = FilterEngine.apply_master_filters(df_prod, f_inicio, f_fin, filtro_tipo, p_ano, p_mes, p_sem, turnos_sel)
        df_par_f  = FilterEngine.apply_master_filters(df_par,  f_inicio, f_fin, filtro_tipo, p_ano, p_mes, p_sem, turnos_sel)

        self.metricas = BusinessLogic.calcular_metricas(df_caps_f, df_prod_f, df_par_f)

        # =============================================================================
        # NUEVA SECCIÓN: FILTROS GRANULARES DE CATEGORÍA Y CAUSA PARA MÓDULO 2
        # =============================================================================
        # Conservamos el DataFrame maestro de paradas (ya filtrado por tiempo/turno) para
        # permitir un filtrado adicional por Categoría y Causa en el Análisis Científico Extendido.
        self.df_paradas_master = df_par_f.copy()

        st.sidebar.markdown("---")
        st.sidebar.markdown("## 🔍 4. Filtros de Análisis de Paradas (Módulo 2)")
        st.sidebar.caption("Aplican exclusivamente al 'Análisis Científico y Pareto Extendido'.")

        col_category = DataProcessor.find_column_exact_or_partial(self.df_paradas_master, ['CATEGORY', 'CATEGORIA'])
        col_cause = DataProcessor.find_column_exact_or_partial(self.df_paradas_master, ['CAUSE', 'CAUSA', 'MOTIVO'])

        if col_category:
            categorias_unicas = sorted(self.df_paradas_master[col_category].dropna().unique().tolist())
            self.filtro_categorias = st.sidebar.multiselect(
                "🗂️  Filtrar por Categoría", 
                options=categorias_unicas, 
                default=categorias_unicas,
                help="Segmente el análisis Pareto por categoría de evento."
            )
        else:
            self.filtro_categorias = []
            st.sidebar.info("Columna 'Category' no detectada en la matriz de paradas.")

        if col_cause:
            causas_unicas = sorted(self.df_paradas_master[col_cause].dropna().unique().tolist())
            self.filtro_causas = st.sidebar.multiselect(
                "🔎  Filtrar por Causa Específica", 
                options=causas_unicas, 
                default=causas_unicas,
                help="Profundice en causas raíz específicas dentro del Pareto."
            )
        else:
            self.filtro_causas = []
            st.sidebar.info("Columna 'Cause' no detectada en la matriz de paradas.")

        # =============================================================================
        # NUEVOS FILTROS: COD Y SISTEMAS PARA MÓDULO 2
        # =============================================================================
        # Se busca la columna COD exactamente como aparece en el Excel (sin tilde, en mayúsculas).
        # Esto garantiza que se use la columna correcta y no se confunda con otras similares.
        col_cod = DataProcessor.find_column_exact_or_partial(self.df_paradas_master, ['COD'])
        col_sistemas = DataProcessor.find_column_exact_or_partial(self.df_paradas_master, ['SISTEMAS', 'SISTEMA'])

        if col_cod:
            cod_unicos = sorted(self.df_paradas_master[col_cod].dropna().unique().tolist())
            self.filtro_cod = st.sidebar.multiselect(
                "🔢  Filtrar por COD", 
                options=cod_unicos, 
                default=cod_unicos,
                help="Segmente el análisis Pareto por código de parada (COD)."
            )
        else:
            self.filtro_cod = []
            st.sidebar.info("Columna 'COD' no detectada en la matriz de paradas.")

        if col_sistemas:
            sistemas_unicos = sorted(self.df_paradas_master[col_sistemas].dropna().unique().tolist())
            self.filtro_sistemas = st.sidebar.multiselect(
                "⚙️  Filtrar por Sistemas", 
                options=sistemas_unicos, 
                default=sistemas_unicos,
                help="Segmente el análisis Pareto por sistema afectado."
            )
        else:
            self.filtro_sistemas = []
            st.sidebar.info("Columna 'Sistemas' no detectada en la matriz de paradas.")

        # Persistencia de Metadata para Títulos y PDFs
        if filtro_tipo in ["Turno de Hoy (Smart)", "Día Exacto"]:
            self.ctx_str = f"{f_inicio}"
        else:
            self.ctx_str = f"Ventana Extendida: {f_inicio} hasta {f_fin}"

        self.str_turnos = ', '.join([str(t) for t in turnos_sel]) if turnos_sel else '100% Cobertura'

        return target_oee, df_caps_f

    def render_tab_executive_dashboard(self, target_oee, df_caps_f):
        """
        [PESTAÑA 1: DASHBOARD RESUMEN] 
        Construye la vista tradicional, manteniendo el control visual superior y la métrica de volúmenes.
        """
        st.markdown("### 1. Cuadro de Mando Integral: Resumen de Desempeño y Producción Neta")
        c1, c2, c3, c4 = st.columns(4)

        with c1:
            co_oee = "var(--success-color)" if self.metricas['OEE'] >= target_oee else "var(--danger-color)"
            st.markdown(f"<div class='metric-container'><div class='metric-title'>Índice OEE Consolidado</div><div class='metric-value' style='color:{co_oee};'>{self.metricas['OEE']:.1f}%</div><div class='metric-subtitle'>Meta Asignada: {target_oee}%</div></div>", unsafe_allow_html=True)
        with c2:
            st.markdown(f"<div class='metric-container'><div class='metric-title'>Producción Neta Conforme</div><div class='metric-value' style='color:var(--primary-color);'>{self.metricas['Prod_Conforme']:,.0f}</div><div class='metric-subtitle'>Volumen Aprobado (Liberado)</div></div>", unsafe_allow_html=True)
        with c3:
            st.markdown(f"<div class='metric-container'><div class='metric-title'>Chatarra Física / Mermas</div><div class='metric-value' style='color:var(--warning-color);'>{self.metricas['Prod_No_Conforme']:,.0f}</div><div class='metric-subtitle'>Unidades No Conformes</div></div>", unsafe_allow_html=True)
        with c4:
            st.markdown(f"<div class='metric-container'><div class='metric-title'>Destrucción Analítica (QA)</div><div class='metric-value' style='color:var(--text-muted);'>{self.metricas['Muestras_Calidad']:,.0f}</div><div class='metric-subtitle'>Muestreos de Calidad Retirados</div></div>", unsafe_allow_html=True)

        # -------------------------------------------------------------
        # GAUGES - FACTORES DEL OEE
        # -------------------------------------------------------------
        st.markdown("---")
        st.markdown("### 2. Disgregación Factorial TEE (Disponibilidad, Rendimiento, Calidad)")

        g1, g2, g3 = st.columns(3)
        self.fig_disp = PlotlyEngine.create_gauge(self.metricas['Disponibilidad'], "Factor Disponibilidad (A)", 90.0, "#0A2540")
        self.fig_rend = PlotlyEngine.create_gauge(self.metricas['Rendimiento'], "Factor Rendimiento (P)", 95.0, "#C07F00")
        self.fig_cal  = PlotlyEngine.create_gauge(self.metricas['Calidad'], "Factor Calidad Global (Q)", 99.0, "#2E8B57")

        with g1: st.plotly_chart(self.fig_disp, width="stretch")
        with g2: st.plotly_chart(self.fig_rend, width="stretch")
        with g3: st.plotly_chart(self.fig_cal, width="stretch")

        # -------------------------------------------------------------
        # LÍNEA DE VIDA DEL EQUIPO (TIMELINE)
        # -------------------------------------------------------------
        st.markdown("---")
        st.markdown("### 3. Trazabilidad de Estados: Línea de Vida de la Máquina en el Turno")
        self.fig_timeline = PlotlyEngine.create_timeline_gantt(self.metricas['Data_Timeline'])
        st.plotly_chart(self.fig_timeline, width="stretch")

        # -------------------------------------------------------------
        # RESPONSABILIDADES Y TOP 10 (INTACTO)
        # -------------------------------------------------------------
        st.markdown("---")
        col_izq, col_der = st.columns([1.6, 1])

        with col_izq:
            st.markdown("### 4. Matriz Pareto Crítica: Top 10 Detractores Operativos (Resumen)")
            self.fig_bar = PlotlyEngine.create_pareto_bar(self.metricas['Top_Paradas'])
            st.plotly_chart(self.fig_bar, width="stretch")

        with col_der:
            st.markdown("### 5. Responsabilidad Volumétrica por Operador")
            if 'Data_Operadores' in self.metricas and not self.metricas['Data_Operadores'].empty:
                self.fig_pie = PlotlyEngine.create_operator_pie(self.metricas['Data_Operadores'])
                st.plotly_chart(self.fig_pie, width="stretch")

                # Respaldo Tabular Físico
                st.markdown("**Matriz Auditiva de Volumen por Colaborador:**")
                st.dataframe(self.metricas['Data_Operadores'].style.format({self.metricas['Data_Operadores'].columns[1]: "{:,.0f}"}), width="stretch", hide_index=True)
            else:
                st.info("La matriz no contiene registros válidos de responsables de operación para este periodo.")

    def render_tab_deep_analytics(self):
        """
        [PESTAÑA 2: ANÁLISIS PROFUNDO]
        Despliega el estudio exhaustivo de Pareto sin límites de Top 10, y traza estadísticas duras.
        Ahora incorpora filtros dinámicos de Categoría, Causa, COD y Sistemas desde el panel lateral.
        """
        st.markdown("### 📈 Laboratorio de Análisis Profundo de Incidentes (Pareto Maestro)")
        st.write("Esta sección rompe el filtro del Top 10 y grafica la totalidad de los incidentes que mermaron la disponibilidad, aplicando la ley matemática del 80/20. Utilice los filtros de Categoría, Causa, COD y Sistemas en el panel lateral para segmentar el análisis.")

        # =============================================================================
        # MOTOR DE FILTRADO DINÁMICO POR CATEGORÍA, CAUSA, COD Y SISTEMAS (MÓDULO 2)
        # =============================================================================
        df_full = pd.DataFrame()

        if hasattr(self, 'df_paradas_master') and not self.df_paradas_master.empty:
            df_par = self.df_paradas_master.copy()

            # Localización robusta de columnas de clasificación
            col_category = DataProcessor.find_column_exact_or_partial(df_par, ['CATEGORY', 'CATEGORIA'])
            col_cause = DataProcessor.find_column_exact_or_partial(df_par, ['CAUSE', 'CAUSA', 'MOTIVO'])
            col_cod = DataProcessor.find_column_exact_or_partial(df_par, ['COD'])
            col_sistemas = DataProcessor.find_column_exact_or_partial(df_par, ['SISTEMAS', 'SISTEMA'])
            col_min = DataProcessor.find_column_exact_or_partial(df_par, ['PARADAS (MINUTOS)', 'MINUTOS'])
            col_desc = DataProcessor.find_column_exact_or_partial(df_par, ['DESCRIPCIÓN ESPECIFICA', 'DESCRIPCION ESPECIFICA', 'MOTIVO DE PARADA', 'FALLA'])

            # Aplicación de máscaras condicionales desde el sidebar
            if col_category and hasattr(self, 'filtro_categorias') and self.filtro_categorias:
                df_par = df_par[df_par[col_category].isin(self.filtro_categorias)]

            if col_cause and hasattr(self, 'filtro_causas') and self.filtro_causas:
                df_par = df_par[df_par[col_cause].isin(self.filtro_causas)]

            # Aplicación de filtros COD y Sistemas
            if col_cod and hasattr(self, 'filtro_cod') and self.filtro_cod:
                df_par = df_par[df_par[col_cod].isin(self.filtro_cod)]

            if col_sistemas and hasattr(self, 'filtro_sistemas') and self.filtro_sistemas:
                df_par = df_par[df_par[col_sistemas].isin(self.filtro_sistemas)]

            # Recálculo clínico del Pareto Maestro con los filtros aplicados
            if col_min and col_desc:
                df_par[col_min] = DataProcessor.safe_numeric_conversion(df_par[col_min])
                df_full = df_par.groupby(col_desc)[col_min].sum().reset_index()
                df_full = df_full.sort_values(by=col_min, ascending=False)
                df_full.rename(columns={col_desc: 'Descripcion', col_min: 'Minutos'}, inplace=True)
            else:
                # Fallback al Pareto total pre-calculado si no se pueden aplicar filtros granulares
                df_full = self.metricas.get('Data_Pareto_Total', pd.DataFrame()).copy()
        else:
            # Fallback si no hay DataFrame maestro disponible
            df_full = self.metricas.get('Data_Pareto_Total', pd.DataFrame()).copy()

        if df_full.empty:
            st.success("Operación a régimen óptimo. El sistema no ha capturado fallos mecánicos ni eléctricos en la matriz clínica para los filtros seleccionados.")
            return

        # =============================================================================
        # VISUALIZACIÓN ESPECTRAL DE PARETO (80/20)
        # =============================================================================
        self.fig_pareto_adv = PlotlyEngine.create_pareto_advanced(df_full)
        st.plotly_chart(self.fig_pareto_adv, width="stretch")

        # =============================================================================
        # DESPLIEGUE CRUDO DE DATA CON ACUMULADO NUMÉRICO
        # =============================================================================
        st.markdown("#### Matriz Descriptiva de Acumulación Numérica")
        df_display = df_full.copy()
        if not df_display.empty and 'Minutos' in df_display.columns:
            total_min = df_display['Minutos'].sum()
            if total_min > 0:
                df_display['Acumulado %'] = (df_display['Minutos'].cumsum() / total_min) * 100
            else:
                df_display['Acumulado %'] = 0.0

        st.dataframe(
            df_display.style.format({'Minutos': '{:.1f} m', 'Acumulado %': '{:.2f}%'}),
            width="stretch", hide_index=True
        )

    def trigger_pdf_pipeline(self):
        """
        Dispara y Orquesta: 
        1. Renderizado Invisible (Kaleido)
        2. Ensamblado A4 (FPDF) 
        3. Túnel API (Telegram)
        4. Descarga Local
        """
        with st.spinner("Desplegando Clúster Gráfico Kaleido. Vectorizando documentos e instanciando Reporte PDF en A4..."):

            # Ensamble invisible de Gauges para evitar 3 imágenes sueltas en el PDF
            fig_comb = make_subplots(rows=1, cols=3, specs=[[{'type': 'indicator'}, {'type': 'indicator'}, {'type': 'indicator'}]])
            fig_comb.add_trace(self.fig_disp.data[0], row=1, col=1)
            fig_comb.add_trace(self.fig_rend.data[0], row=1, col=2)
            fig_comb.add_trace(self.fig_cal.data[0], row=1, col=3)
            fig_comb.update_layout(height=350, margin=dict(t=50, b=20), paper_bgcolor='rgba(255,255,255,1)')

            img_paths = {}
            try:
                # Escribir los plots a disco en ultra-resolución HD
                p_g = os.path.join(AppConfig.TEMP_DIR, "x_gauges.png")
                fig_comb.write_image(p_g, engine="kaleido", width=1100, height=320)
                img_paths['gauges'] = p_g

                p_b = os.path.join(AppConfig.TEMP_DIR, "x_bar.png")
                self.fig_bar.write_image(p_b, engine="kaleido", width=950, height=450)
                img_paths['bar_paradas'] = p_b

                p_t = os.path.join(AppConfig.TEMP_DIR, "x_time.png")
                self.fig_timeline.write_image(p_t, engine="kaleido", width=1050, height=450)
                img_paths['timeline'] = p_t

                if hasattr(self, 'fig_pareto_adv'):
                    p_pa = os.path.join(AppConfig.TEMP_DIR, "x_pareto.png")
                    self.fig_pareto_adv.write_image(p_pa, engine="kaleido", width=1050, height=550)
                    img_paths['pareto_adv'] = p_pa

            except Exception as e:
                LogManager.error(f"Error de Kaleido (Vector Rendering): {e}")
                st.error("No se han podido trazar las gráficas para el PDF. Falla la dependencia 'kaleido'.")

            try:
                # Creación Física del FPDF A4
                pdf_engine = ReportGenerator(self.ctx_str, self.str_turnos)
                pdf_engine.add_cover_page()

                insights_array = QualityControl.generate_insights(self.metricas, 85.0)
                pdf_engine.build_executive_body(self.metricas, img_paths, insights_array)

                pdf_filename = f"Reporte_Consolidado_219_{datetime.now().strftime('%Y%m%d%H%M')}.pdf"
                pdf_path = os.path.join(AppConfig.TEMP_DIR, pdf_filename)
                pdf_engine.output(pdf_path)

                # Transmisión vía Bot Telegram (Capa Segura)
                success = TelegramGateway.dispatch_report(pdf_path, self.metricas, self.ctx_str)
                if success:
                    st.success("✅ **Certificado de Éxito:** El Reporte Gerencial fue vectorizado, convertido a A4 y distribuido por WhatsApp/Telegram a la Gerencia.")
                    st.balloons()
                else:
                    st.warning("⚠️ Interrupción de Red: El PDF está intacto en la computadora, pero los Servidores de Telegram no respondieron.")

                # Despliegue de botón de Rescate / Descarga Física
                with open(pdf_path, "rb") as pdf_file:
                    st.download_button(
                        label="💾 Descargar Respaldo Físico del Reporte PDF (A4)",
                        data=pdf_file,
                        file_name=pdf_filename,
                        mime="application/pdf"
                    )

            except Exception as e:
                LogManager.error(f"Falla masiva en generador FPDF: {e}")
                st.error(f"El Reporte PDF colapsó al estructurarse: {e}")

    def start_kernel(self):
        """Inicializador de Arranque del Dashboard (Main Boot Sequence)."""
        self.render_sidebar_ingestion()

        if not self.data_dict:
            st.info("👈 Señor, la plataforma está a la escucha. Arrastre la Matriz Excel de Planta (LURIN CAPS OEE) al panel izquierdo para inyectar los datos.")
            st.title(f"📊 Sistema Operativo Central (OEE) - {AppConfig.MAQUINA_NOMBRE}")

            with st.expander("📖 Asistente Interactivo de Inicialización", expanded=True):
                st.markdown("""
                **Bienvenido al entorno analítico corporativo CAVA Robotics.**

                El motor ha sido configurado para cargar automáticamente los incidentes del **Día de Hoy**.
                1. Al arrojar el Excel, el *Smart Default* se anclará a la última fecha productiva válida.
                2. Si ocurrieron fallas, las visualizará cronológicamente en el diagrama tipo Gantt de *Línea de Vida*.
                3. Al pulsar 'Exportar', todo será vectorizado en un Documento PDF y enviado sin requerir intervención extra.
                """)
            return

        target_oee, df_caps_f = self.render_sidebar_filters()

        st.title("📊 Panel Gerencial Corporativo (Dashboard OEE)")
        st.markdown(f"""
            <div class="info-box">
                <h4><span style="font-size: 1.3em;">⚙️</span> Identificador de Activo: {AppConfig.MAQUINA_NOMBRE} (Línea {AppConfig.MAQUINA_ID})</h4>
                <p>
                    <strong>Alcance Dinámico:</strong> {self.ctx_str} &nbsp;|&nbsp;
                    <strong>Turno de Trabajo:</strong> {self.str_turnos} &nbsp;|&nbsp;
                    <strong>Velocidad de Planta:</strong> {AppConfig.PRODUCCION_NOMINAL_HORA:,.0f} placas/hora
                </p>
            </div>
        """, unsafe_allow_html=True)

        # -------------------------------------------------------------
        # SISTEMA DE PESTAÑAS (TABS) MODERNOS
        # -------------------------------------------------------------
        tab1, tab2 = st.tabs([
            "📋 MÓDULO 1: Dashboard Ejecutivo (Resumen)", 
            "📈 MÓDULO 2: Análisis Científico y Pareto Extendido"
        ])

        with tab1:
            self.render_tab_executive_dashboard(target_oee, df_caps_f)

        with tab2:
            self.render_tab_deep_analytics()

        # -------------------------------------------------------------
        # DESPACHO PDF E INTERFAZ DE EXPORTACIÓN
        # -------------------------------------------------------------
        st.markdown("---")
        st.markdown("### 📤 CAVA Bot: Despacho Automatizado de Análisis")
        _, col_btn, _ = st.columns([1,2,1])
        with col_btn:
            if st.button("📄 Generar Informe Ejecutivo en Alta Resolución y Despachar vía CAVA Bot"):
                self.trigger_pdf_pipeline()

# ==================================================================================================
# EJECUCIÓN DEL KERNEL CAVA (INICIO DEL PROGRAMA)
# ==================================================================================================
if __name__ == "__main__":
    app_kernel = DashboardUI()
    app_kernel.start_kernel()

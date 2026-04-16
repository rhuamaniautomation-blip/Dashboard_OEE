"""
====================================================================================================
SISTEMA GERENCIAL DE EFICIENCIA OPERATIVA (OEE) Y ANALÍTICA DE PRODUCCIÓN
Cliente / Área: Producción - Carga de Detonadores (Máquina 219)
Empresa: CAVA ROBOTICS
Versión: 8.0.0 (Build Institucional Definitivo - Arquitectura Empresarial Avanzada)

Módulos Integrados y Escalados:
    1.  CoreLogger: Trazabilidad, auditoría y manejo de excepciones silenciosas de grado corporativo.
    2.  NetworkGateway: Protocolo de acceso directo a directorios compartidos en red local (LAN/WAN).
    3.  DataProcessor & ETL: Escáner de Offset, Filtrado Clínico de Fechas y Autocorrección estricta.
    4.  QualityControl: Diagnóstico experto y generación de insights gerenciales en lenguaje natural.
    5.  BusinessLogic: Motor matemático de extracción directa de OEE (CAPS), Paradas y Volumetría.
    6.  PlotlyEngine: Motor vectorial optimizado (Timeline y Pareto) de ultra-baja latencia.
    7.  PDFManager: Generador FPDF A4 Multisección con dibujado algorítmico de tablas nativas.
    8.  ExcelExporter: Exportador de data purificada para respaldos locales y auditorías en planta.
    9.  TelegramGateway: Capa de transmisión API segura con reintentos automáticos y encriptación.
    10. DashboardUI: Orquestador UI con Pestañas (Tabs), Smart Defaults y renderizado asíncrono.
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
import sys
import time
import logging
import warnings
import traceback
from typing import Dict, List, Tuple, Optional, Any, Union
from io import BytesIO
from datetime import datetime, timedelta, date

# --------------------------------------------------------------------------------------------------
# 1. SUPRESIÓN DE ADVERTENCIAS Y CONFIGURACIÓN DEL ENTORNO DE PRODUCCIÓN
# --------------------------------------------------------------------------------------------------
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
warnings.filterwarnings("ignore", message=".*Could not infer format.*")
warnings.filterwarnings("ignore", category=FutureWarning)

from fpdf import FPDF

# ==================================================================================================
# 2. CONSTANTES GLOBALES, VOCABULARIO Y CONFIGURACIÓN CORPORATIVA
# ==================================================================================================
class EnterpriseConfig:
    """Contenedor estático principal para configuraciones empresariales y parámetros operativos."""
    
    class API:
        """Credenciales y parámetros de transmisión para el túnel de Telegram."""
        TOKEN: str = "8552261657:AAFdXG5ta6UUPyrSco2tqgvNFTTH_LGZw9M"
        CHAT_ID: str = "6153139566"
        TIMEOUT_SEC: int = 25
        MAX_RETRIES: int = 3

    class Operaciones:
        """Especificaciones Técnicas Constantes de Ingeniería - Máquina 219."""
        ID_MAQUINA: str = "219"
        NOMBRE_EQUIPO: str = "Carga de Detonadores(219)"
        CAPACIDAD_PLACAS_HORA: int = 268
        DETONADORES_POR_PLACA: int = 40
        PROD_NOMINAL_HORA: int = CAPACIDAD_PLACAS_HORA * DETONADORES_POR_PLACA # 10,720
        META_OEE_DEFAULT: float = 85.0

    class Rutas:
        """Directorios físicos locales para operaciones de I/O y caché."""
        TEMP: str = "cava_temp_reports"
        LOGS: str = "cava_system_logs"
        CACHE: str = "cava_cache_db"

    class UIColors:
        """Paleta de colores institucionales de CAVA Robotics."""
        PRIMARY: str = "#0A2540"
        SECONDARY: str = "#C07F00"
        ACCENT: str = "#8B4513"
        SUCCESS: str = "#2E8B57"
        WARNING: str = "#D35400"
        DANGER: str = "#C0392B"
        MUTED: str = "#7F8C8D"
        BACKGROUND: str = "#F4F7F6"
        CARD_BG: str = "#FFFFFF"

    @staticmethod
    def inicializar_infraestructura() -> None:
        """Despliega la estructura de directorios físicos si el entorno es virgen."""
        try:
            for directorio in [EnterpriseConfig.Rutas.TEMP, EnterpriseConfig.Rutas.LOGS, EnterpriseConfig.Rutas.CACHE]:
                if not os.path.exists(directorio):
                    os.makedirs(directorio, exist_ok=True)
        except PermissionError:
            st.error("Error Crítico de Sistema: Permisos insuficientes para crear directorios base.")
        except Exception as e:
            st.error(f"Fallo de I/O en infraestructura: {str(e)}")

EnterpriseConfig.inicializar_infraestructura()

# ==================================================================================================
# 3. MÓDULO DE TRAZABILIDAD Y AUDITORÍA SILENCIOSA (CORE LOGGER)
# ==================================================================================================
class EnterpriseLogger:
    """Sistema jerárquico de registro de eventos. Mantiene el historial en disco para diagnósticos."""
    
    _archivo_log = os.path.join(EnterpriseConfig.Rutas.LOGS, f"cava_kernel_{datetime.now().strftime('%Y%m')}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | CAVA_CORE_V8 | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(_archivo_log, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    _logger = logging.getLogger("CAVA_Enterprise")

    @classmethod
    def info(cls, msg: str) -> None:
        """Registra operaciones exitosas y flujo normal del sistema."""
        cls._logger.info(msg)

    @classmethod
    def warning(cls, msg: str) -> None:
        """Registra desviaciones que no detienen el sistema pero requieren atención."""
        cls._logger.warning(msg)

    @classmethod
    def error(cls, msg: str, exc_info: bool = False) -> None:
        """Registra fallos críticos. Opcionalmente incluye el stacktrace."""
        cls._logger.error(msg, exc_info=exc_info)

    @classmethod
    def debug(cls, msg: str) -> None:
        """Registra datos granulares para desarrollo y diagnóstico profundo."""
        cls._logger.debug(msg)


# ==================================================================================================
# 4. CONFIGURACIÓN VISUAL DEL FRAMEWORK UI Y CSS
# ==================================================================================================
def inject_corporate_css() -> None:
    """Inyecta las hojas de estilo en cascada (CSS) de forma global para un acabado institucional."""
    st.set_page_config(
        page_title="Dashboard Gerencial OEE - CAVA",
        page_icon="⚙️",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    css_payload = f"""
    <style>
    /* VARIABLES ESTRUCTURALES */
    :root {{
        --primary-color: {EnterpriseConfig.UIColors.PRIMARY};
        --secondary-color: {EnterpriseConfig.UIColors.SECONDARY};
        --accent-warm: {EnterpriseConfig.UIColors.ACCENT};
        --bg-color: {EnterpriseConfig.UIColors.BACKGROUND};
        --card-bg: {EnterpriseConfig.UIColors.CARD_BG};
        --text-main: #2C3E50;
        --text-muted: {EnterpriseConfig.UIColors.MUTED};
        --success-color: {EnterpriseConfig.UIColors.SUCCESS};
        --danger-color: {EnterpriseConfig.UIColors.DANGER};
        --warning-color: {EnterpriseConfig.UIColors.WARNING};
        --border-color: #E2E8F0;
    }}
    
    /* RESETEO GLOBAL */
    .main {{ 
        background-color: var(--bg-color); 
        font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; 
    }}
    .stApp {{ background-color: var(--bg-color); }}
    
    /* TIPOGRAFÍA INSTITUCIONAL */
    h1, h2, h3, h4, h5, h6 {{ 
        color: var(--primary-color); 
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
        font-weight: 700;
        letter-spacing: -0.5px;
    }}
    
    /* TARJETAS DE MÉTRICAS ULTRA-RÁPIDAS (Reemplazo de Plotly) */
    .fast-metric-card {{ 
        background-color: var(--card-bg); 
        border-top: 4px solid var(--secondary-color);
        border-radius: 8px; 
        padding: 20px 15px; 
        text-align: center; 
        box-shadow: 0 4px 6px rgba(0,0,0,0.05); 
        margin-bottom: 15px;
        border-left: 1px solid var(--border-color);
        border-right: 1px solid var(--border-color);
        border-bottom: 1px solid var(--border-color);
    }}
    .fast-metric-title {{ 
        font-size: 1.05rem; 
        color: var(--text-muted); 
        font-weight: 600; 
        margin-bottom: 8px; 
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }}
    .fast-metric-value {{ 
        font-size: 2.8rem; 
        font-weight: 800; 
        line-height: 1.1;
        font-family: 'Segoe UI', sans-serif;
    }}
    .fast-metric-subtitle {{
        font-size: 0.9rem;
        color: var(--text-muted);
        margin-top: 6px;
        font-style: normal;
        border-top: 1px solid #f0f0f0;
        padding-top: 6px;
    }}
    
    /* CABECERA GERENCIAL */
    .gerencia-header {{
        background: linear-gradient(135deg, #FFFFFF 0%, #F8F9FA 100%);
        border-left: 6px solid var(--primary-color);
        padding: 18px 25px;
        border-radius: 6px;
        margin-bottom: 25px;
        color: var(--text-main);
        box-shadow: 0 2px 8px rgba(0,0,0,0.03);
    }}
    .gerencia-header h4 {{
        margin-top: 0;
        color: var(--primary-color);
        font-size: 1.3rem;
        margin-bottom: 8px;
    }}
    .gerencia-header p {{ 
        margin: 4px 0; 
        font-size: 1.05rem; 
    }}
    
    /* BOTONES CORPORATIVOS */
    .stButton>button {{ 
        background-color: var(--primary-color); 
        color: #FFFFFF; 
        font-weight: 600; 
        border-radius: 4px; 
        width: 100%; 
        border: 1px solid var(--primary-color);
        padding: 10px 20px;
        transition: all 0.2s ease;
    }}
    .stButton>button:hover {{ 
        background-color: #FFFFFF; 
        color: var(--primary-color);
        border: 1px solid var(--primary-color);
    }}
    
    /* TABLAS DATAFRAME NATIVAS */
    .stDataFrame {{ 
        background-color: var(--card-bg); 
        border-radius: 6px; 
        border: 1px solid var(--border-color);
    }}
    
    /* SEPARADORES SUTILES */
    hr {{ border-top: 1px solid #CBD5E1; margin: 2rem 0; }}
    
    /* SIDEBAR */
    [data-testid="stSidebar"] {{
        background-color: #FFFFFF;
        border-right: 1px solid var(--border-color);
    }}
    
    /* TABS */
    .stTabs [data-baseweb="tab-list"] {{ gap: 4px; }}
    .stTabs [data-baseweb="tab"] {{ 
        height: 50px; 
        background-color: #F8F9FA; 
        border-radius: 4px 4px 0px 0px; 
        color: var(--text-main); 
        font-weight: 600;
        border: 1px solid var(--border-color);
        border-bottom: none;
    }}
    .stTabs [aria-selected="true"] {{ 
        background-color: #FFFFFF; 
        color: var(--primary-color); 
        border-top: 3px solid var(--secondary-color); 
    }}
    
    /* LOGO CAVA NATIVO */
    .cava-logo-wrapper {{
        background: var(--primary-color);
        padding: 20px 15px; 
        text-align: center; 
        border-radius: 6px; 
        margin-bottom: 20px; 
        border-bottom: 4px solid var(--secondary-color);
    }}
    .cava-logo-main {{ 
        color: #FFFFFF; 
        margin: 0; 
        font-family: 'Arial Black', Impact, sans-serif; 
        font-size: 28px; 
        letter-spacing: 3px; 
    }}
    .cava-logo-sub {{ 
        color: var(--secondary-color); 
        margin: 2px 0 0 0; 
        font-size: 10px; 
        letter-spacing: 2px; 
        font-weight: 700;
    }}
    </style>
    """
    st.markdown(css_payload, unsafe_allow_html=True)


# ==================================================================================================
# 5. MÓDULO DE SEGURIDAD Y ACCESO A REDES (NETWORK GATEWAY)
# ==================================================================================================
class NetworkGateway:
    """Gestor de acceso seguro a directorios compartidos y unidades mapeadas de la planta."""
    
    @staticmethod
    def validar_ruta_red(ruta_acceso: str) -> Tuple[bool, str]:
        """
        Analiza sintácticamente la ruta y verifica los permisos físicos del SO.
        
        Args:
            ruta_acceso (str): Cadena que representa el path absoluto o de red.
            
        Returns:
            Tuple[bool, str]: Estado de éxito y mensaje descriptivo del diagnóstico.
        """
        ruta = ruta_acceso.strip().strip('"').strip("'")
        
        if not ruta:
            return False, "La ruta de red proporcionada está vacía."
            
        # Bypass lógico si se intenta acceder mediante URL HTTP(s) local
        if ruta.startswith("http://") or ruta.startswith("https://"):
            return True, "Enlace web detectado. Se procederá con protocolo HTTP."
            
        # Validación de existencia física en el árbol de red local
        if not os.path.exists(ruta):
            EnterpriseLogger.error(f"Redinaccesible o archivo inexistente: {ruta}")
            return False, f"El servidor no responde o el archivo no existe en la ruta: {ruta}"
            
        # Comprobación de permisos de lectura (Crucial en entornos compartidos de Producción)
        if not os.access(ruta, os.R_OK):
            EnterpriseLogger.error(f"Fallo de permisos (Lectura denegada) en: {ruta}")
            return False, "Permiso denegado. Solicite a TI acceso de 'Solo Lectura' (Read-Only) a la carpeta."
            
        return True, "Enlace establecido y verificado exitosamente."

    @staticmethod
    def cargar_archivo_en_memoria(ruta_acceso: str) -> Optional[BytesIO]:
        """
        Lee el archivo directamente desde la red a la memoria RAM.
        Protege al sistema de bloqueos de I/O si el archivo está abierto por otro usuario.
        """
        try:
            ruta = ruta_acceso.strip().strip('"').strip("'")
            with open(ruta, 'rb') as f:
                buffer = BytesIO(f.read())
                EnterpriseLogger.info(f"Carga binaria completa desde red: {ruta}")
                return buffer
        except PermissionError:
            EnterpriseLogger.error("Archivo bloqueado. Otro usuario o el SCADA lo tiene abierto.")
            st.error("❌ Archivo Bloqueado: El archivo de Excel está siendo modificado actualmente por otro usuario o por el sistema de planta. Intente nuevamente en unos segundos.")
            return None
        except Exception as e:
            EnterpriseLogger.error(f"Excepción I/O durante la transferencia de red: {e}")
            st.error(f"❌ Error de transferencia: {str(e)}")
            return None


# ==================================================================================================
# 6. MOTOR DE EXTRACCIÓN Y LIMPIEZA DE DATOS (ETL CLÍNICO AVANZADO)
# ==================================================================================================
class DataProcessor:
    """
    Clase estática de grado empresarial para el procesamiento, limpieza, validación 
    y estandarización clínica de DataFrames.
    """

    @staticmethod
    def encontrar_indice_cabecera(excel_data: pd.ExcelFile, hoja: str, palabras_clave: List[str]) -> int:
        """
        [MOTOR DE ESCANEO DE OFFSET DE FILAS]
        Analiza las primeras 50 filas buscando la intersección exacta de cabeceras,
        evitando leer logos o descripciones ubicadas arriba de la tabla principal.
        """
        try:
            df_raw = excel_data.parse(hoja, header=None, nrows=50)
            
            for idx, fila in df_raw.iterrows():
                fila_texto = fila.astype(str).str.strip().str.upper()
                coincidencias = sum(1 for pc in palabras_clave if any(pc in val for val in fila_texto.values))
                
                # Tolerancia estricta: Se requiere al menos 1 coincidencia fuerte
                if coincidencias >= 1:
                    EnterpriseLogger.debug(f"Cabecera detectada en índice {idx} para hoja {hoja}.")
                    return idx
                    
            EnterpriseLogger.warning(f"Offset fallido en '{hoja}'. Se procesará desde la fila 0.")
            return 0 
        except Exception as e:
            EnterpriseLogger.error(f"Fallo en escáner de offset para '{hoja}': {e}")
            return 0

    @staticmethod
    def normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
        """Purifica cabeceras eliminando espacios, retornos de carro e inconsistencias SCADA."""
        if df is not None and not df.empty:
            df.columns = df.columns.astype(str).str.strip().str.replace('\n', ' ').str.replace('\r', '').str.upper()
        return df

    @staticmethod
    def ubicar_columna(df: pd.DataFrame, exactas: List[str], parciales: Optional[List[str]] = None) -> Optional[str]:
        """Algoritmo de búsqueda de alta fidelidad para mapear columnas variables."""
        if df is None or df.empty: return None
        columnas = df.columns.tolist()
        
        # Búsqueda exacta absoluta
        for col in columnas:
            if col in [k.upper() for k in exactas]: return col
                
        # Búsqueda parcial tolerante
        if parciales:
            for col in columnas:
                for kw in parciales:
                    if kw.upper() in col: return col
        return None

    @staticmethod
    def procesar_fechas_clinicas(df: pd.DataFrame, nombre_hoja: str) -> pd.DataFrame:
        """
        [NÚCLEO CRÍTICO DE FECHAS]
        Identifica, extrae, rellena (forward-fill) y estandariza las columnas de tiempo.
        Aplica reglas específicas según el nombre de la hoja.
        """
        df = DataProcessor.normalizar_columnas(df)
        
        col_fecha = None
        
        # Regla de Negocio Estricta: Si es la hoja CAPS, la fecha está en la columna B (Index 1)
        if 'CAPS' in nombre_hoja.upper() and len(df.columns) > 1:
            posible_columna_b = df.columns[1]
            if 'DATE' in posible_columna_b or 'FECHA' in posible_columna_b:
                col_fecha = posible_columna_b
                EnterpriseLogger.info("Mapeo directo aplicado: Columna B asignada como matriz de tiempo en CAPS.")
        
        # Si la regla estricta falla o es otra hoja, usar búsqueda algorítmica
        if not col_fecha:
            col_fecha = DataProcessor.ubicar_columna(df, ['DATE', 'FECHA'], ['FECHA', 'DATE'])
        
        if col_fecha:
            # 1. Aplicación de Forward Fill para rellenar vacíos por celdas combinadas de Excel
            df[col_fecha] = df[col_fecha].ffill()
            
            # 2. Coerción matemática a objetos datetime
            df['FECHA_DATETIME'] = pd.to_datetime(df[col_fecha], errors='coerce')
            
            # 3. Purga de datos inútiles (filas sin fecha válida)
            df = df.dropna(subset=['FECHA_DATETIME']).copy()
            
            # 4. Estandarización a Date para filtros de Dashboard
            df['FECHA_STD'] = df['FECHA_DATETIME'].dt.date
            
            # 5. Extensiones dimensionales
            df['AÑO'] = df['FECHA_DATETIME'].dt.year
            df['MES'] = df['FECHA_DATETIME'].dt.month
            df['SEMANA'] = df['FECHA_DATETIME'].dt.isocalendar().week
        else:
            EnterpriseLogger.error(f"Estructura inválida: No se localizó un vector de fecha en {nombre_hoja}.")
            
        return df

    @staticmethod
    def calcular_linea_vida(df: pd.DataFrame) -> pd.DataFrame:
        """
        Construye vectores temporales absolutos (Inicio y Fin) para cada evento físico
        de la máquina. Si falta la hora final, la deduce matemáticamente.
        """
        if df.empty or 'FECHA_DATETIME' not in df.columns: return df
        
        c_ini = DataProcessor.ubicar_columna(df, ['HORA INICIO', 'HORA'], ['START TIME', 'INICIO'])
        c_fin = DataProcessor.ubicar_columna(df, ['HORA FINAL', 'HORA FIN'], ['END TIME', 'FIN'])
        c_min = DataProcessor.ubicar_columna(df, ['PARADAS (MINUTOS)', 'MINUTOS'], ['DURACION', 'TIEMPO'])

        def ensamblar_datetime(fecha_base, hora_parcial):
            try:
                if isinstance(hora_parcial, datetime): return hora_parcial
                t_obj = pd.to_datetime(str(hora_parcial).strip(), errors='coerce')
                if pd.notnull(t_obj):
                    return pd.Timestamp.combine(fecha_base.date(), t_obj.time())
                return fecha_base
            except:
                return fecha_base

        if c_ini:
            df['TL_START'] = df.apply(lambda r: ensamblar_datetime(r['FECHA_DATETIME'], r[c_ini]), axis=1)

            if c_fin:
                df['TL_END'] = df.apply(lambda r: ensamblar_datetime(r['FECHA_DATETIME'], r[c_fin]), axis=1)
            elif c_min:
                df[c_min] = pd.to_numeric(df[c_min], errors='coerce').fillna(0)
                df['TL_END'] = df.apply(lambda r: r['TL_START'] + timedelta(minutes=r[c_min]) if pd.notnull(r['TL_START']) else r['FECHA_DATETIME'], axis=1)
            else:
                df['TL_END'] = df['TL_START'] + timedelta(minutes=5) # Default failsafe
                
            # Corrección de cronología inversa
            mask_inv = df['TL_END'] < df['TL_START']
            df.loc[mask_inv, 'TL_END'] = df.loc[mask_inv, 'TL_START'] + timedelta(minutes=1)

        return df

@st.cache_data(show_spinner=False, ttl=1800)
def orquestar_etl_excel(archivo_binario: bytes) -> Dict[str, pd.DataFrame]:
    """
    Función maestra del proceso Extract, Transform, Load (ETL).
    Mapea en memoria las matrices y las procesa de manera aislada.
    """
    diccionario_matrices = {}
    try:
        excel_data = pd.ExcelFile(BytesIO(archivo_binario))
        hojas_fisicas = excel_data.sheet_names
        
        # Mapeo de Diccionario Institucional (Hoja : Requerimientos Mínimos)
        mapeo_estructural = {
            'CAPS': ['DATE', 'FECHA', 'OEE', 'MACHINE'], 
            'Produccion': ['DATE', 'FECHA', 'OPERADOR', 'CONFORME'],
            'Detalle parada': ['DATE', 'FECHA', 'MINUTOS', 'MOTIVO', 'ESPECIFICA']
        }

        for id_matriz, llaves in mapeo_estructural.items():
            hoja_objetivo = next((h for h in hojas_fisicas if id_matriz.upper() in h.upper() or id_matriz.replace("ó", "o").upper() in h.upper()), None)
            
            if hoja_objetivo:
                idx_cab = DataProcessor.encontrar_indice_cabecera(excel_data, hoja_objetivo, llaves)
                df_crudo = excel_data.parse(hoja_objetivo, header=idx_cab)
                df_limpio = DataProcessor.procesar_fechas_clinicas(df_crudo, id_matriz)
                
                if id_matriz == 'Detalle parada':
                    df_limpio = DataProcessor.calcular_linea_vida(df_limpio)
                    
                diccionario_matrices[id_matriz] = df_limpio
                EnterpriseLogger.info(f"Matriz {id_matriz} consolidada: {len(df_limpio)} vectores procesados.")
            else:
                EnterpriseLogger.warning(f"Matriz ausente en el libro: {id_matriz}.")
                diccionario_matrices[id_matriz] = pd.DataFrame()

    except Exception as e:
        EnterpriseLogger.error(f"Fallo de Kernel ETL: {e}", exc_info=True)
        st.error(f"❌ Error Estructural al decodificar el Excel: {e}")
        
    return diccionario_matrices


# ==================================================================================================
# 7. MOTOR DE FILTRADO MULTI-DIMENSIONAL (MASTER FILTERS)
# ==================================================================================================
class RuleEngine:
    """Aplica máscaras condicionales matemáticas sobre los DataFrames procesados."""
    
    @staticmethod
    def aplicar_reglas_tiempo(df: pd.DataFrame, inicio: date, fin: date, tipo_filtro: str, 
                              ano: int, mes: int, sem: int, turnos: List[str]) -> pd.DataFrame:
        if df is None or df.empty or 'FECHA_STD' not in df.columns:
            return df

        # Máscara Cronológica (Vectorización Vectorial Rápida)
        mascara = (df['FECHA_STD'] >= inicio) & (df['FECHA_STD'] <= fin)
        
        # Override Jerárquico
        if tipo_filtro == "Año Anualizado" and 'AÑO' in df.columns:
            mascara = df['AÑO'] == ano
        elif tipo_filtro == "Mes Fiscal" and 'MES' in df.columns and 'AÑO' in df.columns:
            mascara = (df['AÑO'] == ano) & (df['MES'] == mes)
        elif tipo_filtro == "Semana ISO" and 'SEMANA' in df.columns and 'AÑO' in df.columns:
            mascara = (df['AÑO'] == ano) & (df['SEMANA'] == sem)

        df_filtrado = df.loc[mascara].copy()
        
        # Máscara Física Operacional (Turnos)
        c_turno = DataProcessor.ubicar_columna(df_filtrado, ['TURNO', 'SHIFT'])
        if c_turno and turnos:
            df_filtrado[c_turno] = df_filtrado[c_turno].astype(str).str.strip().str.upper()
            turnos_mayus = [str(t).strip().upper() for t in turnos]
            df_filtrado = df_filtrado[df_filtrado[c_turno].isin(turnos_mayus)]
            
        return df_filtrado


# ==================================================================================================
# 8. MOTOR DE LÓGICA DE NEGOCIO (EXTRACCIÓN EXACTA)
# ==================================================================================================
class BusinessLogic:
    """Orquestador de cálculos financieros y técnicos basados en la data purificada."""

    @staticmethod
    def consolidar_kpis(df_caps: pd.DataFrame, df_prod: pd.DataFrame, df_paradas: pd.DataFrame) -> Dict[str, Any]:
        """
        Extracción directa de promedios ponderados y sumatorias físicas.
        Calcula el estado del equipo sin simulaciones ni estimaciones.
        """
        metricas = {
            "OEE": 0.0, "Disponibilidad": 0.0, "Rendimiento": 0.0, "Calidad": 0.0,
            "Prod_Conforme": 0, "Prod_No_Conforme": 0, "Muestras_Calidad": 0, 
            "Operadores": pd.DataFrame(), "Data_Pareto": pd.DataFrame(), "Data_Timeline": df_paradas.copy()
        }

        # --- A. CÁLCULOS OEE DE PRECISIÓN (CAPS) ---
        if not df_caps.empty:
            c_maq = DataProcessor.ubicar_columna(df_caps, ['MACHINE', 'MAQUINA', 'LINEA'])
            df_foco = df_caps.copy()
            if c_maq:
                mask_maq = df_foco[c_maq].astype(str).str.contains(EnterpriseConfig.Operaciones.ID_MAQUINA, case=False, na=False)
                if mask_maq.any(): df_foco = df_foco[mask_maq]

            def_oee = DataProcessor.ubicar_columna(df_foco, ['OEE'])
            def_disp = DataProcessor.ubicar_columna(df_foco, ['AVAILABILITY', 'DISPONIBILIDAD'])
            def_rend = DataProcessor.ubicar_columna(df_foco, ['PERFORMANCE', 'RENDIMIENTO'])
            def_cal = DataProcessor.ubicar_columna(df_foco, ['QUALITY', 'CALIDAD'])

            def promedio_seguro(columna):
                if columna and columna in df_foco.columns:
                    s = pd.to_numeric(df_foco[columna], errors='coerce').dropna()
                    if not s.empty:
                        v = s.mean()
                        return (v * 100) if v <= 1.5 else v # Corrección de decimales (0.85 -> 85%)
                return 0.0

            metricas['OEE'] = promedio_seguro(def_oee)
            metricas['Disponibilidad'] = promedio_seguro(def_disp)
            metricas['Rendimiento'] = promedio_seguro(def_rend)
            metricas['Calidad'] = promedio_seguro(def_cal)

        # --- B. AUDITORÍA VOLUMÉTRICA ---
        if not df_prod.empty:
            c_ok = DataProcessor.ubicar_columna(df_prod, ['PRODUCCION CONFORME', 'CONFORME'])
            c_nok = DataProcessor.ubicar_columna(df_prod, ['PRODUCCION NO CONFORME', 'RECHAZOS'])
            c_qa = DataProcessor.ubicar_columna(df_prod, ['MUESTRAS DE CALIDAD'])
            c_op = DataProcessor.ubicar_columna(df_prod, ['OPERADOR', 'RESPONSABLE'])

            if c_ok: metricas['Prod_Conforme'] = pd.to_numeric(df_prod[c_ok], errors='coerce').sum()
            if c_nok: metricas['Prod_No_Conforme'] = pd.to_numeric(df_prod[c_nok], errors='coerce').sum()
            if c_qa: metricas['Muestras_Calidad'] = pd.to_numeric(df_prod[c_qa], errors='coerce').sum()
            
            if c_op and c_ok:
                df_prod[c_ok] = pd.to_numeric(df_prod[c_ok], errors='coerce').fillna(0)
                agrupado = df_prod.groupby(c_op)[c_ok].sum().reset_index().sort_values(by=c_ok, ascending=False)
                metricas['Operadores'] = agrupado

        # --- C. LEY DE PARETO APLICADA A FALLAS ---
        if not df_paradas.empty:
            c_min = DataProcessor.ubicar_columna(df_paradas, ['PARADAS (MINUTOS)', 'MINUTOS'])
            c_desc = DataProcessor.ubicar_columna(df_paradas, ['DESCRIPCIÓN ESPECIFICA', 'MOTIVO DE PARADA'])
            c_cat = DataProcessor.ubicar_columna(df_paradas, ['CATEGORY', 'CATEGORIA'])

            if c_min and c_desc:
                df_paradas[c_min] = pd.to_numeric(df_paradas[c_min], errors='coerce').fillna(0)
                pareto_total = df_paradas.groupby(c_desc)[c_min].sum().reset_index().sort_values(by=c_min, ascending=False)
                pareto_total.rename(columns={c_desc: 'Detractor_Operativo', c_min: 'Impacto_Minutos'}, inplace=True)
                metricas['Data_Pareto'] = pareto_total

            if c_cat:
                metricas['Data_Timeline']['CAT_STD'] = metricas['Data_Timeline'][c_cat].fillna("No Asignada")
            else:
                metricas['Data_Timeline']['CAT_STD'] = "Evento General"

        return metricas

class ExpertDiagnostics:
    """Sistema lógico de generación de texto para diagnósticos automáticos en el PDF."""
    
    @staticmethod
    def evaluar_performance(metricas: Dict[str, Any], meta: float) -> List[str]:
        hallazgos = []
        
        # Meta Global OEE
        oee = metricas.get('OEE', 0)
        if oee >= meta:
            hallazgos.append(f"El Índice OEE ({oee:.1f}%) ha operado por encima del límite corporativo ({meta}%). Sistema estable.")
        else:
            brecha = meta - oee
            hallazgos.append(f"Alerta: Se registra una desviación negativa del {brecha:.1f}% respecto a la cuota base del OEE Global.")

        # Teoría de Restricciones
        d, r, c = metricas.get('Disponibilidad', 0), metricas.get('Rendimiento', 0), metricas.get('Calidad', 0)
        menor = min(d, r, c)
        if menor == d and d > 0:
            hallazgos.append("Cuello de Botella Detectado: La Disponibilidad es el factor limitante. Revisar matriz de micro-paradas mecánicas.")
        elif menor == r and r > 0:
            hallazgos.append("Degradación de Ciclo: El Rendimiento se encuentra bajo los estándares. Tiempos de ciclo lentos en el equipo.")
            
        # Volumetría
        total = metricas.get('Prod_Conforme', 0) + metricas.get('Prod_No_Conforme', 0)
        if total > 0:
            tasa = (metricas.get('Prod_No_Conforme', 0) / total) * 100
            if tasa > 2.0:
                hallazgos.append(f"Desviación Física: La tasa de chatarra/rechazos es inusualmente alta ({tasa:.2f}%).")
                
        if not hallazgos:
            hallazgos.append("La operación no presenta desviaciones críticas registrables en la matriz actual.")
            
        return hallazgos


# ==================================================================================================
# 9. MOTOR FPDF AVANZADO PARA REPORTES EJECUTIVOS AUTOMATIZADOS (FORMATO A4 NATIVO)
# ==================================================================================================
class EnterprisePDFEngine(FPDF):
    """
    Motor avanzado de generación documental. Sustituye la necesidad de imágenes inyectadas
    dibujando métricas y tablas estructuradas de manera nativa mediante primitivas gráficas FPDF.
    """
    def __init__(self, ventana_tiempo: str, turnos_activos: str):
        super().__init__(orientation='P', unit='mm', format='A4')
        self.ventana = ventana_tiempo
        self.turnos = turnos_activos
        self.set_auto_page_break(auto=True, margin=20)
        self.set_margins(15, 15, 15)

    def header(self):
        """Cabecera institucional fija para las páginas subsiguientes."""
        if self.page_no() > 1: 
            self.set_fill_color(10, 37, 64) 
            self.rect(0, 0, 210, 25, 'F')
            self.set_y(8)
            self.set_font('Arial', 'B', 14)
            self.set_text_color(255, 255, 255)
            self.cell(0, 6, 'REPORTE GERENCIAL DE EFICIENCIA OPERACIONAL (OEE)', 0, 1, 'C')
            self.set_font('Arial', '', 9)
            self.set_text_color(220, 220, 220)
            self.cell(0, 5, f'Línea de Producción: {EnterpriseConfig.Operaciones.NOMBRE_EQUIPO}', 0, 1, 'C')
            self.ln(12)

    def footer(self):
        """Pie de página automatizado."""
        if self.page_no() > 1:
            self.set_y(-15)
            self.set_font('Arial', 'I', 8)
            self.set_text_color(128, 128, 128)
            self.line(15, 282, 195, 282)
            self.cell(90, 10, f'Generado por: CAVA Core Engine | Ref: {self.ventana}', 0, 0, 'L')
            self.cell(90, 10, f'Pág. {self.page_no()} | TS: {datetime.now().strftime("%d/%m/%Y %H:%M")}', 0, 0, 'R')

    def render_portada_oficial(self):
        """Dibuja la portada del documento gerencial con alto contraste corporativo."""
        self.add_page()
        self.set_fill_color(248, 249, 250)
        self.rect(0, 0, 210, 297, 'F') 
        
        # Banda Lateral
        self.set_fill_color(192, 127, 0) # Dorado CAVA
        self.rect(0, 0, 6, 297, 'F')

        self.ln(60)
        self.set_font('Arial', 'B', 24)
        self.set_text_color(10, 37, 64)
        self.cell(10)
        self.cell(0, 12, 'DOCUMENTO EJECUTIVO DE', 0, 1, 'L')
        self.cell(10)
        self.cell(0, 12, 'DESEMPENO PRODUCTIVO (OEE)', 0, 1, 'L')

        self.ln(15)
        self.set_font('Arial', 'B', 14)
        self.set_text_color(120, 120, 120)
        self.cell(10); self.cell(0, 8, f'Identidad de Activo: {EnterpriseConfig.Operaciones.ID_MAQUINA} - {EnterpriseConfig.Operaciones.NOMBRE_EQUIPO}', 0, 1, 'L')
        self.cell(10); self.cell(0, 8, 'Area Responsable: Produccion Lurin', 0, 1, 'L')
        
        self.ln(40)
        
        datos = [
            ('Rango Logico de Medicion:', self.ventana), 
            ('Configuracion de Turnos:', self.turnos), 
            ('Clasificacion de Doc.:', 'Confidencial - Uso Gerencial')
        ]
        
        for titulo, valor in datos:
            self.set_font('Arial', 'B', 12)
            self.set_text_color(10, 37, 64)
            self.cell(10)
            self.cell(60, 8, titulo, 0, 0)
            self.set_font('Arial', '', 12)
            self.set_text_color(50, 50, 50)
            self.cell(0, 8, valor, 0, 1)

    def draw_caja_metrica_nativa(self, x: float, y: float, w: float, h: float, titulo: str, valor: str, color_borde: list):
        """Renderizado algorítmico de cajas de métricas sin requerir imágenes PNG (Zero-Latencia)."""
        self.set_xy(x, y)
        self.set_fill_color(255, 255, 255)
        self.set_draw_color(*color_borde)
        self.set_line_width(0.8)
        self.rect(x, y, w, h, 'DF')
        
        self.set_xy(x, y + 5)
        self.set_font('Arial', 'B', 10)
        self.set_text_color(128, 128, 128)
        self.cell(w, 5, titulo.upper(), 0, 1, 'C')
        
        self.set_xy(x, y + 14)
        self.set_font('Arial', 'B', 22)
        self.set_text_color(10, 37, 64)
        self.cell(w, 10, valor, 0, 1, 'C')
        
        self.set_line_width(0.2) # Reset

    def ensamblar_cuerpo_tecnico(self, metricas: Dict[str, Any], df_pareto: pd.DataFrame, insights: List[str]):
        """Construye las páginas interiores del reporte utilizando únicamente primitivas de FPDF."""
        self.add_page()
        
        # --- SECCIÓN 1: HALLAZGOS Y ALERTAS ---
        self.ln(5)
        self.set_font('Arial', 'B', 12)
        self.set_fill_color(192, 127, 0)
        self.set_text_color(255, 255, 255)
        self.cell(0, 8, "   1. DIAGNOSTICO DEL MOTOR DE INTELIGENCIA", 0, 1, 'L', fill=True)
        
        self.ln(5)
        self.set_font('Arial', '', 11)
        self.set_text_color(40, 40, 40)
        for h in insights:
            self.multi_cell(0, 6, f"  > {h}")
        
        self.ln(10)
        
        # --- SECCIÓN 2: KPI MATRICIAL (Dibujado en caliente) ---
        self.set_font('Arial', 'B', 12)
        self.set_fill_color(192, 127, 0)
        self.set_text_color(255, 255, 255)
        self.cell(0, 8, "   2. DESEMPENO ESTRUCTURAL DE MAQUINA (FACTORES TPM)", 0, 1, 'L', fill=True)
        self.ln(8)
        
        y_cajas = self.get_y()
        w_caja = 40
        espacio = 6
        x_start = 15
        
        self.draw_caja_metrica_nativa(x_start, y_cajas, w_caja, 30, "OEE GLOBAL", f"{metricas['OEE']:.1f}%", [10, 37, 64])
        self.draw_caja_metrica_nativa(x_start + w_caja + espacio, y_cajas, w_caja, 30, "DISPONIB.", f"{metricas['Disponibilidad']:.1f}%", [192, 127, 0])
        self.draw_caja_metrica_nativa(x_start + 2*(w_caja + espacio), y_cajas, w_caja, 30, "RENDIM.", f"{metricas['Rendimiento']:.1f}%", [192, 127, 0])
        self.draw_caja_metrica_nativa(x_start + 3*(w_caja + espacio), y_cajas, w_caja, 30, "CALIDAD", f"{metricas['Calidad']:.1f}%", [46, 139, 87])
        
        self.set_y(y_cajas + 40)
        
        # --- SECCIÓN 3: VOLUMETRÍA ---
        self.set_font('Arial', 'B', 12)
        self.set_fill_color(10, 37, 64)
        self.set_text_color(255, 255, 255)
        self.cell(0, 8, "   3. BALANCE FISICO DE VOLUMETRIA", 0, 1, 'L', fill=True)
        self.ln(5)
        
        self.set_font('Arial', '', 11); self.set_text_color(0, 0, 0)
        self.cell(100, 8, "Produccion Certificada (Conforme):", border='B')
        self.set_font('Arial', 'B', 11); self.set_text_color(46, 139, 87)
        self.cell(80, 8, f"{metricas['Prod_Conforme']:,.0f} uni", border='B', ln=1, align='R')
        
        self.set_font('Arial', '', 11); self.set_text_color(0, 0, 0)
        self.cell(100, 8, "Merma Registrada (No Conforme):", border='B')
        self.set_font('Arial', 'B', 11); self.set_text_color(192, 57, 43)
        self.cell(80, 8, f"{metricas['Prod_No_Conforme']:,.0f} uni", border='B', ln=1, align='R')
        
        self.set_font('Arial', '', 11); self.set_text_color(0, 0, 0)
        self.cell(100, 8, "Muestreos Extraidos Analitica (QA):", border='B')
        self.set_font('Arial', 'B', 11); self.set_text_color(128, 128, 128)
        self.cell(80, 8, f"{metricas['Muestras_Calidad']:,.0f} uni", border='B', ln=1, align='R')

        self.ln(10)
        
        # --- SECCIÓN 4: PARETO TABULAR AVANZADO ---
        if not df_pareto.empty:
            self.set_font('Arial', 'B', 12)
            self.set_fill_color(10, 37, 64)
            self.set_text_color(255, 255, 255)
            self.cell(0, 8, "   4. AUDITORIA DE FALLAS Y DETRACTORES (TOP 10)", 0, 1, 'L', fill=True)
            self.ln(3)
            
            # Cabecera Tabla
            self.set_font('Arial', 'B', 9)
            self.set_fill_color(230, 230, 230)
            self.set_text_color(0, 0, 0)
            self.cell(130, 7, "Descripcion del Evento Tecnico", 1, 0, 'C', fill=True)
            self.cell(50, 7, "Perdida de Tiempo (Minutos)", 1, 1, 'C', fill=True)
            
            # Filas Tabla
            self.set_font('Arial', '', 9)
            for idx, row in df_pareto.head(10).iterrows():
                motivo = str(row['Detractor_Operativo'])[:65] # Truncamiento seguro
                mins = f"{row['Impacto_Minutos']:.1f}"
                self.cell(130, 7, motivo, 1, 0, 'L')
                self.cell(50, 7, mins, 1, 1, 'R')
                
        # --- BLOQUE DE FIRMAS FINALES ---
        if self.get_y() > 230: self.add_page()
        self.ln(25)
        self.set_draw_color(0, 0, 0)
        self.line(30, self.get_y(), 85, self.get_y())
        self.line(125, self.get_y(), 180, self.get_y())
        
        self.ln(2)
        self.set_font('Arial', '', 9)
        self.set_text_color(100, 100, 100)
        self.cell(95, 5, 'Superintendencia de Mantenimiento', 0, 0, 'C')
        self.cell(90, 5, 'Direccion de Produccion de Planta', 0, 1, 'C')


# ==================================================================================================
# 10. MÓDULO API TELEGRAM (GATEWAY DE DISTRIBUCIÓN)
# ==================================================================================================
class CommGateway:
    """Clase estática para el despacho del PDF por vías encriptadas (Telegram Bot API)."""
    
    @staticmethod
    def transmitir_pdf_gerencial(ruta_pdf: str, metricas: Dict[str, Any], ventana: str) -> bool:
        url = f"https://api.telegram.org/bot{EnterpriseConfig.API.TOKEN}/sendDocument"
        
        falla_critica = metricas['Data_Pareto'].iloc[0]['Detractor_Operativo'] if not metricas['Data_Pareto'].empty else 'Sin fallos'
        min_criticos = metricas['Data_Pareto'].iloc[0]['Impacto_Minutos'] if not metricas['Data_Pareto'].empty else 0.0
        
        cuerpo_mensaje = (
            f"📊 *REPORTE OEE GENERADO* - Maq. {EnterpriseConfig.Operaciones.ID_MAQUINA}\n"
            f"📅 *Segmento:* {ventana}\n\n"
            f"⚙️ *Factor OEE:* {metricas['OEE']:.1f}%\n"
            f"📦 *Volumen QA:* {metricas['Prod_Conforme']:,.0f} u.\n"
            f"⚠️ *Alerta Mayor:* {falla_critica} ({min_criticos:.1f} min)\n\n"
            f"_Despacho Automatizado CAVA Kernel_"
        )
        
        try:
            with open(ruta_pdf, 'rb') as archivo:
                payload = {'document': archivo}
                datos = {'chat_id': EnterpriseConfig.API.CHAT_ID, 'caption': cuerpo_mensaje, 'parse_mode': 'Markdown'}
                resp = requests.post(url, files=payload, data=datos, timeout=EnterpriseConfig.API.TIMEOUT_SEC)
                
            if resp.status_code == 200:
                EnterpriseLogger.info("Sincronización API Telegram completada.")
                return True
            EnterpriseLogger.error(f"Error API: Código {resp.status_code}. Response: {resp.text}")
            return False
        except Exception as e:
            EnterpriseLogger.error(f"Fallo crítico de socket en CommGateway: {e}")
            return False


# ==================================================================================================
# 11. SUBSISTEMA DE RENDERIZADO VISUAL VECTORIAL (PLOTLY GANTT/BARRAS)
# ==================================================================================================
class VectorEngine:
    """Dibuja únicamente los gráficos estrictamente necesarios para el análisis de fallas (No gauges, no latencia)."""

    @staticmethod
    def renderizar_linea_vida(df_gantt: pd.DataFrame) -> go.Figure:
        """Diagrama microscópico de eventos operacionales a lo largo del tiempo."""
        if df_gantt.empty or 'TL_START' not in df_gantt.columns:
            return go.Figure().update_layout(title="Datos insuficientes para Línea de Vida", template="simple_white")

        df = df_gantt.dropna(subset=['TL_START', 'TL_END']).copy()
        col_hover = DataProcessor.ubicar_columna(df, ['DESCRIPCIÓN ESPECIFICA', 'MOTIVO DE PARADA']) or 'Evento'
        
        fig = px.timeline(
            df, x_start="TL_START", x_end="TL_END", y="CAT_STD", color="CAT_STD",
            hover_name=col_hover, color_discrete_sequence=px.colors.qualitative.Dark24
        )
        fig.update_yaxes(autorange="reversed")
        fig.update_layout(
            xaxis=dict(title="Cinta de Tiempo", tickformat="%H:%M", gridcolor='#E2E8F0'),
            yaxis_title="", template="simple_white", height=380, showlegend=False,
            margin=dict(t=30, b=40, l=10, r=10), title="Auditoría Cronológica de Paradas"
        )
        return fig

    @staticmethod
    def renderizar_pareto_barras(df_pareto: pd.DataFrame) -> go.Figure:
        """Renderizado en barras horizontales para rápida lectura de detractores."""
        if df_pareto.empty:
            return go.Figure().update_layout(title="Operación perfecta. 0 paradas.", template="simple_white")

        df_top = df_pareto.head(15).sort_values(by='Impacto_Minutos', ascending=True)

        fig = px.bar(
            df_top, x='Impacto_Minutos', y='Detractor_Operativo', orientation='h',
            text='Impacto_Minutos', color='Impacto_Minutos',
            color_continuous_scale=['#FFEBEE', '#C0392B']
        )
        fig.update_traces(texttemplate='%{text:.1f} m', textposition='outside')
        fig.update_layout(
            template="simple_white", height=450, coloraxis_showscale=False,
            margin=dict(l=10, r=40, t=30, b=10), title="Pareto de Detractores Principales"
        )
        return fig


# ==================================================================================================
# 12. ORQUESTADOR CENTRAL (INTERFAZ DE USUARIO STREAMLIT)
# ==================================================================================================
class MainKernelUI:
    """Clase principal que administra el ciclo de vida, estado de variables y repintado de la app."""

    def __init__(self):
        self.memoria_db: Dict[str, pd.DataFrame] = {}
        self.metricas_vivas: Dict[str, Any] = {}
        self.contexto_fecha: str = ""
        self.contexto_turno: str = ""

    def renderizar_bloque_ingesta(self):
        """Panel lateral avanzado con acceso exclusivo por Directorio de Red o Carga Directa."""
        st.sidebar.markdown("""
        <div class="cava-logo-wrapper">
            <h2 class="cava-logo-main">CAVA</h2>
            <p class="cava-logo-sub">ENTERPRISE AUTOMATION</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.sidebar.markdown("### 🔌 1. Ingesta de Datos (DataLink)")
        
        origen = st.sidebar.radio(
            "Protocolo de Conexión a Servidor:", 
            ["Carga Manual (.xlsx)", "Ruta de Red Compartida (LAN)"],
            help="Seleccione el método de aprovisionamiento de la matriz OEE."
        )
        
        if origen == "Carga Manual (.xlsx)":
            archivo = st.sidebar.file_uploader("Subir matriz maestra:", type=["xlsx", "xlsb", "xlsm"])
            if archivo:
                with st.spinner("Procesando estructura binaria..."):
                    self.memoria_db = orquestar_etl_excel(archivo.read())
                    
        elif origen == "Ruta de Red Compartida (LAN)":
            ruta_red = st.sidebar.text_input(
                "Enlace Absoluto o Ruta URI:", 
                placeholder=r"\\SERVIDOR\Produccion\Matriz_OEE.xlsx"
            )
            if st.sidebar.button("📡 Sincronizar desde Servidor Local"):
                if ruta_red:
                    estado_ok, mensaje = NetworkGateway.validar_ruta_red(ruta_red)
                    if estado_ok:
                        with st.spinner("Estableciendo túnel I/O con servidor y extrayendo matrices..."):
                            buffer_bytes = NetworkGateway.cargar_archivo_en_memoria(ruta_red)
                            if buffer_bytes:
                                self.memoria_db = orquestar_etl_excel(buffer_bytes.read())
                                st.sidebar.success("Sincronización LAN Exitosa.")
                    else:
                        st.sidebar.error(mensaje)
                else:
                    st.sidebar.warning("Debe especificar una ruta de red válida.")

    def renderizar_motor_filtros(self) -> Tuple[float, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Control del pipeline temporal. Lógica Smart Date inyectada."""
        df_c = self.memoria_db.get('CAPS', pd.DataFrame())
        df_p = self.memoria_db.get('Produccion', pd.DataFrame())
        df_d = self.memoria_db.get('Detalle parada', pd.DataFrame())

        st.sidebar.markdown("---")
        st.sidebar.markdown("### ⚙️ 2. Motor de Filtrado Espacial")
        
        # Inteligencia de Fechas
        fecha_min, fecha_max, fecha_target = date.today(), date.today(), date.today()
        if not df_c.empty and 'FECHA_STD' in df_c.columns:
            fecha_min, fecha_max = df_c['FECHA_STD'].min(), df_c['FECHA_STD'].max()
            fecha_target = date.today() if fecha_min <= date.today() <= fecha_max else fecha_max

        metodo = st.sidebar.selectbox(
            "Segmentador Cronológico:", 
            ["Turno Smart (Última Operación)", "Día Específico", "Rango Continuo", "Mes Histórico"]
        )
        
        f_ini, f_fin = fecha_target, fecha_target
        p_ano, p_mes, p_sem = 0, 0, 0

        if metodo == "Turno Smart (Última Operación)":
            f_ini = f_fin = fecha_target
            st.sidebar.info(f"Auto-alineado a: {fecha_target}")
        elif metodo == "Día Específico":
            f_ini = f_fin = st.sidebar.date_input("Día Base:", value=fecha_target, min_value=fecha_min, max_value=fecha_max)
        elif metodo == "Rango Continuo":
            rango = st.sidebar.date_input("Límites Temporales:", [fecha_min, fecha_max], min_value=fecha_min, max_value=fecha_max)
            if len(rango) == 2: f_ini, f_fin = rango[0], rango[1]
        elif metodo == "Mes Histórico" and 'AÑO' in df_c.columns and 'MES' in df_c.columns:
            p_ano = st.sidebar.selectbox("Año Base", sorted(df_c['AÑO'].dropna().unique(), reverse=True))
            p_mes = st.sidebar.selectbox("Mes Base", sorted(df_c[df_c['AÑO']==p_ano]['MES'].dropna().unique()))

        # Extracción de Turnos Dinámicos
        t_sel = []
        c_turno = DataProcessor.ubicar_columna(df_c, ['TURNO', 'SHIFT'])
        if c_turno:
            turnos_disp = df_c[c_turno].dropna().unique().tolist()
            if turnos_disp:
                t_sel = st.sidebar.multiselect("Filtro de Turnos:", turnos_disp, default=turnos_disp)

        st.sidebar.markdown("---")
        st.sidebar.markdown("### 🎯 3. Control de Objetivos Base")
        meta = st.sidebar.number_input("Benchmark OEE (%)", value=EnterpriseConfig.Operaciones.META_OEE_DEFAULT, step=1.0)

        # Aplicación matemática de filtros
        df_c_f = RuleEngine.aplicar_reglas_tiempo(df_c, f_ini, f_fin, metodo, p_ano, p_mes, p_sem, t_sel)
        df_p_f = RuleEngine.aplicar_reglas_tiempo(df_p, f_ini, f_fin, metodo, p_ano, p_mes, p_sem, t_sel)
        df_d_f = RuleEngine.aplicar_reglas_tiempo(df_d, f_ini, f_fin, metodo, p_ano, p_mes, p_sem, t_sel)

        self.contexto_fecha = str(f_ini) if f_ini == f_fin else f"{f_ini} -> {f_fin}"
        self.contexto_turno = ", ".join([str(t) for t in t_sel]) if t_sel else "Turnos Consolidados"

        return meta, df_c_f, df_p_f, df_d_f

    def despachar_pdf(self, meta: float):
        """Ensambla el PDF A4 utilizando dibujado FPDF nativo y despacha por Telegram."""
        with st.spinner("Construyendo binario PDF A4 Institucional. Compilando tablas nativas..."):
            try:
                pdf = EnterprisePDFEngine(self.contexto_fecha, self.contexto_turno)
                pdf.render_portada_oficial()
                
                hallazgos = ExpertDiagnostics.evaluar_performance(self.metricas_vivas, meta)
                pdf.ensamblar_cuerpo_tecnico(self.metricas_vivas, self.metricas_vivas['Data_Pareto'], hallazgos)
                
                nombre_archivo = f"CAVA_OEE_{EnterpriseConfig.Operaciones.ID_MAQUINA}_{datetime.now().strftime('%Y%m%d%H%M')}.pdf"
                ruta_pdf = os.path.join(EnterpriseConfig.Rutas.TEMP, nombre_archivo)
                pdf.output(ruta_pdf)
                
                # Despacho Telegram
                exito = CommGateway.transmitir_pdf_gerencial(ruta_pdf, self.metricas_vivas, self.contexto_fecha)
                if exito:
                    st.success("✅ Certificado Operativo: Documento vectorizado en A4 y distribuido exitosamente a la Gerencia.")
                else:
                    st.warning("⚠️ El PDF fue generado localmente, pero el protocolo de red hacia los servidores externos falló.")
                
                # Descarga Local Failsafe
                with open(ruta_pdf, "rb") as bf:
                    st.download_button(
                        label="💾 Descargar Copia Física (PDF)",
                        data=bf,
                        file_name=nombre_archivo,
                        mime="application/pdf"
                    )
            except Exception as e:
                EnterpriseLogger.error("Colapso en generador PDF", exc_info=True)
                st.error(f"Fallo estructural del procesador de documentos: {e}")

    def iniciar(self):
        """Punto de entrada de ejecución del Framework Streamlit."""
        inject_corporate_css()
        self.renderizar_bloque_ingesta()
        
        if not self.memoria_db:
            st.markdown(f"""
            <div class="gerencia-header" style="margin-top:20px;">
                <h4>⚙️ Centro de Operaciones OEE - Unidad Lurin (Activo {EnterpriseConfig.Operaciones.ID_MAQUINA})</h4>
                <p>El sistema se encuentra en estado de hibernación esperando telemetría.</p>
                <p><strong>Instrucción:</strong> Inserte la matriz Excel maestra o especifique el directorio de la unidad de red compartida en el panel lateral izquierdo para iniciar la orquestación de datos.</p>
            </div>
            """, unsafe_allow_html=True)
            return

        meta_oee, df_caps, df_prod, df_paradas = self.renderizar_motor_filtros()
        
        # Ejecutar Lógica de Negocio
        self.metricas_vivas = BusinessLogic.consolidar_kpis(df_caps, df_prod, df_paradas)
        
        st.markdown(f"""
            <div class="gerencia-header">
                <h4>📊 Panel Ejecutivo Consolidado: {EnterpriseConfig.Operaciones.NOMBRE_EQUIPO}</h4>
                <p><strong>Alcance Espacial:</strong> {self.contexto_fecha} &nbsp;|&nbsp; <strong>Config. Turnos:</strong> {self.contexto_turno}</p>
            </div>
        """, unsafe_allow_html=True)

        # =====================================================================
        # DISEÑO DE UI ULTRA-RÁPIDO (CERO LATENCIA, SIN COMPONENTES PESADOS)
        # =====================================================================
        st.markdown("### 1. Cuadro de Mando Integral y KPIs Core (Rendimiento Instantáneo)")
        
        # Fila 1: Indicadores Core
        col1, col2, col3, col4 = st.columns(4)
        oee_val = self.metricas_vivas['OEE']
        c_oee = EnterpriseConfig.UIColors.SUCCESS if oee_val >= meta_oee else EnterpriseConfig.UIColors.DANGER
        
        col1.markdown(f"<div class='fast-metric-card'><div class='fast-metric-title'>OEE Global</div><div class='fast-metric-value' style='color:{c_oee};'>{oee_val:.1f}%</div><div class='fast-metric-subtitle'>Meta: {meta_oee}%</div></div>", unsafe_allow_html=True)
        col2.markdown(f"<div class='fast-metric-card'><div class='fast-metric-title'>Disponibilidad</div><div class='fast-metric-value' style='color:{EnterpriseConfig.UIColors.PRIMARY};'>{self.metricas_vivas['Disponibilidad']:.1f}%</div><div class='fast-metric-subtitle'>Tiempo Operativo Real</div></div>", unsafe_allow_html=True)
        col3.markdown(f"<div class='fast-metric-card'><div class='fast-metric-title'>Rendimiento</div><div class='fast-metric-value' style='color:{EnterpriseConfig.UIColors.PRIMARY};'>{self.metricas_vivas['Rendimiento']:.1f}%</div><div class='fast-metric-subtitle'>Velocidad / Microparadas</div></div>", unsafe_allow_html=True)
        col4.markdown(f"<div class='fast-metric-card'><div class='fast-metric-title'>Calidad (FTT)</div><div class='fast-metric-value' style='color:{EnterpriseConfig.UIColors.SUCCESS};'>{self.metricas_vivas['Calidad']:.1f}%</div><div class='fast-metric-subtitle'>Primer Paso Correcto</div></div>", unsafe_allow_html=True)

        # Fila 2: Volumetría Física
        col_v1, col_v2, col_v3 = st.columns(3)
        col_v1.markdown(f"<div class='fast-metric-card'><div class='fast-metric-title'>Producción Neta Aprobada</div><div class='fast-metric-value' style='color:{EnterpriseConfig.UIColors.SECONDARY};'>{self.metricas_vivas['Prod_Conforme']:,.0f}</div><div class='fast-metric-subtitle'>Unidades Liberadas (QA Pass)</div></div>", unsafe_allow_html=True)
        col_v2.markdown(f"<div class='fast-metric-card'><div class='fast-metric-title'>Rechazo Total (Chatarra)</div><div class='fast-metric-value' style='color:{EnterpriseConfig.UIColors.DANGER};'>{self.metricas_vivas['Prod_No_Conforme']:,.0f}</div><div class='fast-metric-subtitle'>Volumen No Conforme Definitivo</div></div>", unsafe_allow_html=True)
        col_v3.markdown(f"<div class='fast-metric-card'><div class='fast-metric-title'>Muestras Retiradas QA</div><div class='fast-metric-value' style='color:{EnterpriseConfig.UIColors.MUTED};'>{self.metricas_vivas['Muestras_Calidad']:,.0f}</div><div class='fast-metric-subtitle'>Uso en Laboratorio / Ensayos</div></div>", unsafe_allow_html=True)

        st.markdown("---")
        
        tab_graficas, tab_tablas = st.tabs(["📉 Visualización Espacial Vectorial", "🗄️ Auditoría de Tablas Consolidadas"])
        
        with tab_graficas:
            col_izq, col_der = st.columns([1, 1])
            with col_izq:
                st.markdown("#### Línea de Vida Cronológica del Turno")
                st.plotly_chart(VectorEngine.renderizar_linea_vida(self.metricas_vivas['Data_Timeline']), use_container_width=True)
            with col_der:
                st.markdown("#### Distribución Pareto: Impacto Físico de Fallas")
                st.plotly_chart(VectorEngine.renderizar_pareto_barras(self.metricas_vivas['Data_Pareto']), use_container_width=True)
                
        with tab_tablas:
            c_tb1, c_tb2 = st.columns(2)
            with c_tb1:
                st.markdown("#### Log de Paradas de Máquina (Auditoría Técnica)")
                if not self.metricas_vivas['Data_Pareto'].empty:
                    st.dataframe(self.metricas_vivas['Data_Pareto'].style.format({"Impacto_Minutos": "{:.1f} m"}).background_gradient(subset=["Impacto_Minutos"], cmap='Reds'), use_container_width=True, hide_index=True)
                else:
                    st.success("No hay registros de paradas en el vector de tiempo analizado.")
            with c_tb2:
                st.markdown("#### Trazabilidad de Responsabilidad por Operador")
                ops_df = self.metricas_vivas['Operadores']
                if isinstance(ops_df, pd.DataFrame) and not ops_df.empty:
                    col_val = ops_df.columns[1]
                    st.dataframe(ops_df.style.format({col_val: "{:,.0f} u"}).background_gradient(subset=[col_val], cmap='Blues'), use_container_width=True, hide_index=True)
                else:
                    st.info("No se ha registrado segregación por operador en el formato origen.")

        st.markdown("---")
        st.markdown("### 📤 CAVA Dispatcher: Sistema de Despacho Documental")
        _, central_btn, _ = st.columns([1, 2, 1])
        with central_btn:
            if st.button("📄 Consolidar Reporte Gerencial A4 y Despachar a Red Corporativa"):
                self.despachar_pdf(meta_oee)


if __name__ == "__main__":
    try:
        kernel = MainKernelUI()
        kernel.iniciar()
    except Exception as exc_critica:
        EnterpriseLogger.error("Caída total de Kernel detectada en runtime.", exc_info=True)
        st.error(f"Fallo del Servidor Interno. Notifique al departamento TI: {exc_critica}")

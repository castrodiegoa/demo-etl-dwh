import pandas as pd
from sqlalchemy import create_engine
from config.config import ORACLE_CONFIG


def get_oracle_engine():
    user = ORACLE_CONFIG["user"]
    password = ORACLE_CONFIG["password"]
    dsn = ORACLE_CONFIG["dsn"]
    connection_url = f"oracle+oracledb://{user}:{password}@{dsn}"
    engine = create_engine(connection_url)
    return engine


def extract_enc_vent():
    engine = get_oracle_engine()
    query = """
    SELECT 
        BOD_CODI        AS COD_BODEGA,
        TKT_NMRO        AS NUMERO_TICKET,
        FEC_OPER        AS FECHA_OPERACION,
        COD_CLTE        AS COD_CLIENTE
    FROM DEMO_DWH.ENC_VENT
    """
    df = pd.read_sql(query, con=engine)
    return df


def extract_det_vent():
    engine = get_oracle_engine()
    query = """
    SELECT 
        TKT_NMRO        AS NUMERO_TICKET,
        COD_BARR        AS COD_PRODUCTO,
        CAN_ARTI        AS CANTIDAD,
        VAL_UNIT        AS VALOR_UNITARIO,
        VAL_DCTO        AS VALOR_DESCUENTO,
        VAL_VENT        AS VALOR_VENTA,
        IVA_PORC        AS IVA_PORC
    FROM DEMO_DWH.DET_VENT
    """
    df = pd.read_sql(query, con=engine)
    return df


def extract_art_vent():
    engine = get_oracle_engine()
    # Consulta con INNER JOIN entre ART_VENT y MAE_ARTI para obtener solo los productos vendidos
    query = """
    SELECT 
        A.ART_CODI        AS COD_PRODUCTO,
        A.ART_DESC        AS DESCRIPCION_PRODUCTO,
        A.TIP_ARTI        AS TIPO_PRODUCTO,
        A.EST_ARTI        AS ESTADO_PRODUCTO
    FROM DEMO_DWH.MAE_ARTI A
    INNER JOIN DEMO_DWH.ART_VENT V
        ON A.ART_CODI = V.ART_CODI
    """
    df = pd.read_sql(query, con=engine)
    return df


def extract_pos_clte():
    engine = get_oracle_engine()
    query = """
    SELECT
        COD_CLTE                    AS COD_CLIENTE,
        NOM_CLTE                    AS NOMBRE_CLIENTE,
        APE_CLTE                    AS APELLIDO_CLIENTE,
        DIR_CLTE                    AS DIRECCION_CLIENTE,
        TEL_CLTE                    AS TELEFONO_CLIENTE,
        FEC_NACI                    AS FECHA_NACIMIENTO_CLIENTE,
        CLT_MAIL                    AS EMAIL_CLIENTE,
        SEX_CLTE                    AS SEXO_CLIENTE
    FROM DEMO_DWH.POS_CLTE
    """
    df = pd.read_sql(query, con=engine)
    return df


def extract_mae_bode():
    engine = get_oracle_engine()
    query = """
    SELECT
        BOD_CODI        AS COD_BODEGA,
        BOD_DESC        AS DESCRIPCION_BODEGA,
        DIR_BODE        AS DIRECCION_BODEGA
    FROM DEMO_DWH.MAE_BODE
    """
    df = pd.read_sql(query, con=engine)
    return df

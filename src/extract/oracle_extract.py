import pandas as pd
from sqlalchemy import create_engine
from src.config.config import ORACLE_CONFIG


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
        BOD_CODI        AS codigo_bodega,
        TKT_NMRO        AS numero_ticket,
        CAJ_CODI        AS codigo_caja,
        EVE_CODI        AS codigo_evento,
        FEC_OPER        AS fecha_operacion,
        COD_CLTE        AS codigo_cliente
    FROM DEMO_DWH.ENC_VENT
    """
    df = pd.read_sql(query, con=engine)
    return df


def extract_det_vent_in_chunks(chunksize=10000):
    engine = get_oracle_engine()
    query = """
    SELECT 
        D.BOD_CODI      AS codigo_bodega,
        D.CAJ_CODI      AS codigo_caja,
        D.EVE_CODI      AS codigo_evento,
        D.TKT_NMRO      AS numero_ticket,
        D.TKT_CONS      AS numero_consecutivo,
        D.COD_BARR      AS codigo_barras,
        D.CAN_ARTI      AS cantidad,
        D.VAL_UNIT      AS valor_unitario,
        D.VAL_DCTO      AS valor_descuento,
        D.VAL_VENT      AS valor_venta,
        D.IVA_PORC      AS iva_porcentaje,
        V.ART_CODI      AS codigo_producto
    FROM DEMO_DWH.DET_VENT D
    INNER JOIN DEMO_DWH.ART_VENT V
        ON D.COD_BARR = V.COD_BARR
    """
    df = pd.read_sql(query, con=engine, chunksize=chunksize)
    return df


def extract_art_vent():
    engine = get_oracle_engine()
    query = """
    SELECT 
        V.ART_CODI        AS codigo_producto,
        V.ART_DESC        AS descripcion_producto,
        V.ART_TIPO        AS tipo_producto,
        V.EST_ARTI        AS estado_producto,
        A.ART_REFE        AS referencia_producto,
        A.VAL_CURV        AS valor_curva
    FROM DEMO_DWH.ART_VENT V
    LEFT JOIN DEMO_DWH.MAE_ARTI A
        ON V.ART_CODI = A.ART_CODI
    """
    df = pd.read_sql(query, con=engine)
    return df


def extract_pos_clte():
    engine = get_oracle_engine()
    query = """
    SELECT
        COD_CLTE        AS codigo_cliente,
        NOM_CLTE        AS nombre_cliente,
        APE_CLTE        AS apellido_cliente,
        DIR_CLTE        AS direccion_cliente,
        TEL_CLTE        AS telefono_cliente,
        FEC_NACI        AS fecha_nacimiento_cliente,
        CLT_MAIL        AS email_cliente,
        SEX_CLTE        AS sexo_cliente
    FROM DEMO_DWH.POS_CLTE
    """
    df = pd.read_sql(query, con=engine)
    return df


def extract_mae_bode():
    engine = get_oracle_engine()
    query = """
    SELECT
        BOD_CODI        AS codigo_bodega,
        BOD_DESC        AS descripcion_bodega,
        DIR_BODE        AS direccion_bodega
    FROM DEMO_DWH.MAE_BODE
    """
    df = pd.read_sql(query, con=engine)
    return df

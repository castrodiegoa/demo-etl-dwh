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


def extract_fact_ventas_base():
    engine = get_oracle_engine()
    query = """
    SELECT 
        -- Claves de unión
        D.BOD_CODI      AS codigo_bodega,
        D.CAJ_CODI      AS codigo_caja,
        D.EVE_CODI      AS codigo_evento,
        D.TKT_NMRO      AS numero_ticket,
        D.TKT_CONS      AS numero_consecutivo,

        -- Datos del detalle
        D.COD_BARR      AS codigo_barras,
        D.CAN_ARTI      AS cantidad,
        D.VAL_UNIT      AS valor_unitario,
        D.VAL_DCTO      AS valor_descuento,
        D.VAL_VENT      AS valor_venta,
        D.IVA_PORC      AS iva_porcentaje,

        -- Datos del producto
        V.ART_CODI      AS codigo_producto,

        -- Datos del encabezado
        E.FEC_OPER      AS fecha_operacion,
        E.COD_CLTE      AS codigo_cliente

    FROM DEMO_DWH.DET_VENT D
    INNER JOIN DEMO_DWH.ART_VENT V
        ON D.BOD_CODI = V.BOD_CODI AND D.COD_BARR = V.COD_BARR
    INNER JOIN DEMO_DWH.ENC_VENT E
        ON D.BOD_CODI = E.BOD_CODI
        AND D.CAJ_CODI = E.CAJ_CODI
        AND D.EVE_CODI = E.EVE_CODI
        AND D.TKT_NMRO = E.TKT_NMRO
    WHERE (UPPER(TKT_OBSE) = 'OK' OR TKT_OBSE IS NULL)
    """
    fact_ventas_base_df = pd.read_sql(query, con=engine)
    return fact_ventas_base_df


def extract_art_vent():
    engine = get_oracle_engine()
    query = """
    SELECT 
        ART_CODI      AS codigo_producto,
        ART_DESC      AS descripcion_producto
    FROM DEMO_DWH.ART_VENT
    """
    art_vent_df = pd.read_sql(query, con=engine)
    return art_vent_df


def extract_pos_clte():
    engine = get_oracle_engine()
    query = """
    SELECT
        COD_CLTE        AS codigo_cliente,
        NOM_CLTE        AS nombre_cliente,
        APE_CLTE        AS apellido_cliente,
        SEX_CLTE        AS sexo_cliente
    FROM DEMO_DWH.POS_CLTE
    """
    pos_clte_df = pd.read_sql(query, con=engine)
    return pos_clte_df


def extract_mae_bode():
    engine = get_oracle_engine()
    query = """
    SELECT
        BOD_CODI        AS codigo_bodega,
        BOD_DESC        AS descripcion_bodega,
        DIR_BODE        AS direccion_bodega
    FROM DEMO_DWH.MAE_BODE
    """
    mae_bode_df = pd.read_sql(query, con=engine)
    return mae_bode_df

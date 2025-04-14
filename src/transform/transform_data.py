import pandas as pd


# ----------------------------------------------------------------
#  DIM TIEMPO
# ----------------------------------------------------------------
def build_dim_tiempo(enc_vent_df):
    """
    Crea la dimensión de tiempo a partir de las fechas encontradas en ENC_VENT.
    Asumimos que FEC_OPER es la fecha de operación.
    """
    # 1. Extraer las fechas únicas
    fechas = enc_vent_df["fecha_operacion"].dropna().unique()
    fechas = pd.to_datetime(fechas)  # Asegurarnos de convertir a datetime

    # 2. Construir un DataFrame base con esas fechas
    dim_tiempo = pd.DataFrame({"fecha_completa": fechas})

    # 3. Crear atributos de fecha
    dim_tiempo["dia"] = dim_tiempo["fecha_completa"].dt.day
    dim_tiempo["mes"] = dim_tiempo["fecha_completa"].dt.month
    dim_tiempo["anio"] = dim_tiempo["fecha_completa"].dt.year
    dim_tiempo["trimestre"] = dim_tiempo["fecha_completa"].dt.quarter

    # Nombre del día y nombre del mes (opcional)
    dim_tiempo["nombre_dia"] = dim_tiempo["fecha_completa"].dt.day_name(locale="es_ES")
    dim_tiempo["nombre_mes"] = dim_tiempo["fecha_completa"].dt.month_name(
        locale="es_ES"
    )

    # Flag si es fin de semana
    dim_tiempo["es_fin_de_semana"] = dim_tiempo["nombre_dia"].isin(
        ["Sábado", "Domingo"]
    )

    # 4. Generar surrogate key
    dim_tiempo = dim_tiempo.sort_values("fecha_completa").reset_index(drop=True)
    dim_tiempo["tiempo_id"] = dim_tiempo.index + 1

    # Orden final de columnas
    dim_tiempo = dim_tiempo[
        [
            "tiempo_id",
            "fecha_completa",
            "dia",
            "mes",
            "anio",
            "trimestre",
            "nombre_dia",
            "nombre_mes",
            "es_fin_de_semana",
        ]
    ]

    return dim_tiempo


# ----------------------------------------------------------------
#  DIM CLIENTE
# ----------------------------------------------------------------
def build_dim_cliente(pos_clte_df):
    """
    Crea la dimensión de cliente con surrogate key (cliente_id).
    """
    dim_cliente = pos_clte_df.copy().drop_duplicates(subset=["codigo_cliente"])

    # Generamos surrogate key
    dim_cliente = dim_cliente.reset_index(drop=True)
    dim_cliente["cliente_id"] = dim_cliente.index + 1

    # Reorganizamos columnas para que aparezca primero la PK
    dim_cliente = dim_cliente[
        [
            "cliente_id",
            "codigo_cliente",
            "nombre_cliente",
            "apellido_cliente",
            "sexo_cliente",
        ]
    ]

    return dim_cliente


# ----------------------------------------------------------------
#  DIM BODEGA
# ----------------------------------------------------------------
def build_dim_bodega(mae_bode_df):
    """
    Crea la dimensión de bodega con surrogate key (bodega_id).
    """
    dim_bodega = mae_bode_df.copy().drop_duplicates(subset=["codigo_bodega"])

    dim_bodega = dim_bodega.reset_index(drop=True)
    dim_bodega["bodega_id"] = dim_bodega.index + 1

    # Reordenar
    dim_bodega = dim_bodega[
        ["bodega_id", "codigo_bodega", "descripcion_bodega", "direccion_bodega"]
    ]

    return dim_bodega


# ----------------------------------------------------------------
#  DIM PRODUCTO
# ----------------------------------------------------------------
def build_dim_producto(art_vent_df):
    # Ordenamos por el código del producto para mantener consistencia
    dim_producto = art_vent_df.copy().drop_duplicates(subset=["codigo_producto"])

    dim_producto = dim_producto.reset_index(drop=True)
    dim_producto["producto_id"] = dim_producto.index + 1

    # Reordenamos las columnas para adecuarlas al modelo DWH
    dim_producto = dim_producto[
        [
            "producto_id",
            "codigo_producto",
            "descripcion_producto",
            "referencia_producto",
            "valor_curva",
        ]
    ]

    return dim_producto


# ----------------------------------------------------------------
#  FACT VENTAS
# ----------------------------------------------------------------
def build_fact_ventas(fact_base_df, dim_tiempo, dim_cliente, dim_bodega, dim_producto):
    """
    Construye la tabla de hechos 'fact_ventas' desde un DataFrame ya unido con enc_vent, det_vent y art_vent.

    Entradas:
    - fact_base_df: resultado de extract_fact_ventas_base (ya incluye datos necesarios de las 3 tablas).
    - dimensiones: DataFrames de tiempo, cliente, bodega, producto.

    Salida:
    - fact_ventas: tabla de hechos final con claves foráneas y métricas.
    """

    # Aseguramos nombres consistentes
    fact_base_df.columns = fact_base_df.columns.str.lower()

    # 1. Merge con dimensión tiempo
    fact_df = fact_base_df.merge(
        dim_tiempo[["tiempo_id", "fecha_completa"]],
        how="left",
        left_on="fecha_operacion",
        right_on="fecha_completa",
    )

    # 2. Merge con dimensión cliente
    fact_df = fact_df.merge(
        dim_cliente[["cliente_id", "codigo_cliente"]],
        how="left",
        on="codigo_cliente",
    )

    # 3. Merge con dimensión bodega
    fact_df = fact_df.merge(
        dim_bodega[["bodega_id", "codigo_bodega"]],
        how="left",
        left_on="codigo_bodega",
        right_on="codigo_bodega",
    )

    # 4. Merge con dimensión producto
    fact_df = fact_df.merge(
        dim_producto[["producto_id", "codigo_producto"]],
        how="left",
        on="codigo_producto",
    )

    # 5. Generar surrogate key (venta_id)
    fact_df = fact_df.reset_index(drop=True)
    fact_df["venta_id"] = fact_df.index + 1

    # 6. Seleccionar columnas finales
    fact_ventas = fact_df[
        [
            "venta_id",
            "tiempo_id",
            "cliente_id",
            "bodega_id",
            "producto_id",
            "numero_ticket",
            "codigo_caja",
            "codigo_evento",
            "numero_consecutivo",
            "cantidad",
            "valor_unitario",
            "valor_descuento",
            "valor_venta",
            "iva_porcentaje",
        ]
    ]

    return fact_ventas

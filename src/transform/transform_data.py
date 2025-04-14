import pandas as pd


# ----------------------------------------------------------------
#  DIM TIEMPO
# ----------------------------------------------------------------
def build_dim_tiempo(fact_ventas_df):
    """
    Crea la dimensión de tiempo a partir de las fechas encontradas como fecha_operacion en fact_ventas_df.
    """

    # Extraer las fechas únicas
    fechas = fact_ventas_df["fecha_operacion"].dropna().unique()

    # Asegurarnos de convertir a datetime
    fechas = pd.to_datetime(fechas)

    # Construir un DataFrame base con esas fechas
    dim_tiempo = pd.DataFrame({"fecha_completa": fechas})

    # Crear atributos de fecha
    dim_tiempo["dia"] = dim_tiempo["fecha_completa"].dt.day
    dim_tiempo["mes"] = dim_tiempo["fecha_completa"].dt.month
    dim_tiempo["anio"] = dim_tiempo["fecha_completa"].dt.year
    dim_tiempo["trimestre"] = dim_tiempo["fecha_completa"].dt.quarter

    # Nombre del día y nombre del mes
    dim_tiempo["nombre_dia"] = dim_tiempo["fecha_completa"].dt.day_name(locale="es_ES")
    dim_tiempo["nombre_mes"] = dim_tiempo["fecha_completa"].dt.month_name(
        locale="es_ES"
    )

    # Si es fin de semana o no
    dim_tiempo["es_fin_de_semana"] = dim_tiempo["nombre_dia"].isin(
        ["Sábado", "Domingo"]
    )

    # Generar surrogate key
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
    Crea la dimensión de cliente a partir de la tabla pos_clte.
    """

    # Aseguramos clientes únicos
    dim_cliente = pos_clte_df.copy().drop_duplicates(subset=["codigo_cliente"])

    # Generamos surrogate key
    dim_cliente = dim_cliente.reset_index(drop=True)
    dim_cliente["cliente_id"] = dim_cliente.index + 1

    # Orden final de columnas
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
    Crea la dimensión de bodega a partir de la tabla mae_bode.
    """

    # Aseguramos bodegas únicas
    dim_bodega = mae_bode_df.copy().drop_duplicates(subset=["codigo_bodega"])

    # Generamos surrogate key
    dim_bodega = dim_bodega.reset_index(drop=True)
    dim_bodega["bodega_id"] = dim_bodega.index + 1

    # Orden final de columnas
    dim_bodega = dim_bodega[
        ["bodega_id", "codigo_bodega", "descripcion_bodega", "direccion_bodega"]
    ]

    return dim_bodega


# ----------------------------------------------------------------
#  DIM PRODUCTO
# ----------------------------------------------------------------
def build_dim_producto(art_vent_df):
    """
    Crea la dimensión de producto a partir de la tabla art_vent.
    """

    # Aseguramos productos únicos
    dim_producto = art_vent_df.copy().drop_duplicates(subset=["codigo_producto"])

    # Generamos surrogate key
    dim_producto = dim_producto.reset_index(drop=True)
    dim_producto["producto_id"] = dim_producto.index + 1

    # Orden final de columnas
    dim_producto = dim_producto[
        [
            "producto_id",
            "codigo_producto",
            "descripcion_producto",
        ]
    ]

    return dim_producto


# ----------------------------------------------------------------
#  FACT VENTAS
# ----------------------------------------------------------------
def build_fact_ventas(
    fact_ventas_base_df, dim_tiempo_df, dim_cliente_df, dim_bodega_df, dim_producto_df
):
    """
    Crea la tabla de hechos de ventas a partir de la tabla base de ventas y las dimensiones.
    """

    # Aseguramos hechos únicos
    fact_ventas_base_df = fact_ventas_base_df.drop_duplicates(
        subset=[
            "codigo_bodega",
            "codigo_caja",
            "codigo_evento",
            "numero_ticket",
            "numero_consecutivo",
        ]
    )

    # Join de dim_tiempo con fact_ventas para asignar tiempo_id
    fact_df = fact_ventas_base_df.merge(
        dim_tiempo_df[["tiempo_id", "fecha_completa"]],
        how="left",
        left_on="fecha_operacion",
        right_on="fecha_completa",
    )

    # Join de dim_cliente con fact_ventas para asignar cliente_id
    fact_df = fact_df.merge(
        dim_cliente_df[["cliente_id", "codigo_cliente"]],
        how="left",
        on="codigo_cliente",
    )

    # Join de dim_bodega con fact_ventas para asignar bodega_id
    fact_df = fact_df.merge(
        dim_bodega_df[["bodega_id", "codigo_bodega"]],
        how="left",
        left_on="codigo_bodega",
        right_on="codigo_bodega",
    )

    # Join de dim_producto con fact_ventas para asignar producto_id
    fact_df = fact_df.merge(
        dim_producto_df[["producto_id", "codigo_producto"]],
        how="left",
        on="codigo_producto",
    )

    # Generamos surrogate key
    fact_df = fact_df.reset_index(drop=True)
    fact_df["venta_id"] = fact_df.index + 1

    # Orden final de columnas
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

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
            "direccion_cliente",
            "telefono_cliente",
            "fecha_nacimiento_cliente",
            "email_cliente",
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
            "tipo_producto",
            "estado_producto",
            "referencia_producto",
            "valor_curva",
        ]
    ]

    return dim_producto


# ----------------------------------------------------------------
#  FACT VENTAS
# ----------------------------------------------------------------
def build_fact_ventas(
    enc_vent_df, det_vent_df, dim_tiempo, dim_cliente, dim_bodega, dim_producto
):
    """
    Construye la tabla de hechos 'fact_ventas' usando claves surrogate de las dimensiones.

    Entradas:
    - enc_vent_df: encabezados de ventas (fecha, cliente, bodega, ticket)
    - det_vent_df: detalle de ventas (producto, cantidad, valores)
    - dim_tiempo, dim_cliente, dim_bodega, dim_producto: dimensiones ya transformadas

    Salida:
    - fact_ventas: tabla de hechos con métricas y claves foráneas hacia dimensiones
    """

    # 1. Unir encabezado con detalle
    merged = det_vent_df.merge(
        enc_vent_df,
        on=["codigo_bodega", "codigo_caja", "codigo_evento", "numero_ticket"],
        how="inner",
    )

    # 2. Obtener tiempo_id desde FECHA_OPERACION
    merged = merged.merge(
        dim_tiempo[["tiempo_id", "fecha_completa"]],
        how="left",
        left_on="fecha_operacion",
        right_on="fecha_completa",
    )

    # 3. Obtener cliente_id
    merged = merged.merge(
        dim_cliente[["cliente_id", "codigo_cliente"]],
        how="left",
        left_on="codigo_cliente",
        right_on="codigo_cliente",
    )

    # 4. Obtener bodega_id
    merged = merged.merge(
        dim_bodega[["bodega_id", "codigo_bodega"]],
        how="left",
        left_on="codigo_bodega",
        right_on="codigo_bodega",
    )

    # 5. Obtener producto_id
    merged = merged.merge(
        dim_producto[["producto_id", "codigo_producto"]],
        how="left",
        left_on="codigo_producto",
        right_on="codigo_producto",
    )

    # 6. Generar clave surrogate para fact_ventas
    merged = merged.reset_index(drop=True)
    merged["venta_id"] = merged.index + 1

    # 7. Seleccionar columnas finales
    fact_ventas = merged[
        [
            "venta_id",
            "tiempo_id",
            "cliente_id",
            "bodega_id",
            "producto_id",
            "numero_ticket",
            "cantidad",
            "valor_unitario",
            "valor_descuento",
            "valor_venta",
            "iva_porcentaje",
        ]
    ]

    return fact_ventas

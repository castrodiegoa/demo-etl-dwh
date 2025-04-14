from src.extract.oracle_extract import (
    extract_fact_ventas_base,
    extract_art_vent,
    extract_pos_clte,
    extract_mae_bode,
)
from src.transform.transform_data import (
    build_dim_tiempo,
    build_dim_cliente,
    build_dim_bodega,
    build_dim_producto,
    build_fact_ventas,
)
from src.load.postgres_load import load_to_postgres, get_postgres_engine


def main():
    # -------- EXTRACT --------
    fact_base_df = extract_fact_ventas_base()
    art_vent_df = extract_art_vent()
    pos_clte_df = extract_pos_clte()
    mae_bode_df = extract_mae_bode()

    # -------- DEDUPLICACIÓN DE BASE --------
    fact_base_df = fact_base_df.drop_duplicates(
        subset=[
            "codigo_bodega",
            "codigo_caja",
            "codigo_evento",
            "numero_ticket",
            "numero_consecutivo",
        ]
    )

    # -------- TRANSFORM - Dimensiones --------
    dim_tiempo = build_dim_tiempo(fact_base_df)
    dim_cliente = build_dim_cliente(pos_clte_df)
    dim_bodega = build_dim_bodega(mae_bode_df)
    dim_producto = build_dim_producto(art_vent_df)

    # -------- LOAD - Dimensiones --------
    load_to_postgres(dim_tiempo, "dim_tiempo")
    load_to_postgres(dim_cliente, "dim_cliente")
    load_to_postgres(dim_bodega, "dim_bodega")
    load_to_postgres(dim_producto, "dim_producto")

    # -------- TRANSFORM & LOAD - HECHOS --------
    fact_ventas = build_fact_ventas(
        fact_base_df=fact_base_df,
        dim_tiempo=dim_tiempo,
        dim_cliente=dim_cliente,
        dim_bodega=dim_bodega,
        dim_producto=dim_producto,
    )

    engine = get_postgres_engine()
    fact_ventas.to_sql("fact_ventas", engine, if_exists="replace", index=False)
    print(f"Cargados {len(fact_ventas)} registros en fact_ventas")
    print("ETL finalizado con éxito.")


if __name__ == "__main__":
    main()

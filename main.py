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
from src.load.postgres_load import load_to_postgres


def main():
    # -------- EXTRACT --------
    fact_ventas_base_df = extract_fact_ventas_base()
    art_vent_df = extract_art_vent()
    pos_clte_df = extract_pos_clte()
    mae_bode_df = extract_mae_bode()

    # -------- TRANSFORM - Dimensiones --------
    dim_tiempo_df = build_dim_tiempo(fact_ventas_base_df)
    dim_cliente_df = build_dim_cliente(pos_clte_df)
    dim_bodega_df = build_dim_bodega(mae_bode_df)
    dim_producto_df = build_dim_producto(art_vent_df)

    # -------- LOAD - Dimensiones --------
    load_to_postgres(dim_tiempo_df, "dim_tiempo")
    load_to_postgres(dim_cliente_df, "dim_cliente")
    load_to_postgres(dim_bodega_df, "dim_bodega")
    load_to_postgres(dim_producto_df, "dim_producto")

    # -------- TRANSFORM - Hechos --------
    fact_ventas = build_fact_ventas(
        fact_ventas_base_df=fact_ventas_base_df,
        dim_tiempo_df=dim_tiempo_df,
        dim_cliente_df=dim_cliente_df,
        dim_bodega_df=dim_bodega_df,
        dim_producto_df=dim_producto_df,
    )

    # -------- LOAD - Hechos --------
    load_to_postgres(fact_ventas, "fact_ventas")

    print("ETL finalizado con éxito.")


if __name__ == "__main__":
    main()

from extract.oracle_extract import (
    extract_enc_vent,
    extract_det_vent,
    extract_art_vent,
    extract_pos_clte,
    extract_mae_bode,
)
from transform.transform_data import (
    build_dim_tiempo,
    build_dim_cliente,
    build_dim_bodega,
    build_dim_producto,
    build_fact_ventas,
)
from load.postgres_load import load_to_postgres


def main():
    # -------- EXTRACT --------
    enc_vent_df = extract_enc_vent()
    det_vent_df = extract_det_vent()
    art_vent_df = extract_art_vent()
    pos_clte_df = extract_pos_clte()
    mae_bode_df = extract_mae_bode()

    # -------- TRANSFORM - Dimensiones --------
    dim_tiempo = build_dim_tiempo(enc_vent_df)
    dim_cliente = build_dim_cliente(pos_clte_df)
    dim_bodega = build_dim_bodega(mae_bode_df)
    dim_producto = build_dim_producto(art_vent_df)

    # -------- TRANSFORM - Hecho --------
    fact_ventas = build_fact_ventas(
        enc_vent_df=enc_vent_df,
        det_vent_df=det_vent_df,
        dim_tiempo=dim_tiempo,
        dim_cliente=dim_cliente,
        dim_bodega=dim_bodega,
        dim_producto=dim_producto,
    )

    # -------- LOAD --------
    load_to_postgres(dim_tiempo, "dim_tiempo")
    load_to_postgres(dim_cliente, "dim_cliente")
    load_to_postgres(dim_bodega, "dim_bodega")
    load_to_postgres(dim_producto, "dim_producto")
    load_to_postgres(fact_ventas, "fact_ventas")

    print("ETL finalizado con éxito.")


if __name__ == "__main__":
    main()

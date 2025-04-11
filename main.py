from src.extract.oracle_extract import (
    extract_enc_vent,
    extract_det_vent_in_chunks,
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
    enc_vent_df = extract_enc_vent()
    art_vent_df = extract_art_vent()
    pos_clte_df = extract_pos_clte()
    mae_bode_df = extract_mae_bode()

    # -------- TRANSFORM - Dimensiones --------
    dim_tiempo = build_dim_tiempo(enc_vent_df)
    dim_cliente = build_dim_cliente(pos_clte_df)
    dim_bodega = build_dim_bodega(mae_bode_df)
    dim_producto = build_dim_producto(art_vent_df)

    # -------- LOAD - Dimensiones --------
    load_to_postgres(dim_tiempo, "dim_tiempo")
    load_to_postgres(dim_cliente, "dim_cliente")
    load_to_postgres(dim_bodega, "dim_bodega")
    load_to_postgres(dim_producto, "dim_producto")

    # -------- TRANSFORM and LOAD - Hecho --------
    for chunk in extract_det_vent_in_chunks(chunksize=10000):

        enc_vent_df = enc_vent_df.drop_duplicates(
            subset=["codigo_bodega", "codigo_caja", "codigo_evento", "numero_ticket"]
        )
        chunk = chunk.drop_duplicates(
            subset=[
                "codigo_bodega",
                "codigo_caja",
                "codigo_evento",
                "numero_ticket",
                "numero_consecutivo",
            ]
        )

        # Procesa el chunk para construir la parte de la tabla de hechos.
        fact_chunk = build_fact_ventas(
            enc_vent_df=enc_vent_df,
            det_vent_df=chunk,
            dim_tiempo=dim_tiempo,
            dim_cliente=dim_cliente,
            dim_bodega=dim_bodega,
            dim_producto=dim_producto,
        )

        # Cargar este chunk a PostgreSQL en modo "append"
        from src.load.postgres_load import get_postgres_engine

        engine = get_postgres_engine()
        # Usamos if_exists="append" para no sobreescribir los registros ya cargados.
        fact_chunk.to_sql("fact_ventas", engine, if_exists="append", index=False)
        print(f"Cargado chunk de {len(fact_chunk)} registros en fact_ventas")

        # Liberar memoria para el chunk procesado
        del chunk, fact_chunk
        import gc

        gc.collect()

    print("ETL finalizado con éxito.")


if __name__ == "__main__":
    main()

1) imports
2) validação de libs (s3fs, pandas, pyarrow)
3) logging
4) dataclasses (PostgresConfig, BronzeConfig, TableSpec)
5) load_configs()               👈 já existe
6) >>> AQUI entram as funções do MinIO <<<
7) conexão Postgres
8) helpers SQL (build_select_sql, count_rows)
9) escrita (write_parquet_chunk_minio)
10) extract_to_bronze
11) main()

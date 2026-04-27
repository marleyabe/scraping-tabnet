"""Converte output.csv → producao.parquet (formato wide).

Schema:
    periodo            VARCHAR  -- "Jan/2024"
    data               DATE     -- 2024-01-01 (1º dia do mês)
    municipio_codigo   VARCHAR
    municipio_nome     VARCHAR
    uf_codigo          VARCHAR
    qtd_<XXXX>         DOUBLE   -- 51 colunas, intercaladas
    val_<XXXX>         DOUBLE

Também gera subgrupos.parquet (codigo, nome) para lookup.
"""

from pathlib import Path

import duckdb

INPUT = Path("output.csv")
OUTPUT = Path("producao.parquet")
SUBGRUPOS = Path("subgrupos.parquet")

MONTH_MAP = [
    ("Jan", 1), ("Fev", 2), ("Mar", 3), ("Abr", 4), ("Mai", 5), ("Jun", 6),
    ("Jul", 7), ("Ago", 8), ("Set", 9), ("Out", 10), ("Nov", 11), ("Dez", 12),
]


def main() -> None:
    if not INPUT.exists():
        raise FileNotFoundError(f"{INPUT.resolve()} não encontrado")

    con = duckdb.connect()
    con.execute("CREATE TABLE meses (nome VARCHAR, num INTEGER)")
    con.executemany("INSERT INTO meses VALUES (?, ?)", MONTH_MAP)

    print("Lendo data/*.csv (union_by_name=true para schemas heterogêneos)...")
    con.execute("""
        CREATE TABLE raw AS SELECT * FROM read_csv(
            'data/*__*.csv',
            delim=';', header=true, all_varchar=true,
            quote='"', escape='"',
            strict_mode=false, null_padding=true,
            union_by_name=true,
            max_line_size=10000000
        )
    """)
    raw_rows = con.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
    print(f"  {raw_rows:,} linhas")
    n_cols = con.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='raw'").fetchone()[0]
    print(f"  {n_cols} colunas após união por nome")

    print("Long (parse + unpivot)...")
    con.execute(r"""
    CREATE TABLE long AS
    WITH unpivoted AS (
        UNPIVOT raw
        ON COLUMNS(* EXCLUDE ("Período", "Conteúdo", "Município", "Total"))
        INTO NAME subgrupo_raw VALUE valor_raw
    )
    SELECT
        u."Período" AS periodo,
        CAST(
            CAST(SPLIT_PART(u."Período", '/', 2) AS INTEGER) || '-' ||
            LPAD(CAST(m.num AS VARCHAR), 2, '0') || '-01' AS DATE
        ) AS data,
        CASE WHEN LOWER(u."Conteúdo") LIKE '%qtd%' THEN 'qtd' ELSE 'val' END AS conteudo_pref,
        REGEXP_EXTRACT(TRIM(u."Município"), '^(\d+)', 1) AS municipio_codigo,
        TRIM(REGEXP_REPLACE(TRIM(u."Município"), '^\d+\s+', '')) AS municipio_nome,
        SUBSTR(REGEXP_EXTRACT(TRIM(u."Município"), '^(\d+)', 1), 1, 2) AS uf_codigo,
        REGEXP_EXTRACT(TRIM(u.subgrupo_raw), '^(\d+)', 1) AS subgrupo_codigo,
        TRIM(REGEXP_REPLACE(TRIM(u.subgrupo_raw), '^\d+\s+', '')) AS subgrupo_nome,
        CASE
            WHEN TRIM(u.valor_raw) IN ('-', '') THEN 0
            ELSE COALESCE(
                TRY_CAST(REPLACE(REPLACE(TRIM(u.valor_raw), '.', ''), ',', '.') AS DOUBLE),
                0
            )
        END AS valor
    FROM unpivoted u
    JOIN meses m ON m.nome = SPLIT_PART(u."Período", '/', 1)
    WHERE NOT REGEXP_MATCHES(LOWER(TRIM(u."Município")), 'total|ignorado')
    """)
    long_rows = con.execute("SELECT COUNT(*) FROM long").fetchone()[0]
    print(f"  {long_rows:,} linhas long")

    # Lookup subgrupos
    con.execute(f"""
        COPY (
            SELECT DISTINCT subgrupo_codigo AS codigo, subgrupo_nome AS nome
            FROM long
            ORDER BY codigo
        ) TO '{SUBGRUPOS}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    print(f"  {SUBGRUPOS}: {SUBGRUPOS.stat().st_size / 1024:.1f} KB")

    # Lista de subgrupos para gerar colunas qtd_XXXX / val_XXXX intercaladas
    subgrupos = [
        r[0] for r in con.execute(
            "SELECT DISTINCT subgrupo_codigo FROM long ORDER BY subgrupo_codigo"
        ).fetchall()
    ]
    print(f"  {len(subgrupos)} subgrupos detectados")

    col_exprs = []
    for sg in subgrupos:
        col_exprs.append(
            f"COALESCE(MAX(CASE WHEN subgrupo_codigo='{sg}' AND conteudo_pref='qtd' THEN valor END), 0) AS qtd_{sg}"
        )
        col_exprs.append(
            f"COALESCE(MAX(CASE WHEN subgrupo_codigo='{sg}' AND conteudo_pref='val' THEN valor END), 0) AS val_{sg}"
        )
    cols_sql = ",\n            ".join(col_exprs)

    print("Pivot wide...")
    con.execute(f"""
        CREATE TABLE wide AS
        SELECT
            periodo,
            data,
            municipio_codigo,
            municipio_nome,
            uf_codigo,
            {cols_sql}
        FROM long
        GROUP BY periodo, data, municipio_codigo, municipio_nome, uf_codigo
        ORDER BY data, uf_codigo, municipio_codigo
    """)
    wide_rows = con.execute("SELECT COUNT(*) FROM wide").fetchone()[0]
    wide_cols = con.execute(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_name='wide'"
    ).fetchone()[0]
    print(f"  {wide_rows:,} linhas × {wide_cols} colunas")

    print(f"Gravando {OUTPUT} (ZSTD)...")
    con.execute(f"COPY wide TO '{OUTPUT}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    print(f"  {OUTPUT.stat().st_size / 1024 / 1024:.1f} MB")
    print("Concluído.")


if __name__ == "__main__":
    main()

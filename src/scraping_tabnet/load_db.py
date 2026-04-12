import csv
import re
import os
from collections import defaultdict
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

INPUT_FILE = "output.csv"

DB = dict(
    host=os.environ["DB_HOST"],
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
)

IGNORED_MUNICIPIOS = re.compile(r"total|ignorado", re.IGNORECASE)


def connect():
    return psycopg2.connect(**DB, connect_timeout=10)


def extract_subgrupo_numbers(header: list[str]) -> list[str]:
    """Extrai os números do início dos nomes de subgrupo (colunas 3+).
    Ex: '0101 Ações de promoção...' → '0101'
    Ignora coluna 'Total'.
    """
    numbers = []
    for col in header[3:]:
        stripped = col.strip()
        if stripped.lower() == "total":
            continue
        match = re.match(r"(\d+)", stripped)
        if match:
            numbers.append(match.group(1))
        else:
            numbers.append(stripped)
    return numbers


def build_dynamic_columns(subgrupo_numbers: list[str]) -> list[str]:
    """Gera lista de colunas dinâmicas: qtd_01, val_01, qtd_02, val_02, ..."""
    cols = []
    for num in subgrupo_numbers:
        cols.append(f"qtd_{num}")
        cols.append(f"val_{num}")
    return cols


def recreate_table(conn, dynamic_cols: list[str]) -> None:
    col_defs = ",\n    ".join(f"{col} TEXT" for col in dynamic_cols)
    sql = f"""
    DROP TABLE IF EXISTS producao_ambulatorial;
    CREATE TABLE producao_ambulatorial (
        periodo     VARCHAR(10)  NOT NULL,
        municipio   TEXT         NOT NULL,
        {col_defs},
        PRIMARY KEY (periodo, municipio)
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print("Tabela 'producao_ambulatorial' recriada com novo schema.")


def load_csv(path: Path) -> tuple[list[str], list[list[str]]]:
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
        rows = list(reader)
    return header, rows


def pivot(header: list[str], rows: list[list[str]], subgrupo_numbers: list[str]) -> list[tuple]:
    """Agrupa por (periodo, municipio) e pivota qtd/val em colunas separadas."""
    dynamic_cols = build_dynamic_columns(subgrupo_numbers)

    # Mapeia índice original no CSV → número do subgrupo (excluindo Total)
    col_index_map: list[tuple[int, str]] = []
    for i, col in enumerate(header[3:]):
        stripped = col.strip()
        if stripped.lower() == "total":
            continue
        match = re.match(r"(\d+)", stripped)
        num = match.group(1) if match else stripped
        col_index_map.append((i, num))

    # Agrupa: chave (periodo, municipio) → {qtd_XX: val, val_XX: val}
    grouped: dict[tuple[str, str], dict[str, str]] = defaultdict(dict)

    for row in rows:
        if len(row) < 4:
            continue

        periodo, conteudo, municipio = row[0], row[1], row[2]

        # Ignorar Total e Município ignorado
        if IGNORED_MUNICIPIOS.search(municipio):
            continue

        valores = row[3:]
        prefix = "qtd" if "qtd" in conteudo.lower() else "val"

        for csv_idx, num in col_index_map:
            col_name = f"{prefix}_{num}"
            valor = valores[csv_idx] if csv_idx < len(valores) else None
            grouped[(periodo, municipio)][col_name] = valor

    # Montar registros na ordem correta
    records = []
    for (periodo, municipio), col_vals in grouped.items():
        values = [periodo, municipio]
        for col in dynamic_cols:
            values.append(col_vals.get(col))
        records.append(tuple(values))

    return records


def insert(conn, records: list[tuple], dynamic_cols: list[str]) -> None:
    all_cols = ["periodo", "municipio"] + dynamic_cols
    col_names = ", ".join(all_cols)
    update_set = ", ".join(f"{col} = EXCLUDED.{col}" for col in dynamic_cols)

    sql = f"""
    INSERT INTO producao_ambulatorial ({col_names})
    VALUES %s
    ON CONFLICT (periodo, municipio) DO UPDATE
        SET {update_set};
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, records, page_size=1000)
    conn.commit()


def main() -> None:
    path = Path(INPUT_FILE)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path.resolve()}")

    print(f"Lendo {path}...")
    header, rows = load_csv(path)
    print(f"  {len(rows)} linhas de dados.")

    subgrupo_numbers = extract_subgrupo_numbers(header)
    dynamic_cols = build_dynamic_columns(subgrupo_numbers)
    print(f"  {len(subgrupo_numbers)} subgrupos detectados: {subgrupo_numbers}")

    print("Pivotando dados (qtd/val como colunas)...")
    records = pivot(header, rows, subgrupo_numbers)
    print(f"  {len(records)} registros após pivot (sem Total/ignorado).")

    print("Conectando ao banco...")
    conn = connect()
    print("Conexão OK.")

    recreate_table(conn, dynamic_cols)

    print("Inserindo no banco (upsert)...")
    insert(conn, records, dynamic_cols)
    print("Concluído!")

    conn.close()


if __name__ == "__main__":
    main()

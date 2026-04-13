import csv
import os
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

INPUT_FILE = "dados_tratados.csv"

DB = dict(
    host=os.environ["DB_HOST"],
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
)


def connect():
    return psycopg2.connect(**DB, connect_timeout=10)


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
    print("Tabela 'producao_ambulatorial' recriada.")


def insert(conn, records: list[tuple], all_cols: list[str]) -> None:
    col_names = ", ".join(all_cols)
    sql = f"INSERT INTO producao_ambulatorial ({col_names}) VALUES %s"
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, records, page_size=1000)
    conn.commit()


def main() -> None:
    path = Path(INPUT_FILE)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path.resolve()}")

    print(f"Lendo {path}...")
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        header = [c.strip() for c in next(reader)]
        rows = [tuple(r) for r in reader if len(r) >= len(header)]

    dynamic_cols = header[2:]
    print(f"  {len(rows)} linhas, {len(dynamic_cols)} colunas dinâmicas.")

    conn = connect()
    print("Conexão OK.")
    recreate_table(conn, dynamic_cols)

    print("Inserindo...")
    insert(conn, rows, header)
    print("Concluído!")
    conn.close()


if __name__ == "__main__":
    main()

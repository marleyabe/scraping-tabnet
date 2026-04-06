import csv
import os
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

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS producao_ambulatorial (
    periodo     VARCHAR(10)  NOT NULL,
    conteudo    VARCHAR(50)  NOT NULL,
    municipio   TEXT         NOT NULL,
    subgrupo    TEXT         NOT NULL,
    valor       TEXT,
    PRIMARY KEY (periodo, conteudo, municipio, subgrupo)
);
"""

INSERT_SQL = """
INSERT INTO producao_ambulatorial (periodo, conteudo, municipio, subgrupo, valor)
VALUES %s
ON CONFLICT (periodo, conteudo, municipio, subgrupo) DO UPDATE
    SET valor = EXCLUDED.valor;
"""


def connect():
    return psycopg2.connect(**DB, connect_timeout=10)


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    print("Tabela 'producao_ambulatorial' verificada/criada.")


def load_csv(path: Path) -> tuple[list[str], list[list[str]]]:
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
        rows = list(reader)
    return header, rows


def melt(header: list[str], rows: list[list[str]]) -> list[tuple]:
    """Converte formato wide para long: uma linha por (período, conteúdo, município, subgrupo)."""
    # header: [Período, Conteúdo, Município, subgrupo1, subgrupo2, ...]
    subgrupos = header[3:]
    records = []
    for row in rows:
        if len(row) < 4:
            continue
        periodo, conteudo, municipio = row[0], row[1], row[2]
        valores = row[3:]
        for subgrupo, valor in zip(subgrupos, valores):
            records.append((periodo, conteudo, municipio, subgrupo, valor))
    return records


def insert(conn, records: list[tuple]) -> None:
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, INSERT_SQL, records, page_size=1000)
    conn.commit()


def main() -> None:
    path = Path(INPUT_FILE)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path.resolve()}")

    print(f"Conectando ao banco...")
    conn = connect()
    print("Conexão OK.")

    ensure_table(conn)

    print(f"Lendo {path}...")
    header, rows = load_csv(path)
    print(f"  {len(rows)} linhas de dados, {len(header) - 3} subgrupos.")

    print("Convertendo para formato long...")
    records = melt(header, rows)
    print(f"  {len(records)} registros a inserir.")

    print("Inserindo no banco (upsert)...")
    insert(conn, records)
    print(f"  Concluído!")

    conn.close()


if __name__ == "__main__":
    main()

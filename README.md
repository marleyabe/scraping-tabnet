# scraping-tabnet

Scraper de **Produção Ambulatorial do SUS** (TabNet/DATASUS) com tratamento e carga em PostgreSQL.

## Stack
- Python 3.12 + [uv](https://docs.astral.sh/uv/)
- Playwright (Chromium headless)
- BeautifulSoup + lxml
- psycopg2 (PostgreSQL)

## Instalação

```bash
uv sync
uv run playwright install chromium
```

Crie um `.env` na raiz baseado em `.env.example`:

```env
DB_HOST=...
DB_NAME=...
DB_USER=...
DB_PASSWORD=...
```

## Uso

### 1. Scraping
Coleta dados do TabNet e gera `output.csv` (formato wide).

```bash
uv run scrape
```

Será solicitado o período:
```
Data de início (MM/AAAA): 01/2024
Data final (MM/AAAA): 05/2025
```

### 2. Carga no banco
Pivota qtd/val em colunas separadas, filtra "Total" e "Ignorado", e recria a tabela `producao_ambulatorial`.

```bash
uv run load-db
```

## Estrutura final da tabela

```sql
CREATE TABLE producao_ambulatorial (
    periodo   VARCHAR(10) NOT NULL,
    municipio TEXT        NOT NULL,
    qtd_0101 TEXT, val_0101 TEXT,
    qtd_0102 TEXT, val_0102 TEXT,
    -- ... uma dupla qtd_XX/val_XX por subgrupo
    PRIMARY KEY (periodo, municipio)
);
```

- **qtd_XX**: quantidade aprovada do subgrupo XX
- **val_XX**: valor aprovado do subgrupo XX
- Schema gerado dinamicamente a partir dos subgrupos do CSV.

## Apresentação
Slides em HTML estão em `presentation/index.html` — abra direto no navegador.

## Estrutura
```
src/scraping_tabnet/
  scraper.py    # coleta do TabNet via Playwright
  load_db.py    # pivot + filtro + carga PostgreSQL
presentation/   # apresentação HTML/CSS
```

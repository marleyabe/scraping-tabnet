# scraping-tabnet

Pipeline de coleta, tratamento e visualização da **Produção Ambulatorial do SUS** (TabNet/DATASUS).

> 📄 Relatório técnico completo: [`relatorio.pdf`](relatorio.pdf)

## O que faz

1. **Coleta** dados do TabNet via 4 *workers* paralelos em Playwright (`uv run scrape`)
2. **Trata** o CSV e gera arquivos Parquet otimizados (`uv run build-parquet`)
3. **Visualiza** num dashboard Streamlit + DuckDB com 7 atos narrativos (`uv run streamlit run app.py`)

## Stack

- Python 3.12 + [uv](https://docs.astral.sh/uv/)
- Playwright (Chromium headless)
- DuckDB (consulta colunar sobre Parquet)
- Streamlit + Plotly (dashboard)
- PostgreSQL via psycopg2 (carga opcional)

## Instalação

```bash
uv sync
uv run playwright install chromium
```

## Uso

### 1. Coleta (opcional — dados já no repo)

```bash
uv run scrape          # 4 workers paralelos (~5-7 min)
NUM_WORKERS=1 uv run scrape   # sequencial, mais lento porém imune a race conditions
```

Pede `MM/AAAA` início e fim. Gera `data/qabrAAMM__Conteudo.csv` por consulta + merge em `output.csv`.

### 2. Conversão para Parquet

```bash
uv run build-parquet
```

Gera `producao.parquet` (formato wide, ~10 MB) e `subgrupos.parquet`.
Usa `union_by_name=true` para acomodar mudanças de esquema do TabNet entre períodos.

### 3. Dashboard

```bash
uv run streamlit run app.py
```

Abre em `http://localhost:8501`. Estrutura em narrativa única (scroll):

| Ato | Pergunta | Visual |
|---|---|---|
| Setup | Qual a escala? | 4 KPIs + headline |
| Onde | Distribuição geográfica | Mapa coroplético + bar UF + custo médio UF |
| O quê | Composição | Treemap + Pareto |
| Quando | Tendência | Linha dual + área stacked regional |
| Quem | Distribuição | Boxplot UF + top 15 municípios |
| Quem cresce | Aceleração/queda | Bar divergente UF + bar divergente subgrupo |
| Atenção | Outliers | Tabela com z-score |

### 4. Carga em PostgreSQL (opcional)

```bash
cp .env.example .env  # edite com credenciais
uv run load-db
```

## Deploy (Streamlit Cloud)

Os arquivos `producao.parquet`, `subgrupos.parquet` e `br_states.geojson` estão versionados — basta apontar `app.py` no Streamlit Cloud. Memória de pico ≈ 30-80 MB.

## Estrutura

```
.
├── app.py                    # dashboard Streamlit
├── generate_figures.py       # exporta PNGs estáticos para o relatório
├── producao.parquet          # dados wide (10 MB) — versionado
├── subgrupos.parquet         # lookup código→nome (1 KB)
├── br_states.geojson         # GeoJSON dos estados do Brasil
├── relatorio.pdf             # relatório técnico ABNT
├── src/scraping_tabnet/
│   ├── scraper.py            # coleta TabNet, 4 workers convergentes
│   ├── build_parquet.py      # CSV → Parquet (union_by_name)
│   └── load_db.py            # carga PostgreSQL
└── presentation/             # slides HTML
```

Diretórios *não* versionados (re-geráveis):
- `data/` — CSVs brutos do scraper
- `output.csv` — merge intermediário
- `CIA046 - Template para Relatório Técnico (2026)/` — fontes LaTeX

## Dados

| | |
|---|---|
| Período coletado | Jan/2024 a Jan/2026 (25 meses) |
| Procedimentos | 10,53 bilhões |
| Valor pago | R$ 69,17 bilhões |
| Municípios | 5.571 |
| Subgrupos | 62 |

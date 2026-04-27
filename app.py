"""Dashboard Produção Ambulatorial SUS — narrativa em scroll único.

Estrutura: setup (KPIs) → onde (mapa) → o quê (treemap) → quando (linha) →
quem (boxplot/top munis) → atenção (anomalias).
"""

import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

PARQUET = Path("producao.parquet")
SUBGRUPOS = Path("subgrupos.parquet")
GEOJSON = Path("br_states.geojson")

UF_NOMES = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL",
    "28": "SE", "29": "BA",
    "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
    "41": "PR", "42": "SC", "43": "RS",
    "50": "MS", "51": "MT", "52": "GO", "53": "DF",
}
UF_REGIAO = {
    "RO": "Norte", "AC": "Norte", "AM": "Norte", "RR": "Norte", "PA": "Norte", "AP": "Norte", "TO": "Norte",
    "MA": "Nordeste", "PI": "Nordeste", "CE": "Nordeste", "RN": "Nordeste", "PB": "Nordeste",
    "PE": "Nordeste", "AL": "Nordeste", "SE": "Nordeste", "BA": "Nordeste",
    "MG": "Sudeste", "ES": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "SC": "Sul", "RS": "Sul",
    "MS": "Centro-Oeste", "MT": "Centro-Oeste", "GO": "Centro-Oeste", "DF": "Centro-Oeste",
}

REGIAO_COLOR = {
    "Sudeste": "#2E86AB", "Sul": "#A23B72", "Nordeste": "#F18F01",
    "Centro-Oeste": "#C73E1D", "Norte": "#3B6E22",
}

st.set_page_config(page_title="Produção Ambulatorial SUS", layout="wide", initial_sidebar_state="expanded")

# ---------- Estilo ----------
st.markdown("""
<style>
    .block-container { padding-top: 2rem; max-width: 1400px; }
    h1 { font-size: 2.4rem; }
    h2 { margin-top: 2rem; padding-top: 1rem; border-top: 1px solid #e6e6e6; }
    [data-testid="stMetricValue"] { font-size: 2.2rem; font-weight: 700; }
    [data-testid="stMetricLabel"] { font-size: 0.95rem; color: #666; }
</style>
""", unsafe_allow_html=True)


# ---------- Conexão ----------

@st.cache_resource
def get_con() -> duckdb.DuckDBPyConnection:
    if not PARQUET.exists():
        st.error(f"{PARQUET} não encontrado. Rode `uv run build-parquet`.")
        st.stop()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA memory_limit='256MB'")
    con.execute(f"CREATE VIEW prod AS SELECT * FROM read_parquet('{PARQUET}')")
    con.execute(f"CREATE VIEW sg AS SELECT * FROM read_parquet('{SUBGRUPOS}')")
    return con


@st.cache_resource
def get_geojson() -> dict:
    if not GEOJSON.exists():
        return {}
    with GEOJSON.open() as f:
        return json.load(f)


@st.cache_data(ttl=3600)
def get_filters() -> dict:
    con = get_con()
    dmin, dmax = con.execute("SELECT MIN(data), MAX(data) FROM prod").fetchone()
    ufs = [r[0] for r in con.execute("SELECT DISTINCT uf_codigo FROM prod ORDER BY uf_codigo").fetchall()]
    sgs = con.execute("SELECT codigo, nome FROM sg ORDER BY codigo").fetchall()
    return {"dmin": dmin, "dmax": dmax, "ufs": ufs, "subgrupos": sgs}


# ---------- Queries ----------

def cols_for(prefix: str, codigos) -> list[str]:
    return [f'"{prefix}_{c}"' for c in codigos]


def where_clause(data_ini, data_fim, ufs: tuple) -> tuple[str, list]:
    parts = ["data BETWEEN ? AND ?"]
    params: list = [data_ini, data_fim]
    if ufs:
        parts.append(f"uf_codigo IN ({','.join(['?'] * len(ufs))})")
        params += list(ufs)
    return " AND ".join(parts), params


@st.cache_data(ttl=600)
def kpis(data_ini, data_fim, ufs: tuple, codigos: tuple) -> dict:
    con = get_con()
    sq = " + ".join(cols_for("qtd", codigos)) or "0"
    sv = " + ".join(cols_for("val", codigos)) or "0"
    where_sql, params = where_clause(data_ini, data_fim, ufs)
    r = con.execute(f"""
        SELECT
            COUNT(DISTINCT municipio_codigo),
            COUNT(DISTINCT periodo),
            SUM({sq}), SUM({sv})
        FROM prod WHERE {where_sql}
    """, params).fetchone()
    munis, periodos = r[0] or 0, r[1] or 0
    qtd, valor = r[2] or 0.0, r[3] or 0.0
    return {
        "munis": munis, "periodos": periodos, "qtd": qtd, "valor": valor,
        "custo_medio": (valor / qtd) if qtd else 0,
    }


@st.cache_data(ttl=600)
def serie_mensal(data_ini, data_fim, ufs: tuple, codigos: tuple) -> pd.DataFrame:
    con = get_con()
    sq = " + ".join(cols_for("qtd", codigos)) or "0"
    sv = " + ".join(cols_for("val", codigos)) or "0"
    where_sql, params = where_clause(data_ini, data_fim, ufs)
    return con.execute(f"""
        SELECT data, SUM({sq}) AS qtd, SUM({sv}) AS valor
        FROM prod WHERE {where_sql}
        GROUP BY data ORDER BY data
    """, params).df()


@st.cache_data(ttl=600)
def por_uf(data_ini, data_fim, ufs: tuple, codigos: tuple, conteudo: str) -> pd.DataFrame:
    con = get_con()
    sum_expr = " + ".join(cols_for(conteudo, codigos)) or "0"
    where_sql, params = where_clause(data_ini, data_fim, ufs)
    df = con.execute(f"""
        SELECT uf_codigo, SUM({sum_expr}) AS total
        FROM prod WHERE {where_sql}
        GROUP BY uf_codigo ORDER BY total DESC
    """, params).df()
    df["uf"] = df["uf_codigo"].map(UF_NOMES).fillna(df["uf_codigo"])
    df["regiao"] = df["uf"].map(UF_REGIAO)
    return df


@st.cache_data(ttl=600)
def por_subgrupo(data_ini, data_fim, ufs: tuple, conteudo: str) -> pd.DataFrame:
    con = get_con()
    codes = [r[0] for r in con.execute("SELECT codigo FROM sg ORDER BY codigo").fetchall()]
    qcols = ", ".join(cols_for("qtd", codes))
    vcols = ", ".join(cols_for("val", codes))
    where_sql, params = where_clause(data_ini, data_fim, ufs)
    df = con.execute(f"""
        WITH base AS (SELECT * FROM prod WHERE {where_sql}),
             q AS (SELECT SUBSTR(name, 5) AS codigo, SUM(value) AS qtd
                   FROM (UNPIVOT base ON {qcols}) GROUP BY codigo),
             v AS (SELECT SUBSTR(name, 5) AS codigo, SUM(value) AS valor
                   FROM (UNPIVOT base ON {vcols}) GROUP BY codigo)
        SELECT q.codigo, q.qtd, v.valor,
               CASE WHEN q.qtd > 0 THEN v.valor / q.qtd ELSE 0 END AS custo_medio
        FROM q JOIN v USING (codigo)
        ORDER BY {'qtd' if conteudo == 'qtd' else 'valor'} DESC
    """, params).df()
    sgs = con.execute("SELECT codigo, nome FROM sg").df()
    df = df.merge(sgs, on="codigo", how="left")
    df["label"] = df["codigo"] + " " + df["nome"].fillna("").str.slice(0, 40)
    return df


@st.cache_data(ttl=600)
def custo_medio_por_uf(data_ini, data_fim, ufs: tuple, codigos: tuple) -> pd.DataFrame:
    con = get_con()
    sq = " + ".join(cols_for("qtd", codigos)) or "0"
    sv = " + ".join(cols_for("val", codigos)) or "0"
    where_sql, params = where_clause(data_ini, data_fim, ufs)
    df = con.execute(f"""
        SELECT uf_codigo, SUM({sq}) AS qtd, SUM({sv}) AS valor
        FROM prod WHERE {where_sql}
        GROUP BY uf_codigo
    """, params).df()
    df["uf"] = df["uf_codigo"].map(UF_NOMES).fillna(df["uf_codigo"])
    df["regiao"] = df["uf"].map(UF_REGIAO)
    df["custo_medio"] = (df["valor"] / df["qtd"]).fillna(0)
    return df.sort_values("custo_medio", ascending=False)


@st.cache_data(ttl=600)
def serie_por_regiao(data_ini, data_fim, ufs: tuple, codigos: tuple) -> pd.DataFrame:
    con = get_con()
    sq = " + ".join(cols_for("qtd", codigos)) or "0"
    where_sql, params = where_clause(data_ini, data_fim, ufs)
    df = con.execute(f"""
        SELECT data, uf_codigo, SUM({sq}) AS qtd
        FROM prod WHERE {where_sql}
        GROUP BY data, uf_codigo
        ORDER BY data
    """, params).df()
    df["uf"] = df["uf_codigo"].map(UF_NOMES).fillna(df["uf_codigo"])
    df["regiao"] = df["uf"].map(UF_REGIAO)
    return df.groupby(["data", "regiao"], as_index=False)["qtd"].sum()


@st.cache_data(ttl=600)
def crescimento_yoy_uf(data_ini, data_fim, ufs: tuple, codigos: tuple) -> pd.DataFrame:
    """Variação % entre primeira e segunda metade do período por UF."""
    con = get_con()
    sq = " + ".join(cols_for("qtd", codigos)) or "0"
    where_sql, params = where_clause(data_ini, data_fim, ufs)
    df = con.execute(f"""
        SELECT data, uf_codigo, SUM({sq}) AS qtd
        FROM prod WHERE {where_sql}
        GROUP BY data, uf_codigo
    """, params).df()
    if df.empty:
        return df
    df["data"] = pd.to_datetime(df["data"])
    midpoint = df["data"].min() + (df["data"].max() - df["data"].min()) / 2
    primeira = df[df["data"] < midpoint].groupby("uf_codigo")["qtd"].mean()
    segunda = df[df["data"] >= midpoint].groupby("uf_codigo")["qtd"].mean()
    out = pd.DataFrame({"primeira": primeira, "segunda": segunda}).reset_index()
    out["var_pct"] = ((out["segunda"] - out["primeira"]) / out["primeira"] * 100).fillna(0)
    out["uf"] = out["uf_codigo"].map(UF_NOMES).fillna(out["uf_codigo"])
    return out.sort_values("var_pct", ascending=False)


@st.cache_data(ttl=600)
def crescimento_subgrupos(data_ini, data_fim, ufs: tuple) -> pd.DataFrame:
    """Variação % por subgrupo entre 1ª e 2ª metade do período."""
    con = get_con()
    codes = [r[0] for r in con.execute("SELECT codigo FROM sg ORDER BY codigo").fetchall()]
    qcols = ", ".join(cols_for("qtd", codes))
    where_sql, params = where_clause(data_ini, data_fim, ufs)
    df = con.execute(f"""
        WITH base AS (SELECT * FROM prod WHERE {where_sql}),
             m AS (SELECT data, SUBSTR(name, 5) AS codigo, SUM(value) AS qtd
                   FROM (UNPIVOT base ON {qcols}) GROUP BY data, codigo)
        SELECT * FROM m
    """, params).df()
    if df.empty:
        return df
    df["data"] = pd.to_datetime(df["data"])
    midpoint = df["data"].min() + (df["data"].max() - df["data"].min()) / 2
    primeira = df[df["data"] < midpoint].groupby("codigo")["qtd"].mean()
    segunda = df[df["data"] >= midpoint].groupby("codigo")["qtd"].mean()
    out = pd.DataFrame({"primeira": primeira, "segunda": segunda}).reset_index()
    out = out[out["primeira"] >= 1000]  # corta ruído
    out["var_pct"] = ((out["segunda"] - out["primeira"]) / out["primeira"] * 100).fillna(0)
    sgs = con.execute("SELECT codigo, nome FROM sg").df()
    out = out.merge(sgs, on="codigo", how="left")
    out["label"] = out["codigo"] + " " + out["nome"].fillna("").str.slice(0, 36)
    return out.sort_values("var_pct", ascending=False)


@st.cache_data(ttl=600)
def municipios_dist(data_ini, data_fim, ufs: tuple, codigos: tuple, conteudo: str) -> pd.DataFrame:
    con = get_con()
    sum_expr = " + ".join(cols_for(conteudo, codigos)) or "0"
    where_sql, params = where_clause(data_ini, data_fim, ufs)
    df = con.execute(f"""
        SELECT uf_codigo, municipio_codigo, municipio_nome, SUM({sum_expr}) AS total
        FROM prod WHERE {where_sql}
        GROUP BY uf_codigo, municipio_codigo, municipio_nome
    """, params).df()
    df["uf"] = df["uf_codigo"].map(UF_NOMES).fillna(df["uf_codigo"])
    df["municipio"] = df["municipio_nome"].str.title()
    return df


@st.cache_data(ttl=600)
def anomalias(data_ini, data_fim, ufs: tuple, conteudo: str, z_thresh: float = 2.5) -> pd.DataFrame:
    con = get_con()
    codes = [r[0] for r in con.execute("SELECT codigo FROM sg ORDER BY codigo").fetchall()]
    cols_sql = ", ".join(cols_for(conteudo, codes))
    where_sql, params = where_clause(data_ini, data_fim, ufs)
    df = con.execute(f"""
        WITH base AS (SELECT * FROM prod WHERE {where_sql}),
             agg AS (SELECT uf_codigo, data, SUBSTR(name, 5) AS codigo, SUM(value) AS total
                     FROM (UNPIVOT base ON {cols_sql}) GROUP BY uf_codigo, data, codigo),
             stats AS (SELECT uf_codigo, codigo,
                              AVG(total) AS mu, STDDEV_SAMP(total) AS sigma
                       FROM agg GROUP BY uf_codigo, codigo)
        SELECT a.uf_codigo, a.data, a.codigo, a.total, s.mu,
               (a.total - s.mu) / NULLIF(s.sigma, 0) AS z
        FROM agg a JOIN stats s USING (uf_codigo, codigo)
        WHERE s.sigma > 0 AND s.mu >= 1000
          AND ABS((a.total - s.mu) / s.sigma) >= {z_thresh}
        ORDER BY ABS((a.total - s.mu) / s.sigma) DESC
        LIMIT 12
    """, params).df()
    if df.empty:
        return df
    sgs = con.execute("SELECT codigo, nome FROM sg").df()
    df = df.merge(sgs, on="codigo", how="left")
    df["uf"] = df["uf_codigo"].map(UF_NOMES).fillna(df["uf_codigo"])
    df["mes"] = pd.to_datetime(df["data"]).dt.strftime("%m/%Y")
    return df


# ---------- Helpers de formatação ----------

def fmt_brl(v):
    if abs(v) >= 1000:
        return f"R$ {v:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
def fmt_int(v): return f"{v:,.0f}".replace(",", ".")

def fmt_compact(v: float) -> str:
    if abs(v) >= 1e9: return f"{v/1e9:.2f} bi".replace(".", ",")
    if abs(v) >= 1e6: return f"{v/1e6:.1f} mi".replace(".", ",")
    if abs(v) >= 1e3: return f"{v/1e3:.0f} mil".replace(".", ",")
    return f"{v:.0f}"


# ============================================================
# UI
# ============================================================

f = get_filters()
sgs_dict = dict(f["subgrupos"])

with st.sidebar:
    st.markdown("### Filtros")
    data_range = st.date_input(
        "Período",
        value=(f["dmin"], f["dmax"]),
        min_value=f["dmin"], max_value=f["dmax"],
        format="DD/MM/YYYY",
    )
    if isinstance(data_range, tuple) and len(data_range) == 2:
        data_ini, data_fim = data_range
    else:
        data_ini, data_fim = f["dmin"], f["dmax"]

    ufs_sel = st.multiselect(
        "UFs (vazio = Brasil)",
        options=f["ufs"],
        format_func=lambda c: f"{UF_NOMES.get(c, c)} ({UF_REGIAO.get(UF_NOMES.get(c, c), '')})",
    )
    sg_sel = st.multiselect(
        "Subgrupos (vazio = todos)",
        options=[c for c, _ in f["subgrupos"]],
        format_func=lambda c: f"{c} — {sgs_dict.get(c, '')[:50]}",
    )
    st.caption(f"Dados de {f['dmin']:%m/%Y} a {f['dmax']:%m/%Y} | DATASUS/TabNet")

ufs_t = tuple(ufs_sel)
sg_t = tuple(sg_sel) if sg_sel else tuple(c for c, _ in f["subgrupos"])

# ============================================================
# ATO 1 — Hero / Setup
# ============================================================

st.markdown("# Produção Ambulatorial do SUS")

k = kpis(data_ini, data_fim, ufs_t, sg_t)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Procedimentos", fmt_compact(k["qtd"]), help=fmt_int(k["qtd"]))
c2.metric("Valor pago", fmt_compact(k["valor"]).replace("bi", "bi (R$)"), help=fmt_brl(k["valor"]))
c3.metric("Municípios atendidos", fmt_int(k["munis"]))
c4.metric("Custo médio / proc.", fmt_brl(k["custo_medio"]))

# Insight headline (calculado)
df_uf_q = por_uf(data_ini, data_fim, ufs_t, sg_t, "qtd")
total_q = df_uf_q["total"].sum()
sudeste_pct = df_uf_q[df_uf_q["regiao"] == "Sudeste"]["total"].sum() / max(total_q, 1) * 100
df_serie = serie_mensal(data_ini, data_fim, ufs_t, sg_t)
crescimento_pct = 0.0
if len(df_serie) >= 12:
    primeiros = df_serie.head(6)["qtd"].mean()
    ultimos = df_serie.tail(6)["qtd"].mean()
    if primeiros > 0:
        crescimento_pct = (ultimos - primeiros) / primeiros * 100

st.markdown(
    f"###### **{sudeste_pct:.0f}%** dos procedimentos no Sudeste · "
    f"crescimento de **{crescimento_pct:+.1f}%** entre os primeiros e últimos 6 meses · "
    f"**{k['periodos']}** meses analisados"
)

# ============================================================
# ATO 2 — Onde (Geografia)
# ============================================================

st.markdown("## Onde está a produção")

col_map, col_uf = st.columns([1, 1])

with col_map:
    geojson = get_geojson()
    if geojson:
        df_uf_q["valor_total"] = por_uf(data_ini, data_fim, ufs_t, sg_t, "val")["total"].values
        df_uf_q["custo_medio_uf"] = (df_uf_q["valor_total"] / df_uf_q["total"]).round(2)
        fig = px.choropleth(
            df_uf_q, geojson=geojson, locations="uf",
            featureidkey="properties.sigla",
            color="total",
            color_continuous_scale="Blues",
            hover_name="uf",
            hover_data={"total": ":,.0f", "regiao": True, "custo_medio_uf": ":,.2f", "uf": False},
            labels={"total": "Procedimentos", "custo_medio_uf": "Custo médio R$"},
        )
        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(height=460, margin=dict(l=0, r=0, t=0, b=0),
                          coloraxis_colorbar=dict(title="Qtd"))
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("GeoJSON não carregado — mostrando barras.")

with col_uf:
    fig = px.bar(
        df_uf_q.head(15), x="total", y="uf", orientation="h", color="regiao",
        color_discrete_map=REGIAO_COLOR,
        labels={"total": "Procedimentos", "uf": "", "regiao": "Região"},
    )
    fig.update_layout(
        yaxis={"categoryorder": "total ascending"},
        height=460, margin=dict(l=0, r=0, t=10, b=0),
        legend=dict(orientation="h", yanchor="top", y=-0.05),
    )
    st.plotly_chart(fig, width="stretch")

# Custo médio por UF (revela disparidade de preço entre estados)
st.markdown("##### Custo médio do procedimento por UF")
df_cm_uf = custo_medio_por_uf(data_ini, data_fim, ufs_t, sg_t)
fig = px.bar(
    df_cm_uf, x="uf", y="custo_medio", color="regiao",
    color_discrete_map=REGIAO_COLOR,
    labels={"custo_medio": "R$ por procedimento", "uf": "", "regiao": "Região"},
)
fig.update_layout(
    height=320, margin=dict(l=0, r=0, t=10, b=0),
    xaxis={"categoryorder": "total descending"},
    legend=dict(orientation="h", yanchor="top", y=-0.15),
)
st.plotly_chart(fig, width="stretch")

# ============================================================
# ATO 3 — O quê (Composição)
# ============================================================

st.markdown("## O que o SUS faz")

df_sg = por_subgrupo(data_ini, data_fim, ufs_t, "qtd")

df_tm = df_sg.head(30).copy()
df_tm["log_custo"] = np.log10(df_tm["custo_medio"].clip(lower=0.01))
fig = px.treemap(
    df_tm, path=[px.Constant("Total"), "label"],
    values="qtd",
    color="log_custo", color_continuous_scale="YlOrRd",
    hover_data={"qtd": ":,.0f", "valor": ":,.0f", "custo_medio": ":,.2f", "log_custo": False},
)
fig.update_layout(height=520, margin=dict(l=0, r=0, t=10, b=0),
                  coloraxis_colorbar=dict(
                      title="R$/proc.",
                      tickvals=[-1, 0, 1, 2, 3], ticktext=["0,1", "1", "10", "100", "1k"],
                  ))
st.plotly_chart(fig, width="stretch")

# Pareto: concentração de procedimentos
st.markdown("##### Concentração — quantos subgrupos formam 80% do volume?")
df_par = df_sg.sort_values("qtd", ascending=False).reset_index(drop=True).copy()
df_par["acum_pct"] = df_par["qtd"].cumsum() / df_par["qtd"].sum() * 100
n_80 = int((df_par["acum_pct"] <= 80).sum()) + 1
top_par = df_par.head(20).copy()
top_par["x"] = top_par["codigo"] + " " + top_par["nome"].fillna("").str.slice(0, 22)
fig = go.Figure()
fig.add_trace(go.Bar(x=top_par["x"], y=top_par["qtd"], name="Procedimentos",
                     marker_color="#2E86AB", yaxis="y1",
                     hovertemplate="%{x}<br>Qtd: %{y:,.0f}<extra></extra>"))
fig.add_trace(go.Scatter(x=top_par["x"], y=top_par["acum_pct"], name="% acumulado",
                         mode="lines+markers", line=dict(color="#C73E1D", width=2),
                         yaxis="y2",
                         hovertemplate="%{x}<br>%{y:.1f}%<extra></extra>"))
fig.add_hline(y=80, line=dict(color="#888", dash="dash"), yref="y2",
              annotation_text=f"80% — {n_80} subgrupos", annotation_position="top right")
fig.update_layout(
    height=420, margin=dict(l=0, r=0, t=10, b=120),
    yaxis=dict(title="Procedimentos"),
    yaxis2=dict(title="% acumulado", overlaying="y", side="right", range=[0, 105]),
    legend=dict(orientation="h", yanchor="bottom", y=1.05),
    xaxis=dict(title="", type="category", tickangle=-45,
               categoryorder="array", categoryarray=top_par["x"].tolist()),
)
st.plotly_chart(fig, width="stretch")

# ============================================================
# ATO 4 — Quando (Evolução)
# ============================================================

st.markdown("## Como evolui no tempo")

if not df_serie.empty:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_serie["data"], y=df_serie["qtd"], name="Procedimentos",
        mode="lines+markers", line=dict(color="#2E86AB", width=3),
        fill="tozeroy", fillcolor="rgba(46,134,171,0.1)",
    ))
    fig.add_trace(go.Scatter(
        x=df_serie["data"], y=df_serie["valor"], name="Valor (R$)",
        mode="lines+markers", line=dict(color="#C73E1D", width=2, dash="dot"),
        yaxis="y2",
    ))
    # Anotações automáticas: pico e vale de qtd
    idx_max = df_serie["qtd"].idxmax()
    idx_min = df_serie["qtd"].idxmin()
    for idx, label, ay in [(idx_max, "pico", -40), (idx_min, "vale", 40)]:
        row = df_serie.loc[idx]
        fig.add_annotation(
            x=row["data"], y=row["qtd"], text=f"{label}<br>{row['data']:%m/%Y}",
            showarrow=True, arrowhead=2, ax=0, ay=ay,
            font=dict(size=11, color="#444"),
            bgcolor="white", bordercolor="#aaa",
        )
    fig.update_layout(
        height=420, margin=dict(l=0, r=0, t=20, b=0),
        xaxis=dict(title=""),
        yaxis=dict(title="Procedimentos", showgrid=True),
        yaxis2=dict(title="Valor (R$)", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        hovermode="x unified",
    )
    st.plotly_chart(fig, width="stretch")

# Composição mensal por região (stacked area)
st.markdown("##### Contribuição de cada região, mês a mês")
df_reg = serie_por_regiao(data_ini, data_fim, ufs_t, sg_t)
fig = px.area(df_reg, x="data", y="qtd", color="regiao",
              color_discrete_map=REGIAO_COLOR,
              category_orders={"regiao": ["Sudeste", "Nordeste", "Sul", "Centro-Oeste", "Norte"]},
              labels={"qtd": "Procedimentos", "data": "", "regiao": "Região"})
fig.update_layout(height=340, margin=dict(l=0, r=0, t=10, b=0),
                  legend=dict(orientation="h", yanchor="bottom", y=1.02))
st.plotly_chart(fig, width="stretch")

# ============================================================
# ATO 5 — Quem (Distribuição entre municípios)
# ============================================================

st.markdown("## Quem entrega")

df_mun = municipios_dist(data_ini, data_fim, ufs_t, sg_t, "qtd")

col_box, col_top = st.columns([1, 1])

with col_box:
    df_box = df_mun[df_mun["total"] > 0].copy()
    # Ordenar UFs por mediana decrescente
    ord_uf = df_box.groupby("uf")["total"].median().sort_values(ascending=False).index.tolist()
    fig = px.box(df_box, x="uf", y="total", color="uf",
                 category_orders={"uf": ord_uf}, points=False, log_y=True,
                 color_discrete_sequence=px.colors.qualitative.Pastel,
                 labels={"total": "Procedimentos (log)", "uf": ""})
    fig.update_layout(height=440, showlegend=False, margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig, width="stretch")

with col_top:
    df_top = df_mun.nlargest(15, "total").copy()
    df_top["label"] = df_top["municipio"] + " — " + df_top["uf"]
    fig = px.bar(
        df_top, x="total", y="label", orientation="h",
        color="total", color_continuous_scale="Blues",
        labels={"total": "Procedimentos", "label": ""},
    )
    fig.update_layout(
        yaxis={"categoryorder": "total ascending"},
        height=440, margin=dict(l=0, r=0, t=10, b=0),
        coloraxis_showscale=False,
    )
    st.plotly_chart(fig, width="stretch")

# ============================================================
# ATO 6 — Quem cresce / Quem cai
# ============================================================

st.markdown("## Quem cresce, quem cai")
st.caption("Variação % entre a primeira e a segunda metade do período selecionado.")

col_uf_var, col_sg_var = st.columns([1, 1])

with col_uf_var:
    df_var = crescimento_yoy_uf(data_ini, data_fim, ufs_t, sg_t)
    if not df_var.empty:
        df_var = df_var.sort_values("var_pct")
        fig = px.bar(df_var, x="var_pct", y="uf", orientation="h",
                     color="var_pct", color_continuous_scale="RdYlGn",
                     color_continuous_midpoint=0,
                     labels={"var_pct": "Variação (%)", "uf": ""})
        fig.update_layout(height=520, margin=dict(l=0, r=0, t=10, b=0),
                          coloraxis_showscale=False,
                          xaxis=dict(ticksuffix="%"))
        st.plotly_chart(fig, width="stretch")

with col_sg_var:
    df_sg_var = crescimento_subgrupos(data_ini, data_fim, ufs_t)
    if not df_sg_var.empty:
        top = df_sg_var.head(8)
        bot = df_sg_var.tail(8)
        df_pick = pd.concat([top, bot]).sort_values("var_pct")
        fig = px.bar(df_pick, x="var_pct", y="label", orientation="h",
                     color="var_pct", color_continuous_scale="RdYlGn",
                     color_continuous_midpoint=0,
                     labels={"var_pct": "Variação (%)", "label": ""})
        fig.update_layout(height=520, margin=dict(l=0, r=0, t=10, b=0),
                          coloraxis_showscale=False,
                          xaxis=dict(ticksuffix="%"))
        st.plotly_chart(fig, width="stretch")

# ============================================================
# ATO 7 — Atenção (Anomalias)
# ============================================================

st.markdown("## Sinais de atenção")
st.caption("Meses onde a produção fugiu mais de 2,5σ da média histórica do par UF × subgrupo.")

df_an = anomalias(data_ini, data_fim, ufs_t, "qtd", 2.5)

if df_an.empty:
    st.info("Nenhuma anomalia significativa.")
else:
    df_view = df_an[["mes", "uf", "nome", "total", "mu", "z"]].copy()
    df_view["nome"] = df_an["codigo"] + " " + df_an["nome"].fillna("").str.slice(0, 50)
    df_view["razao"] = (df_view["total"] / df_view["mu"]).round(1)
    df_view = df_view.rename(columns={
        "mes": "Mês", "uf": "UF", "nome": "Subgrupo",
        "total": "Total", "mu": "Média", "z": "Z-score", "razao": "× média",
    })
    df_view["Total"] = df_view["Total"].apply(lambda v: fmt_int(v))
    df_view["Média"] = df_view["Média"].apply(lambda v: fmt_int(v))
    df_view["Z-score"] = df_view["Z-score"].round(1)
    st.dataframe(df_view, width="stretch", hide_index=True,
                 column_config={
                     "× média": st.column_config.NumberColumn(format="%.1fx"),
                     "Z-score": st.column_config.ProgressColumn(format="%.1f", min_value=0, max_value=6),
                 })

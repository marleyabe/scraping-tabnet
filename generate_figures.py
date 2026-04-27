"""Gera PNGs estáticos para o relatório técnico."""

from pathlib import Path
import json
import duckdb
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

OUT = Path("CIA046 - Template para Relatório Técnico (2026)/Figuras")
OUT.mkdir(parents=True, exist_ok=True)

UF_NOMES = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL",
    "28": "SE", "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
    "41": "PR", "42": "SC", "43": "RS", "50": "MS", "51": "MT", "52": "GO", "53": "DF",
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

con = duckdb.connect()
con.execute("CREATE VIEW prod AS SELECT * FROM read_parquet('producao.parquet')")
con.execute("CREATE VIEW sg AS SELECT * FROM read_parquet('subgrupos.parquet')")
codes = [r[0] for r in con.execute("SELECT codigo FROM sg ORDER BY codigo").fetchall()]
sq = " + ".join(f'"qtd_{c}"' for c in codes)
sv = " + ".join(f'"val_{c}"' for c in codes)


def save(fig, name, w=1100, h=620):
    fig.update_layout(font=dict(family="Latin Modern Roman, serif", size=14),
                      paper_bgcolor="white", plot_bgcolor="white")
    fig.write_image(str(OUT / f"{name}.png"), width=w, height=h, scale=2)
    print(f"  saved {name}.png")


# === Mapa coroplético ===
df_uf = con.execute(f"""
    SELECT uf_codigo, SUM({sq}) AS qtd, SUM({sv}) AS valor
    FROM prod GROUP BY uf_codigo ORDER BY qtd DESC
""").df()
df_uf["uf"] = df_uf["uf_codigo"].map(UF_NOMES)
df_uf["regiao"] = df_uf["uf"].map(UF_REGIAO)
df_uf["custo_medio"] = df_uf["valor"] / df_uf["qtd"]

with open("br_states.geojson") as f:
    geo = json.load(f)

fig = px.choropleth(df_uf, geojson=geo, locations="uf",
                    featureidkey="properties.sigla",
                    color="qtd", color_continuous_scale="Blues",
                    labels={"qtd": "Procedimentos"})
fig.update_geos(fitbounds="locations", visible=False)
fig.update_layout(margin=dict(l=0, r=0, t=10, b=0))
save(fig, "mapa_brasil", w=900, h=700)

# === Bar UF ===
fig = px.bar(df_uf.head(15), x="qtd", y="uf", orientation="h", color="regiao",
             color_discrete_map=REGIAO_COLOR,
             labels={"qtd": "Procedimentos", "uf": "", "regiao": "Região"})
fig.update_layout(yaxis={"categoryorder": "total ascending"},
                  margin=dict(l=0, r=0, t=10, b=0),
                  legend=dict(orientation="h", yanchor="top", y=-0.05))
save(fig, "bar_uf_qtd")

# === Custo médio por UF ===
df_cm = df_uf.sort_values("custo_medio", ascending=False)
fig = px.bar(df_cm, x="uf", y="custo_medio", color="regiao",
             color_discrete_map=REGIAO_COLOR,
             labels={"custo_medio": "R$ por procedimento", "uf": "", "regiao": "Região"})
fig.update_layout(margin=dict(l=0, r=0, t=10, b=0),
                  xaxis={"categoryorder": "total descending"},
                  legend=dict(orientation="h", yanchor="top", y=-0.15))
save(fig, "custo_medio_uf")

# === Treemap subgrupos ===
df_sg = con.execute(f"""
    WITH base AS (SELECT * FROM prod),
         q AS (SELECT SUBSTR(name, 5) AS codigo, SUM(value) AS qtd
               FROM (UNPIVOT base ON {", ".join(f'"qtd_{c}"' for c in codes)}) GROUP BY codigo),
         v AS (SELECT SUBSTR(name, 5) AS codigo, SUM(value) AS valor
               FROM (UNPIVOT base ON {", ".join(f'"val_{c}"' for c in codes)}) GROUP BY codigo)
    SELECT q.codigo, q.qtd, v.valor,
           CASE WHEN q.qtd > 0 THEN v.valor / q.qtd ELSE 0 END AS custo_medio
    FROM q JOIN v USING (codigo) ORDER BY qtd DESC
""").df()
sgs = con.execute("SELECT codigo, nome FROM sg").df()
df_sg = df_sg.merge(sgs, on="codigo", how="left")
df_sg["label"] = df_sg["codigo"] + " " + df_sg["nome"].fillna("").str.slice(0, 36)
df_tm = df_sg.head(30).copy()
df_tm["log_custo"] = np.log10(df_tm["custo_medio"].clip(lower=0.01))
fig = px.treemap(df_tm, path=[px.Constant("Total"), "label"], values="qtd",
                 color="log_custo", color_continuous_scale="YlOrRd")
fig.update_layout(margin=dict(l=0, r=0, t=10, b=0),
                  coloraxis_colorbar=dict(title="R$/proc.",
                                          tickvals=[-1, 0, 1, 2, 3],
                                          ticktext=["0,1", "1", "10", "100", "1k"]))
save(fig, "treemap_subgrupos", w=1200, h=720)

# === Pareto ===
df_par = df_sg.sort_values("qtd", ascending=False).reset_index(drop=True).copy()
df_par["acum_pct"] = df_par["qtd"].cumsum() / df_par["qtd"].sum() * 100
n_80 = int((df_par["acum_pct"] <= 80).sum()) + 1
top_par = df_par.head(20).copy()
top_par["x"] = top_par["codigo"] + " " + top_par["nome"].fillna("").str.slice(0, 18)
fig = go.Figure()
fig.add_trace(go.Bar(x=top_par["x"], y=top_par["qtd"], name="Procedimentos",
                     marker_color="#2E86AB", yaxis="y1"))
fig.add_trace(go.Scatter(x=top_par["x"], y=top_par["acum_pct"], name="% acumulado",
                         mode="lines+markers", line=dict(color="#C73E1D", width=2),
                         yaxis="y2"))
fig.add_hline(y=80, line=dict(color="#888", dash="dash"), yref="y2",
              annotation_text=f"80% — {n_80} subgrupos", annotation_position="top right")
fig.update_layout(margin=dict(l=0, r=0, t=10, b=140),
                  yaxis=dict(title="Procedimentos"),
                  yaxis2=dict(title="% acumulado", overlaying="y", side="right", range=[0, 105]),
                  legend=dict(orientation="h", yanchor="bottom", y=1.05),
                  xaxis=dict(title="", type="category", tickangle=-45,
                             categoryorder="array", categoryarray=top_par["x"].tolist()))
save(fig, "pareto", w=1300, h=720)

# === Série mensal ===
df_serie = con.execute(f"""
    SELECT data, SUM({sq}) AS qtd, SUM({sv}) AS valor
    FROM prod GROUP BY data ORDER BY data
""").df()
fig = go.Figure()
fig.add_trace(go.Scatter(x=df_serie["data"], y=df_serie["qtd"], name="Procedimentos",
                         mode="lines+markers", line=dict(color="#2E86AB", width=3),
                         fill="tozeroy", fillcolor="rgba(46,134,171,0.15)"))
fig.add_trace(go.Scatter(x=df_serie["data"], y=df_serie["valor"], name="Valor (R$)",
                         mode="lines+markers", line=dict(color="#C73E1D", width=2, dash="dot"),
                         yaxis="y2"))
idx_max = df_serie["qtd"].idxmax()
idx_min = df_serie["qtd"].idxmin()
for idx, label, ay in [(idx_max, "pico", -40), (idx_min, "vale", 40)]:
    row = df_serie.loc[idx]
    fig.add_annotation(x=row["data"], y=row["qtd"], text=f"{label}<br>{row['data']:%m/%Y}",
                       showarrow=True, arrowhead=2, ax=0, ay=ay,
                       font=dict(size=11), bgcolor="white", bordercolor="#aaa")
fig.update_layout(margin=dict(l=0, r=0, t=20, b=0),
                  xaxis=dict(title=""), yaxis=dict(title="Procedimentos"),
                  yaxis2=dict(title="Valor (R$)", overlaying="y", side="right"),
                  legend=dict(orientation="h", yanchor="bottom", y=1.02))
save(fig, "serie_temporal")

# === Stacked area regional ===
df_reg = con.execute(f"""
    SELECT data, uf_codigo, SUM({sq}) AS qtd FROM prod GROUP BY data, uf_codigo
""").df()
df_reg["uf"] = df_reg["uf_codigo"].map(UF_NOMES)
df_reg["regiao"] = df_reg["uf"].map(UF_REGIAO)
df_reg = df_reg.groupby(["data", "regiao"], as_index=False)["qtd"].sum()
fig = px.area(df_reg, x="data", y="qtd", color="regiao",
              color_discrete_map=REGIAO_COLOR,
              category_orders={"regiao": ["Sudeste", "Nordeste", "Sul", "Centro-Oeste", "Norte"]},
              labels={"qtd": "Procedimentos", "data": "", "regiao": "Região"})
fig.update_layout(margin=dict(l=0, r=0, t=10, b=0),
                  legend=dict(orientation="h", yanchor="bottom", y=1.02))
save(fig, "area_regiao")

# === Boxplot por UF ===
df_mun = con.execute(f"""
    SELECT uf_codigo, municipio_codigo, SUM({sq}) AS total
    FROM prod GROUP BY uf_codigo, municipio_codigo
""").df()
df_mun["uf"] = df_mun["uf_codigo"].map(UF_NOMES)
df_box = df_mun[df_mun["total"] > 0]
ord_uf = df_box.groupby("uf")["total"].median().sort_values(ascending=False).index.tolist()
fig = px.box(df_box, x="uf", y="total", color="uf",
             category_orders={"uf": ord_uf}, points=False, log_y=True,
             color_discrete_sequence=px.colors.qualitative.Pastel,
             labels={"total": "Procedimentos (log)", "uf": ""})
fig.update_layout(showlegend=False, margin=dict(l=0, r=0, t=10, b=0))
save(fig, "boxplot_uf")

# === Top municípios ===
df_mun2 = con.execute(f"""
    SELECT uf_codigo, municipio_codigo, municipio_nome, SUM({sq}) AS total
    FROM prod GROUP BY uf_codigo, municipio_codigo, municipio_nome
""").df()
df_mun2["uf"] = df_mun2["uf_codigo"].map(UF_NOMES)
df_mun2["label"] = df_mun2["municipio_nome"].str.title() + " — " + df_mun2["uf"]
df_top = df_mun2.nlargest(15, "total")
fig = px.bar(df_top, x="total", y="label", orientation="h",
             color="total", color_continuous_scale="Blues",
             labels={"total": "Procedimentos", "label": ""})
fig.update_layout(yaxis={"categoryorder": "total ascending"},
                  margin=dict(l=0, r=0, t=10, b=0), coloraxis_showscale=False)
save(fig, "top_municipios")

# === Variação por UF ===
df_v = con.execute(f"""
    SELECT data, uf_codigo, SUM({sq}) AS qtd FROM prod GROUP BY data, uf_codigo
""").df()
df_v["data"] = pd.to_datetime(df_v["data"])
mid = df_v["data"].min() + (df_v["data"].max() - df_v["data"].min()) / 2
p = df_v[df_v["data"] < mid].groupby("uf_codigo")["qtd"].mean()
s = df_v[df_v["data"] >= mid].groupby("uf_codigo")["qtd"].mean()
out = pd.DataFrame({"primeira": p, "segunda": s}).reset_index()
out["var_pct"] = ((out["segunda"] - out["primeira"]) / out["primeira"] * 100).fillna(0)
out["uf"] = out["uf_codigo"].map(UF_NOMES)
out = out.sort_values("var_pct")
fig = px.bar(out, x="var_pct", y="uf", orientation="h",
             color="var_pct", color_continuous_scale="RdYlGn",
             color_continuous_midpoint=0,
             labels={"var_pct": "Variação (%)", "uf": ""})
fig.update_layout(margin=dict(l=0, r=0, t=10, b=0),
                  coloraxis_showscale=False, xaxis=dict(ticksuffix="%"))
save(fig, "var_uf", w=900, h=720)

# === Variação por subgrupo ===
df_sgv = con.execute(f"""
    WITH base AS (SELECT * FROM prod),
         m AS (SELECT data, SUBSTR(name, 5) AS codigo, SUM(value) AS qtd
               FROM (UNPIVOT base ON {", ".join(f'"qtd_{c}"' for c in codes)}) GROUP BY data, codigo)
    SELECT * FROM m
""").df()
df_sgv["data"] = pd.to_datetime(df_sgv["data"])
mid = df_sgv["data"].min() + (df_sgv["data"].max() - df_sgv["data"].min()) / 2
p = df_sgv[df_sgv["data"] < mid].groupby("codigo")["qtd"].mean()
s = df_sgv[df_sgv["data"] >= mid].groupby("codigo")["qtd"].mean()
out = pd.DataFrame({"primeira": p, "segunda": s}).reset_index()
out = out[out["primeira"] >= 1000]
out["var_pct"] = ((out["segunda"] - out["primeira"]) / out["primeira"] * 100).fillna(0)
out = out.merge(sgs, on="codigo", how="left")
out["label"] = out["codigo"] + " " + out["nome"].fillna("").str.slice(0, 28)
out = out.sort_values("var_pct", ascending=False)
df_pick = pd.concat([out.head(8), out.tail(8)]).sort_values("var_pct")
fig = px.bar(df_pick, x="var_pct", y="label", orientation="h",
             color="var_pct", color_continuous_scale="RdYlGn",
             color_continuous_midpoint=0,
             labels={"var_pct": "Variação (%)", "label": ""})
fig.update_layout(margin=dict(l=0, r=0, t=10, b=0),
                  coloraxis_showscale=False, xaxis=dict(ticksuffix="%"))
save(fig, "var_subgrupo", w=1100, h=620)

# === KPI image ===
total_q = df_serie["qtd"].sum()
total_v = df_serie["valor"].sum()
n_munis = con.execute("SELECT COUNT(DISTINCT municipio_codigo) FROM prod").fetchone()[0]
n_per = con.execute("SELECT COUNT(DISTINCT periodo) FROM prod").fetchone()[0]
custo = total_v / total_q
print(f"\nResumo geral:")
print(f"  Procedimentos: {total_q:,.0f}")
print(f"  Valor pago:    R$ {total_v:,.2f}")
print(f"  Municípios:    {n_munis:,}")
print(f"  Períodos:      {n_per}")
print(f"  Custo médio:   R$ {custo:.2f}")
print("\nDONE")

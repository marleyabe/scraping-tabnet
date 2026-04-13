"""Dashboard Streamlit — Produção Ambulatorial SUS (TabNet/DATASUS).

Uso:
    uv run streamlit run src/scraping_tabnet/dashboard.py
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from scraping_tabnet.subgrupos import (
    MESES_PT,
    SUBGRUPO_NOMES,
    UF_BY_IBGE_PREFIX,
    subgrupo_label,
)

CSV_PATH = Path(__file__).resolve().parents[2] / "dados_tratados.csv"


# ---------------------------------------------------------------- loaders ---
@st.cache_data(show_spinner="Lendo e tratando dados_tratados.csv...")
def load_wide() -> pd.DataFrame:
    df = pd.read_csv(CSV_PATH, sep=";", dtype=str)
    df.columns = [c.strip() for c in df.columns]

    # município → ibge / nome / uf
    muni = df["municipio"].str.extract(r"^(\d{6})\s+(.*)$")
    df["ibge_code"] = muni[0]
    df["municipio_nome"] = muni[1].str.strip()
    df["uf"] = df["ibge_code"].str[:2].map(UF_BY_IBGE_PREFIX)

    # período → datetime
    per = df["periodo"].str.extract(r"^(\w{3})/(\d{4})$")
    df["data"] = pd.to_datetime(
        {
            "year": per[1].astype(int),
            "month": per[0].map(MESES_PT),
            "day": 1,
        },
        errors="coerce",
    )
    df["ano"] = df["data"].dt.year

    qtd_cols = [c for c in df.columns if c.startswith("qtd_")]
    val_cols = [c for c in df.columns if c.startswith("val_")]

    # "-" → NaN; números BR → float
    for c in qtd_cols:
        df[c] = pd.to_numeric(df[c].replace("-", pd.NA), errors="coerce").astype("Float64")
    for c in val_cols:
        s = df[c].replace("-", pd.NA)
        s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
        df[c] = pd.to_numeric(s, errors="coerce")

    df["qtd_total"] = df[qtd_cols].sum(axis=1, numeric_only=True, min_count=1)
    df["val_total"] = df[val_cols].sum(axis=1, numeric_only=True, min_count=1)
    return df


@st.cache_data(show_spinner="Montando tabela longa...")
def load_long() -> pd.DataFrame:
    df = load_wide()
    qtd_cols = [c for c in df.columns if c.startswith("qtd_")]
    keys = ["periodo", "data", "ano", "municipio", "municipio_nome", "ibge_code", "uf"]

    qtd_long = df[keys + qtd_cols].melt(id_vars=keys, var_name="col", value_name="qtd")
    qtd_long["subgrupo"] = qtd_long["col"].str[4:]
    qtd_long = qtd_long.drop(columns="col")

    val_cols = [c for c in df.columns if c.startswith("val_")]
    val_long = df[keys + val_cols].melt(id_vars=keys, var_name="col", value_name="val")
    val_long["subgrupo"] = val_long["col"].str[4:]
    val_long = val_long.drop(columns="col")

    long = qtd_long.merge(val_long, on=keys + ["subgrupo"], how="outer")
    long["subgrupo_nome"] = long["subgrupo"].map(SUBGRUPO_NOMES).fillna(long["subgrupo"])
    long = long.dropna(subset=["qtd", "val"], how="all")
    return long


# ------------------------------------------------------------- formatters ---
def fmt_brl(v: float) -> str:
    if pd.isna(v):
        return "—"
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_int(v: float) -> str:
    if pd.isna(v):
        return "—"
    return f"{int(v):,}".replace(",", ".")


# ------------------------------------------------------------------- app ---
def main() -> None:
    st.set_page_config(
        page_title="Produção Ambulatorial SUS",
        page_icon="🏥",
        layout="wide",
    )
    st.title("Produção Ambulatorial SUS — TabNet/DATASUS")
    st.caption(
        "Dados aprovados de produção ambulatorial por município e subgrupo SIGTAP. "
        f"Fonte: `{CSV_PATH.name}`."
    )

    wide = load_wide()
    long = load_long()

    # --- Visão geral -------------------------------------------------------
    st.header("1. Visão geral")
    if True:
        qtd_tot = wide["qtd_total"].sum()
        val_tot = wide["val_total"].sum()
        municipios = wide["municipio"].nunique()
        meses = wide["periodo"].nunique()
        ticket = val_tot / qtd_tot if qtd_tot else float("nan")

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Procedimentos aprovados", fmt_int(qtd_tot))
        c2.metric("Valor aprovado", fmt_brl(val_tot))
        c3.metric("Municípios", fmt_int(municipios))
        c4.metric("Meses cobertos", fmt_int(meses))
        c5.metric("Ticket médio", fmt_brl(ticket))

        st.subheader("Evolução mensal")
        serie = (
            wide.groupby("data", as_index=False)[["qtd_total", "val_total"]].sum().sort_values("data")
        )
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(x=serie["data"], y=serie["qtd_total"], name="Qtd", yaxis="y1")
        )
        fig.add_trace(
            go.Scatter(x=serie["data"], y=serie["val_total"], name="Valor (R$)", yaxis="y2")
        )
        fig.update_layout(
            yaxis=dict(title="Qtd aprovada"),
            yaxis2=dict(title="Valor (R$)", overlaying="y", side="right"),
            hovermode="x unified",
            height=420,
            legend=dict(orientation="h"),
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # --- Subgrupos ---------------------------------------------------------
    st.header("2. Subgrupos SIGTAP")
    if True:
        st.subheader("Ranking de subgrupos SIGTAP")
        metrica = st.radio(
            "Métrica", ["Valor (R$)", "Quantidade"], horizontal=True, key="sub_metrica"
        )
        col = "val" if metrica.startswith("Valor") else "qtd"

        agg = (
            long.groupby(["subgrupo", "subgrupo_nome"], as_index=False)[["qtd", "val"]]
            .sum(numeric_only=True)
            .sort_values(col, ascending=False)
        )
        agg["label"] = agg["subgrupo"] + " — " + agg["subgrupo_nome"]
        top_n = st.slider("Top N", 5, 50, 15)
        fig = px.bar(
            agg.head(top_n).iloc[::-1],
            x=col,
            y="label",
            orientation="h",
            labels={col: metrica, "label": "Subgrupo"},
            height=500,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Custo médio por procedimento (val/qtd)")
        agg["custo_medio"] = agg["val"] / agg["qtd"].replace(0, pd.NA)
        st.dataframe(
            agg[["subgrupo", "subgrupo_nome", "qtd", "val", "custo_medio"]]
            .rename(columns={
                "subgrupo_nome": "nome", "qtd": "quantidade",
                "val": "valor (R$)", "custo_medio": "custo médio (R$)",
            })
            .sort_values("custo médio (R$)", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    # --- Geografia ---------------------------------------------------------
    st.header("3. Geografia")
    if True:
        st.subheader("Valor aprovado por UF")
        uf_agg = (
            long.groupby("uf", as_index=False)[["qtd", "val"]].sum(numeric_only=True)
            .dropna(subset=["uf"]).sort_values("val", ascending=False)
        )
        fig = px.bar(uf_agg, x="uf", y="val", labels={"val": "Valor (R$)", "uf": "UF"}, height=380)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Top municípios")
        sub_opts = ["(todos)"] + [subgrupo_label(s) for s in sorted(SUBGRUPO_NOMES)]
        escolha = st.selectbox("Filtrar por subgrupo", sub_opts, key="geo_sub")
        if escolha == "(todos)":
            muni_df = (
                wide.groupby(["municipio", "uf"], as_index=False)[["qtd_total", "val_total"]]
                .sum(numeric_only=True)
                .rename(columns={"qtd_total": "qtd", "val_total": "val"})
            )
        else:
            code = escolha.split(" — ")[0]
            muni_df = (
                long[long["subgrupo"] == code]
                .groupby(["municipio", "uf"], as_index=False)[["qtd", "val"]]
                .sum(numeric_only=True)
            )
        top_m = muni_df.sort_values("val", ascending=False).head(20)
        fig = px.bar(
            top_m.iloc[::-1], x="val", y="municipio", orientation="h",
            color="uf", labels={"val": "Valor (R$)", "municipio": ""}, height=520,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Cobertura — municípios com o subgrupo selecionado")
        if escolha != "(todos)":
            code = escolha.split(" — ")[0]
            com = (long[(long["subgrupo"] == code) & (long["qtd"].fillna(0) > 0)]
                   ["municipio"].nunique())
            total = wide["municipio"].nunique()
            pct = com / total * 100 if total else 0
            c1, c2, c3 = st.columns(3)
            c1.metric("Municípios com registro", fmt_int(com))
            c2.metric("Total de municípios", fmt_int(total))
            c3.metric("Cobertura", f"{pct:.1f}%")
        else:
            st.caption("Selecione um subgrupo para ver cobertura.")

    st.divider()

    # --- Série temporal por subgrupo --------------------------------------
    st.header("4. Série temporal por subgrupo")
    if True:
        default = ["0301", "0202", "0204"]
        sel = st.multiselect(
            "Subgrupos", sorted(SUBGRUPO_NOMES),
            default=[c for c in default if c in SUBGRUPO_NOMES],
            format_func=subgrupo_label,
        )
        if sel:
            sub = long[long["subgrupo"].isin(sel)]
            ts = sub.groupby(["data", "subgrupo", "subgrupo_nome"], as_index=False)[
                ["qtd", "val"]
            ].sum(numeric_only=True)
            ts["label"] = ts["subgrupo"] + " — " + ts["subgrupo_nome"]
            met = st.radio("Métrica", ["Valor (R$)", "Quantidade"], horizontal=True, key="ts_m")
            y = "val" if met.startswith("Valor") else "qtd"
            fig = px.line(ts, x="data", y=y, color="label", markers=True, height=450,
                          labels={y: met, "data": "Mês"})
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("Variação ano-a-ano (média mensal)")
            yoy = (
                sub.groupby(["subgrupo", "subgrupo_nome", "ano"], as_index=False)["val"]
                .mean().pivot(index=["subgrupo", "subgrupo_nome"], columns="ano", values="val")
                .reset_index()
            )
            anos = [c for c in yoy.columns if isinstance(c, (int,))]
            if 2024 in anos and 2025 in anos:
                yoy["Δ% 2025 vs 2024"] = (yoy[2025] / yoy[2024] - 1) * 100
            st.dataframe(yoy, use_container_width=True, hide_index=True)
        else:
            st.info("Selecione ao menos um subgrupo.")

    st.divider()

    # --- Comparador de municípios -----------------------------------------
    st.header("5. Comparador de municípios")
    if True:
        opcoes = sorted(wide["municipio"].dropna().unique())
        sel_m = st.multiselect("Municípios", opcoes, max_selections=8)
        if sel_m:
            sub = wide[wide["municipio"].isin(sel_m)]
            ts = sub.groupby(["data", "municipio"], as_index=False)[["qtd_total", "val_total"]].sum()
            met = st.radio("Métrica", ["Valor (R$)", "Quantidade"], horizontal=True, key="cm_m")
            y = "val_total" if met.startswith("Valor") else "qtd_total"
            fig = px.line(ts, x="data", y=y, color="municipio", markers=True, height=450,
                          labels={y: met, "data": "Mês"})
            st.plotly_chart(fig, use_container_width=True)

            # subgrupo dominante por município
            long_sel = long[long["municipio"].isin(sel_m)]
            dom = (
                long_sel.groupby(["municipio", "subgrupo", "subgrupo_nome"], as_index=False)["val"]
                .sum().sort_values("val", ascending=False)
                .drop_duplicates("municipio")
                .rename(columns={"subgrupo_nome": "subgrupo_dominante"})
            )
            resumo = sub.groupby("municipio", as_index=False)[["qtd_total", "val_total"]].sum()
            resumo["ticket_medio"] = resumo["val_total"] / resumo["qtd_total"]
            resumo = resumo.merge(dom[["municipio", "subgrupo_dominante"]], on="municipio", how="left")
            st.dataframe(resumo, use_container_width=True, hide_index=True)
        else:
            st.info("Selecione ao menos um município.")

    st.divider()

    # --- Explorador --------------------------------------------------------
    st.header("6. Explorador de dados")
    if True:
        c1, c2, c3 = st.columns(3)
        per_sel = c1.multiselect("Período", sorted(wide["periodo"].unique()))
        uf_sel = c2.multiselect("UF", sorted(wide["uf"].dropna().unique()))
        sub_sel = c3.multiselect(
            "Subgrupo", sorted(SUBGRUPO_NOMES), format_func=subgrupo_label
        )
        f = long.copy()
        if per_sel:
            f = f[f["periodo"].isin(per_sel)]
        if uf_sel:
            f = f[f["uf"].isin(uf_sel)]
        if sub_sel:
            f = f[f["subgrupo"].isin(sub_sel)]
        st.caption(f"{len(f):,} linhas após filtros")
        st.dataframe(f.head(5000), use_container_width=True, hide_index=True)
        st.download_button(
            "📥 Baixar recorte CSV",
            data=f.to_csv(index=False, sep=";").encode("utf-8"),
            file_name="recorte.csv",
            mime="text/csv",
        )


if __name__ == "__main__":
    main()

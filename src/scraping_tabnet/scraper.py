import csv
import io
import time
from pathlib import Path

from bs4 import BeautifulSoup
from playwright.sync_api import Page, sync_playwright

URL = "https://tabnet.datasus.gov.br/cgi/deftohtm.exe?sia/cnv/qabr.def"
OUTPUT_FILE = "output.csv"
DELAY_BETWEEN_REQUESTS = 10  # segundos entre cada query

MONTHS_PT = [
    "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
    "Jul", "Ago", "Set", "Out", "Nov", "Dez",
]

CONTENTS = [
    ("Qtd.aprovada", "Qtd.aprovada"),
    ("Valor_aprovado", "Valor_aprovado"),
]


def generate_periods() -> list[tuple[str, str]]:
    """Retorna lista de (label, filename) para Jan/2024 a Jan/2026 (25 meses)."""
    periods = []
    for month in range(1, 13):
        label = f"{MONTHS_PT[month - 1]}/2024"
        filename = f"qabr24{month:02d}.dbf"
        periods.append((label, filename))
    for month in range(1, 13):
        label = f"{MONTHS_PT[month - 1]}/2025"
        filename = f"qabr25{month:02d}.dbf"
        periods.append((label, filename))
    periods.append(("Jan/2026", "qabr2601.dbf"))
    return periods  # 25 itens


def deselect_all(page: Page, selector: str) -> None:
    """Desseleciona todas as opções de um <select multiple>."""
    page.locator(selector).evaluate(
        "el => { for (const opt of el.options) opt.selected = false; }"
    )


def setup_fixed_fields(page: Page) -> None:
    """Define linha, coluna e checkbox — campos que não mudam entre iterações."""
    page.select_option("select[name='Linha']", value="Município")
    page.select_option("select[name='Coluna']", value="Subgrupo_proced.")

    # Colunas separadas por ";"
    page.locator("input[name='formato'][value='prn']").check()

    checkbox = page.locator("input[name='zeradas']")
    if not checkbox.is_checked():
        checkbox.check()


def select_and_submit(page: Page, content_value: str, period_filename: str) -> Page:
    """Desseleciona tudo, seleciona 1 conteúdo + 1 período e submete.
    Retorna a popup assim que aberta — o chamador é responsável por fechá-la."""
    deselect_all(page, "select[name='Incremento']")
    page.select_option("select[name='Incremento']", value=content_value)

    deselect_all(page, "select[name='Arquivos']")
    page.select_option("select[name='Arquivos']", value=period_filename)

    with page.expect_popup() as popup_info:
        page.click("input[name='mostre']")

    return popup_info.value


def extract_pre(popup_page: Page) -> list[list[str]]:
    """Extrai e parseia o conteúdo do <pre> separado por ';'."""
    html = popup_page.content()
    soup = BeautifulSoup(html, "lxml")

    pre = soup.find("pre")
    if pre is None:
        raise RuntimeError("Elemento <pre> não encontrado na página de resultado.")

    text = pre.get_text()

    rows: list[list[str]] = []
    reader = csv.reader(io.StringIO(text), delimiter=";")
    for row in reader:
        # Ignora linhas vazias e a linha com apenas "&"
        if not row or all(cell.strip() in ("", "&") for cell in row):
            continue
        rows.append(row)

    return rows


def main() -> None:
    periods = generate_periods()
    total = len(periods) * len(CONTENTS)
    print(f"Total de queries: {total} ({len(periods)} meses × {len(CONTENTS)} conteúdos)")

    output_path = Path(OUTPUT_FILE)
    header_written = False
    step = 0

    with (
        sync_playwright() as p,
        output_path.open("w", newline="", encoding="utf-8") as f,
    ):
        writer = csv.writer(f, delimiter=";")

        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        print(f"Acessando {URL}")
        page.goto(URL, wait_until="domcontentloaded", timeout=60_000)

        print("Configurando campos fixos (linha, coluna, formato, checkbox)...")
        setup_fixed_fields(page)

        for content_label, content_value in CONTENTS:
            for period_label, period_filename in periods:
                step += 1
                print(f"[{step:02d}/{total}] {content_label} | {period_label} ({period_filename})")

                popup_page = None
                try:
                    popup_page = select_and_submit(page, content_value, period_filename)
                    popup_page.wait_for_load_state("domcontentloaded", timeout=60_000)
                    popup_page.wait_for_selector("pre", timeout=120_000)

                    rows = extract_pre(popup_page)

                    if rows:
                        header = rows[0]
                        data_rows = rows[1:]

                        if not header_written:
                            writer.writerow(["Período", "Conteúdo"] + header)
                            header_written = True

                        for row in data_rows:
                            writer.writerow([period_label, content_label] + row)

                        f.flush()
                        print(f"  -> {len(data_rows)} linhas gravadas em disco")

                except Exception as e:
                    print(f"  -> ERRO: {e}")

                finally:
                    if popup_page is not None:
                        try:
                            popup_page.close()
                        except Exception:
                            pass

                time.sleep(DELAY_BETWEEN_REQUESTS)

        browser.close()

    print(f"\nConcluído! Salvo em: {output_path.resolve()}")


if __name__ == "__main__":
    main()

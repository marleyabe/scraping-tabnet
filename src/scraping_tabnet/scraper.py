import csv
import io
import re
import threading
import time
from pathlib import Path

from bs4 import BeautifulSoup
from playwright.sync_api import Page, sync_playwright

URL = "https://tabnet.datasus.gov.br/cgi/deftohtm.exe?sia/cnv/qabr.def"
DATA_DIR = Path("data")
OUTPUT_FILE = Path("output.csv")
DELAY_BETWEEN_REQUESTS = 10  # segundos entre cada query, por worker

MONTHS_PT = [
    "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
    "Jul", "Ago", "Set", "Out", "Nov", "Dez",
]

CONTENTS = [
    ("Qtd.aprovada", "Qtd.aprovada"),
    ("Valor_aprovado", "Valor_aprovado"),
]


def generate_periods(start_month: int, start_year: int, end_month: int, end_year: int) -> list[tuple[str, str]]:
    """Retorna lista de (label, filename) do período informado."""
    periods = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        label = f"{MONTHS_PT[month - 1]}/{year}"
        filename = f"qabr{year % 100:02d}{month:02d}.dbf"
        periods.append((label, filename))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return periods


def ask_period(prompt: str) -> tuple[int, int]:
    """Pede mês/ano ao usuário no formato MM/AAAA."""
    while True:
        raw = input(prompt).strip()
        try:
            month, year = raw.split("/")
            m, y = int(month), int(year)
            if 1 <= m <= 12 and y >= 2000:
                return m, y
        except ValueError:
            pass
        print("Formato inválido. Use MM/AAAA (ex: 01/2024)")


def deselect_all(page: Page, selector: str) -> None:
    page.locator(selector).evaluate(
        "el => { for (const opt of el.options) opt.selected = false; }"
    )


def setup_fixed_fields(page: Page) -> None:
    page.select_option("select[name='Linha']", value="Município")
    page.select_option("select[name='Coluna']", value="Subgrupo_proced.")
    page.locator("input[name='formato'][value='prn']").check()
    checkbox = page.locator("input[name='zeradas']")
    if not checkbox.is_checked():
        checkbox.check()


def select_and_submit(page: Page, content_value: str, period_filename: str) -> Page:
    deselect_all(page, "select[name='Incremento']")
    page.select_option("select[name='Incremento']", value=content_value)

    deselect_all(page, "select[name='Arquivos']")
    page.select_option("select[name='Arquivos']", value=period_filename)

    with page.expect_popup(timeout=90_000) as popup_info:
        page.click("input[name='mostre']")

    return popup_info.value


def extract_pre(popup_page: Page) -> list[list[str]]:
    html = popup_page.content()
    soup = BeautifulSoup(html, "lxml")

    pre = soup.find("pre")
    if pre is None:
        raise RuntimeError("Elemento <pre> não encontrado na página de resultado.")

    text = pre.get_text()

    rows: list[list[str]] = []
    reader = csv.reader(io.StringIO(text), delimiter=";")
    for row in reader:
        if not row or all(cell.strip() in ("", "&") for cell in row):
            continue
        rows.append(row)
    return rows


def _slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_")


def query_csv_path(period_filename: str, content_value: str) -> Path:
    base = period_filename[:-4] if period_filename.endswith(".dbf") else period_filename
    return DATA_DIR / f"{base}__{_slug(content_value)}.csv"


def all_contents_done(period_filename: str) -> bool:
    return all(query_csv_path(period_filename, cv).exists() for _, cv in CONTENTS)


def write_query_csv_atomic(out_path: Path, period_label: str, content_label: str, rows: list[list[str]]) -> int:
    """Grava o CSV da query de forma atômica (tmp + replace). Retorna nº de linhas de dados."""
    if not rows:
        return 0
    header = rows[0]
    data_rows = rows[1:]
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["Período", "Conteúdo"] + header)
        for row in data_rows:
            w.writerow([period_label, content_label] + row)
        f.flush()
    tmp_path.replace(out_path)
    return len(data_rows)


def process_period(page: Page, period: tuple[str, str], name: str) -> None:
    """Roda os CONTENTS de um período. Cada query é gravada imediatamente em arquivo próprio."""
    period_label, period_filename = period
    for content_label, content_value in CONTENTS:
        out_path = query_csv_path(period_filename, content_value)
        if out_path.exists():
            print(f"[{name}] {period_label} | {content_label}: já existe ({out_path.name}), pulando.")
            continue

        popup_page = None
        try:
            popup_page = select_and_submit(page, content_value, period_filename)
            popup_page.wait_for_load_state("domcontentloaded", timeout=60_000)
            popup_page.wait_for_selector("pre", timeout=120_000)

            rows = extract_pre(popup_page)
            n_data = write_query_csv_atomic(out_path, period_label, content_label, rows)
            print(f"[{name}] {period_label} | {content_label}: {n_data} linhas → {out_path.name}")

        except Exception as e:
            print(f"[{name}] ERRO em {period_label} | {content_label}: {e}")

        finally:
            if popup_page is not None:
                try:
                    popup_page.close()
                except Exception:
                    pass

        time.sleep(DELAY_BETWEEN_REQUESTS)


def worker(
    name: str,
    indices: list[int],
    periods: list[tuple[str, str]],
    done: set,
    in_progress: set,
    lock: threading.Lock,
) -> None:
    """Worker independente: cada um com seu próprio playwright/browser/page.
    - `done`: índices já completos (run anterior ou concluídos por outro worker).
    - `in_progress`: índices sendo processados agora por algum worker.
    Encerra quando bate em `in_progress` (convergência); pula `done` e continua.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        try:
            print(f"[{name}] acessando {URL}")
            page.goto(URL, wait_until="domcontentloaded", timeout=60_000)
            setup_fixed_fields(page)

            for idx in indices:
                with lock:
                    if idx in in_progress:
                        print(f"[{name}] convergência em idx {idx}; encerrando.")
                        break
                    if idx in done:
                        continue
                    in_progress.add(idx)

                try:
                    process_period(page, periods[idx], name)
                finally:
                    with lock:
                        in_progress.discard(idx)
                        done.add(idx)
        finally:
            try:
                browser.close()
            except Exception:
                pass
            print(f"[{name}] finalizado.")


def build_worker_plans(n: int) -> list[tuple[str, list[int]]]:
    """Workers. Use NUM_WORKERS env var (default 4) para paralelismo.
    Com 1 worker: sequencial puro. Com 4: convergência asc/desc + mid→start/mid→end."""
    import os
    workers = int(os.environ.get("NUM_WORKERS", "4"))
    if workers <= 1:
        return [("seq", list(range(0, n)))]
    mid = n // 2
    return [
        ("asc",        list(range(0, n))),
        ("mid->start", list(range(mid - 1, -1, -1))),
        ("mid->end",   list(range(mid, n))),
        ("desc",       list(range(n - 1, -1, -1))),
    ]


def merge_to_output(periods: list[tuple[str, str]], output_path: Path) -> None:
    """Mescla os arquivos por-query em um único CSV, na ordem cronológica."""
    written_header = False
    n_files = 0
    with output_path.open("w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out, delimiter=";")
        for _, period_filename in periods:
            for _, content_value in CONTENTS:
                p = query_csv_path(period_filename, content_value)
                if not p.exists():
                    continue
                with p.open("r", newline="", encoding="utf-8") as src:
                    reader = csv.reader(src, delimiter=";")
                    rows = list(reader)
                if not rows:
                    continue
                if not written_header:
                    writer.writerow(rows[0])
                    written_header = True
                for row in rows[1:]:
                    writer.writerow(row)
                n_files += 1
    print(f"Mesclados {n_files} arquivos em: {output_path.resolve()}")


def main() -> None:
    start_month, start_year = ask_period("Data de início (MM/AAAA): ")
    end_month, end_year = ask_period("Data final (MM/AAAA): ")
    periods = generate_periods(start_month, start_year, end_month, end_year)
    n = len(periods)
    if n == 0:
        print("Nenhum período no intervalo informado.")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    total = n * len(CONTENTS)
    print(f"Total de queries: {total} ({n} meses × {len(CONTENTS)} conteúdos) | 4 workers paralelos")
    print(f"Diretório de saída por-query: {DATA_DIR.resolve()}")

    done: set[int] = {i for i, (_, pf) in enumerate(periods) if all_contents_done(pf)}
    in_progress: set[int] = set()
    if done:
        print(f"{len(done)} períodos já completos em runs anteriores; pulando-os.")

    lock = threading.Lock()

    threads = []
    for name, indices in build_worker_plans(n):
        t = threading.Thread(
            target=worker,
            args=(name, indices, periods, done, in_progress, lock),
            daemon=False,
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    merge_to_output(periods, OUTPUT_FILE)
    print("Concluído.")


if __name__ == "__main__":
    main()

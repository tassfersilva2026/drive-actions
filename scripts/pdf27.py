# scripts/pdf27.py
import os, re, glob, json, time, argparse, logging, hashlib
from datetime import datetime
from pathlib import Path

import pdfplumber
import pandas as pd
from tqdm import tqdm
from openpyxl.utils import get_column_letter

# ── CONFIGS ───────────────────────────────────────────────────────────────────
ROOT = Path(".")
PDF_DIR        = str(ROOT / "inbox")   # onde os PDFs baixam
OUT_DIR        = ROOT / "out"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Saídas
MATRIX_XLSX    = str(OUT_DIR / "OFERTASMATRIZ.xlsx")
PARQUET_INC    = str(OUT_DIR / "OFERTASMATRIZ_OFERTAS.parquet")   # incremento
PARQUET_ERR    = str(OUT_DIR / "OFERTASMATRIZ_ERROS.parquet")
MASTER_OUT     = str(OUT_DIR / "OFERTAS.parquet")                 # base-mãe atualizada aqui

# Base-mãe já versionada (prioriza raiz)
MASTER_CANDIDATES = [
    Path("OFERTAS.parquet"),
    Path("data/OFERTAS.parquet"),
    Path("out/OFERTAS.parquet"),
]

SHEET_OFERTAS  = "OFERTAS"
SHEET_ERROS    = "ERRO_MONITORAMENTO"

STATE_JSON     = str(OUT_DIR / "OFERTASMATRIZ_STATE.json")
LOOP_INTERVAL_SEC = 10 * 60
# ──────────────────────────────────────────────────────────────────────────────

logging.getLogger("pdfminer").setLevel(logging.ERROR)

VALID_ENTITIES = {
    "123milhas","agoda","airbnb","aircanada","airfrance","aeromexico","americanairlines",
    "ancoradouro","azul","booking.com","capoviagens","cestarollitravel","confianca",
    "cvc","decolar","esferatur","expedia","flipmilhas","flytourgapnet","gol",
    "googleflights","gotogate","hoteis.com","hurb","iberia","jetsmart","kayak",
    "kissandfly","kiwi.com","latam","lufthansa","maxmilhas","momondo","mrsmrssmith",
    "mytrip","passagenspromo","primetour","queensberryviagens","rexturadvance",
    "sakuratur","skyscanner","submarinoviagens","tap","trendoperadora","traveloka",
    "trip.com","unitedairlines","viajanet","visualturismo","voepass","vrbo","zarpo","zupper"
}
AIRLINES = ["gol","latam","azul","voepass","jetsmart","airfrance","unitedairlines",
            "iberia","lufthansa","aeromexico","aircanada","americanairlines","tap"]

dates_regex   = re.compile(r"(\d{2}/\d{2}/\d{4},\s*\d{2}:\d{2})")
price_regex   = re.compile(r"R\$\s*([\d\s\.,]+)")
CUTOFF_OFFERS = "complemente sua viagem"
TIMES_CUTOFF  = "verificando preços e disponibilidade"
FIRST_PAGE_ERROR_RULES = {
    re.compile(r"as melhores ofertas e promoções", re.IGNORECASE):  "ERRO DE PAGINA",
    re.compile(r"destinos nacionais mais buscados", re.IGNORECASE):  "ERRO DE PAGINA",
    re.compile(r"encaminhando para o website soli", re.IGNORECASE):  "ERRO DE PAGINA",
    re.compile(r"passagens aéreas em promoção\s*\|\s*l", re.IGNORECASE): "ERRO DE PAGINA",
    re.compile(r"skyscanner\s+você\s+é\s+uma\s+pessoa\s+ou", re.IGNORECASE): "ERRO ANTIBOT",
}
PAGE_ERROR_PATTERNS = {
    re.compile(r"passagens aéreas.*hotéis.*aluguel de carros", re.IGNORECASE): "ErroPaginaInicial",
    re.compile(r"pacotes de viagens", re.IGNORECASE): "ErroPaginaDecolar"
}
MONTH_MAP = {"jan":1,"fev":2,"mar":3,"abr":4,"mai":5,"jun":6,"jul":7,"ago":8,"set":9,"out":10,"nov":11,"dez":12}

# ── Chave de identidade para deduplicação ─────────────────────────────────────
OF_ID_COLS = [
    "Nome do Arquivo","Companhia Aérea","Horário1","Horário2","Horário3",
    "Tipo de Voo","Data do Voo","Data/Hora da Busca","Agência/Companhia",
    "Preço","TRECHO","ADVP"
]

def to_upper_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    return df.applymap(lambda x: x.upper() if isinstance(x, str) else x)

def _fmt_date_series_ddmmyyyy(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, dayfirst=True, errors="coerce").dt.strftime("%d/%m/%Y").fillna("")

def _fmt_price_series_str(s: pd.Series) -> pd.Series:
    s2 = pd.to_numeric(s, errors="coerce").round(2)
    return s2.map(lambda x: f"{x:.2f}" if pd.notna(x) else "").astype(str)

def _canon_ofertas(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: 
        return pd.DataFrame(columns=OF_ID_COLS)
    c = df.copy()
    for col in ["Nome do Arquivo","Companhia Aérea","Horário1","Horário2","Horário3",
                "Tipo de Voo","Agência/Companhia","TRECHO"]:
        if col in c: c[col] = c[col].astype(str).str.strip().str.upper()
    if "Data do Voo" in c:            c["Data do Voo"] = _fmt_date_series_ddmmyyyy(c["Data do Voo"])
    if "Data/Hora da Busca" in c:
        so_data = c["Data/Hora da Busca"].astype(str).str.extract(r"(\d{2}/\d{2}/\d{4})", expand=False)
        c["Data/Hora da Busca"] = _fmt_date_series_ddmmyyyy(so_data)
    if "Preço" in c:                  c["Preço"] = _fmt_price_series_str(c["Preço"])
    if "ADVP" in c:                   c["ADVP"] = pd.to_numeric(c["ADVP"], errors="coerce").fillna(0).astype(int).astype(str)
    for col in OF_ID_COLS:
        if col not in c: c[col] = ""
        c[col] = c[col].fillna("")
    return c[OF_ID_COLS]

def _hash_concat(df: pd.DataFrame, cols: list) -> pd.Series:
    s = df[cols[0]].astype(str)
    for col in cols[1:]:
        s = s.str.cat(df[col].astype(str), sep="||")
    return s.map(lambda t: hashlib.sha1(t.encode("utf-8")).hexdigest())

def build_row_ids(df: pd.DataFrame) -> pd.Series:
    base = _canon_ofertas(df)
    return _hash_concat(base, OF_ID_COLS)

# ── Estado simples (evitar reprocessar PDF igual) ─────────────────────────────
def load_state() -> dict:
    try:
        with open(STATE_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(state: dict):
    try:
        os.makedirs(os.path.dirname(STATE_JSON), exist_ok=True)
        with open(STATE_JSON, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# ── Extração PDF ──────────────────────────────────────────────────────────────
def first_page_error_code(pdf_path):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages: return None, None
            text = (pdf.pages[0].extract_text() or "")
    except Exception:
        return None, None
    low = text.lower()
    for pat, code in FIRST_PAGE_ERROR_RULES.items():
        m = pat.search(low)
        if m:
            start = max(0, m.start()-25); end = min(len(text), m.end()+25)
            trecho = text[start:end].replace("\n"," ").strip()
            return code, trecho
    return None, None

def extract_flight_info(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        text = pdf.pages[0].extract_text() or ""
    low = text.lower()

    for pat, err in PAGE_ERROR_PATTERNS.items():
        if pat.search(text): return None, err

    idx = low.find(TIMES_CUTOFF)
    snippet = text[:idx] if idx != -1 else text

    all_times = re.findall(r"\b(\d{2}:\d{2})\b", snippet)
    times_dict = {f"Horário{i+1}": t for i, t in enumerate(all_times)}

    tipo = ""
    matches = list(re.finditer(r"\b(\d{2}:\d{2})\b", snippet))
    if len(matches) >= 2:
        pos2 = matches[1].end()
        window = snippet[pos2:pos2+200].lower()
        if re.search(r"\bdireto\b", window): tipo = "DIRETO"
        else:
            m_esc = re.search(r"(\d+)\s*escalas?", window)
            m_par = re.search(r"(\d+)\s*paradas?", window)
            if m_esc: tipo = f"{m_esc.group(1)} ESCALAS"
            elif m_par: tipo = f"{m_par.group(1)} PARADAS"

    cia = next((c.upper() for c in AIRLINES if c in low), "")

    dm = re.search(r"ida[^\d]*(\d{1,2})\s+de\s+([a-zç]+)\.?\s+de\s+(\d{4})", low)
    if dm:
        day, m_pt, yr = dm.groups()
        m = MONTH_MAP.get(m_pt[:3], 0)
        flight_date = f"{int(day):02d}/{m:02d}/{yr}"
    else:
        flight_date = ""

    return {"Companhia Aérea": cia, **times_dict, "Tipo de Voo": tipo, "Data do Voo": flight_date}, None

def extract_offers_from_pdf(pdf_path, search_dt):
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for p in pdf.pages:
            text += (p.extract_text() or "") + "\n"

    text = re.sub(r"(\d)\n(\d)", r"\1\2", text)
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    cutoff = next((i for i,l in enumerate(lines) if CUTOFF_OFFERS in l.lower()), len(lines))
    lines = lines[:cutoff]

    if not search_dt:
        for l in lines[:10]:
            m = dates_regex.search(l)
            if m: search_dt = m.group(1); break

    offers, last_ent = [], None
    for l in lines:
        norm = re.sub(r"\s+","", l.lower())
        for ent in VALID_ENTITIES:
            if ent.replace(" ","") in norm:
                last_ent = ent; break
        pm = price_regex.search(l)
        if pm and last_ent:
            raw = pm.group(1)
            num = re.sub(r"[^\d,]","", raw).replace(",", ".")
            try: price = float(num)
            except: continue
            offers.append({"Agência/Companhia": last_ent, "Preço": price})
            last_ent = None
    return offers, search_dt

def get_trecho(file_name):
    parts = file_name[:6].upper().split('_')
    return f"{parts[0]}-{parts[1]}" if len(parts)>1 else parts[0]

def rank_prices(df):
    df['Ranking'] = df.groupby('Nome do Arquivo')['Preço'].rank(method='min', ascending=True).astype(int)
    return df

def todas_colunas_preenchidas(row, cols_req):
    return all(pd.notna(row.get(col)) and str(row.get(col)).strip() != "" for col in cols_req)

# ── Excel/Parquet ─────────────────────────────────────────────────────────────
def write_back_preserving(file_path, df_ofertas, df_erros):
    mode = "a" if os.path.exists(file_path) else "w"
    with pd.ExcelWriter(file_path, engine="openpyxl", mode=mode, if_sheet_exists=("replace" if mode=="a" else None)) as writer:
        df_ofertas.to_excel(writer, index=False, sheet_name=SHEET_OFERTAS)
        df_erros.to_excel(writer, index=False, sheet_name=SHEET_ERROS)
        ws = writer.sheets[SHEET_OFERTAS]
        cols = list(df_ofertas.columns)
        for col_name in ["Data do Voo","Data/Hora da Busca"]:
            if col_name in cols:
                idx = cols.index(col_name) + 1
                letter = get_column_letter(idx)
                for cell in ws[letter][1:]:
                    cell.number_format = "DD/MM/YYYY"

def _to_datetime_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in ["Data do Voo","Data/Hora da Busca"]:
        if c in out:
            out[c] = pd.to_datetime(out[c], errors="coerce")
    return out

def _load_master_df() -> pd.DataFrame:
    for p in MASTER_CANDIDATES:
        if p.exists():
            try:
                return pd.read_parquet(p)
            except Exception:
                pass
    return pd.DataFrame()

def _save_master_out(df: pd.DataFrame):
    df2 = _to_datetime_cols(df)
    df2.to_parquet(MASTER_OUT, index=False)

def _dedup_by_id(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    tmp = df.copy()
    tmp["__id__"] = build_row_ids(tmp)
    tmp = tmp.drop_duplicates("__id__", keep="last").drop(columns="__id__")
    return tmp

def export_increment_and_update_master(increment_df: pd.DataFrame):
    # Salva incremento
    inc = increment_df.copy()
    if not inc.empty:
        _to_datetime_cols(inc).to_parquet(PARQUET_INC, index=False)
    else:
        pd.DataFrame(columns=OF_ID_COLS).to_parquet(PARQUET_INC, index=False)

    # Merge com base-mãe e dedupe
    master = _load_master_df()
    all_cols = sorted(set(list(master.columns) + list(inc.columns)))
    if not master.empty:
        for c in all_cols:
            if c not in master.columns: master[c] = pd.NA
        master = master[all_cols]
    if not inc.empty:
        for c in all_cols:
            if c not in inc.columns: inc[c] = pd.NA
        inc = inc[all_cols]

    merged = pd.concat([master, inc], ignore_index=True)
    merged = _dedup_by_id(merged)

    if "Data/Hora da Busca" in merged:
        merged = merged.sort_values(by=["Data/Hora da Busca","Preço"], ascending=[False, True], ignore_index=True)

    _save_master_out(merged)

# ── 1 ciclo de atualização ────────────────────────────────────────────────────
def run_cycle():
    pdfs = sorted(glob.glob(os.path.join(PDF_DIR, "*.pdf")))
    print(f"[{datetime.now():%H:%M:%S}] PDFs na pasta: {len(pdfs)}")
    if not pdfs:
        master = _load_master_df()
        if not master.empty:
            _save_master_out(master)
        return pd.DataFrame(), pd.DataFrame(), 0, 0

    # Cache simples (mtime+tamanho)
    state = load_state()
    def _sig(p: str) -> str | None:
        try:
            st = os.stat(p)
            return f"{st.st_size}-{int(st.st_mtime)}"
        except Exception:
            return None

    to_process = []
    for p in pdfs:
        sig = _sig(p)
        if sig is None:
            continue
        if state.get(p) != sig:
            to_process.append(p)

    if not to_process:
        print(f"[{datetime.now():%H:%M:%S}] Nada novo. Todos os PDFs já convertidos.")
        master = _load_master_df()
        if not master.empty:
            _save_master_out(master)
        return pd.DataFrame(), pd.DataFrame(), 0, 0

    print(f"[{datetime.now():%H:%M:%S}] Novos/alterados: {len(to_process)} de {len(pdfs)}")

    offers_rows, errors_rows = [], []
    for path in tqdm(to_process, desc="Processando PDFs"):
        fn = os.path.basename(path)

        code, trecho = first_page_error_code(path)
        if code:
            errors_rows.append({"Nome do Arquivo": fn, "Erro": code, "Trecho": trecho, "Pagina": 1})

        flight_info, err = extract_flight_info(path)
        if err:
            errors_rows.append({"Nome do Arquivo": fn, "Erro": err, "Trecho": "", "Pagina": 1})

        offers, sdt = extract_offers_from_pdf(path, "")
        for o in offers:
            offers_rows.append({
                "Nome do Arquivo": fn,
                **(flight_info or {}),
                "Data/Hora da Busca": sdt,
                **o,
                "TRECHO": get_trecho(fn)
            })

    new_offers_df = pd.DataFrame(offers_rows)
    new_erros_df  = pd.DataFrame(errors_rows)

    if not new_offers_df.empty:
        new_offers_df["Data do Voo"] = pd.to_datetime(new_offers_df["Data do Voo"], dayfirst=True, errors="coerce")
        so_data = new_offers_df["Data/Hora da Busca"].astype(str).str.extract(r"(\d{2}/\d{2}/\d{4})", expand=False)
        new_offers_df["Data/Hora da Busca"] = pd.to_datetime(so_data, dayfirst=True, errors="coerce")
        diff_days = (new_offers_df["Data do Voo"].dt.normalize() - new_offers_df["Data/Hora da Busca"].dt.normalize()).dt.days
        new_offers_df["ADVP"] = diff_days.fillna(0).astype(int)
        new_offers_df = rank_prices(new_offers_df)

        req = ["Nome do Arquivo","Companhia Aérea","Horário1","Horário2","Horário3",
               "Tipo de Voo","Data do Voo","Data/Hora da Busca",
               "Agência/Companhia","Preço","TRECHO","ADVP","Ranking"]
        new_offers_df = new_offers_df[new_offers_df.apply(lambda r: todas_colunas_preenchidas(r, req), axis=1)]
        new_offers_df = new_offers_df[new_offers_df["Agência/Companhia"].str.lower() != "skyscanner"]
        new_offers_df = to_upper_df(new_offers_df)

    if not new_erros_df.empty:
        new_erros_df = to_upper_df(new_erros_df)

    # Excel e erros parquet (auditoria)
    try:
        base_ofertas = pd.read_excel(MATRIX_XLSX, sheet_name=SHEET_OFERTAS, engine="openpyxl")
    except Exception:
        base_ofertas = pd.DataFrame()
    try:
        base_erros = pd.read_excel(MATRIX_XLSX, sheet_name=SHEET_ERROS, engine="openpyxl")
    except Exception:
        base_erros = pd.DataFrame()

    final_ofertas = pd.concat([base_ofertas, new_offers_df], ignore_index=True) if not new_offers_df.empty else base_ofertas
    final_erros   = pd.concat([base_erros, new_erros_df], ignore_index=True) if not new_erros_df.empty else base_erros
    if not final_ofertas.empty or base_ofertas.empty:
        write_back_preserving(MATRIX_XLSX, final_ofertas, final_erros)
    if not new_erros_df.empty:
        _to_datetime_cols(new_erros_df).to_parquet(PARQUET_ERR, index=False)

    # incremento + atualizar base-mãe (dedupe)
    export_increment_and_update_master(new_offers_df)

    # cache
    for p in to_process:
        try:
            st = os.stat(p)
            state[p] = f"{st.st_size}-{int(st.st_mtime)}"
        except Exception:
            pass
    save_state(state)

    return final_ofertas, final_erros, len(new_offers_df), len(new_erros_df)

# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="Executa apenas 1 ciclo.")
    args = ap.parse_args()

    if args.once:
        of_df, er_df, n_of, n_er = run_cycle()
        print(f"✅ Ciclo concluído. Ofertas novas: {n_of} | Erros novos: {n_er}")
    else:
        while True:
            try:
                start = datetime.now()
                of_df, er_df, n_of, n_er = run_cycle()
                print(f"✅ Ciclo concluído {start:%d/%m %H:%M:%S}. Ofertas novas: {n_of} | Erros novos: {n_er}")
            except Exception as e:
                print(f"❌ Erro no ciclo: {e}")
            print(f"⏲️ Próxima execução em 10 minutos.")
            time.sleep(LOOP_INTERVAL_SEC)

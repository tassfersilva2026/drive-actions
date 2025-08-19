import os, sys, re, io
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID") or (sys.argv[1] if len(sys.argv) > 1 else None)
if not FOLDER_ID:
    print("Defina DRIVE_FOLDER_ID (secret) ou passe como argumento. Ex.: python scripts/drive_pull.py <FOLDER_ID>", flush=True)
    sys.exit(1)

MAX_FILES = int(os.environ.get("MAX_FILES", "0"))       # 0 = sem limite
SINCE_HOURS = int(os.environ.get("SINCE_HOURS", "0"))   # 0 = sem filtro por data
FORCE_REDOWNLOAD = os.environ.get("FORCE_REDOWNLOAD", "0") in ("1","true","yes")

OUT_DIR = Path("./inbox")
OUT_DIR.mkdir(parents=True, exist_ok=True)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
creds = Credentials.from_service_account_file("sa.json", scopes=SCOPES)
service = build("drive", "v3", credentials=creds)

def safe(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', "_", name)

def iso_utc(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

# --- ler base-mãe (prioriza raiz do repo) ---
MASTER_CANDIDATES = [
    Path("OFERTAS.parquet"),
    Path("data/OFERTAS.parquet"),
    Path("out/OFERTAS.parquet"),
]

def load_master_filenames() -> set[str]:
    for p in MASTER_CANDIDATES:
        if p.exists():
            try:
                df = pd.read_parquet(p)
                if "Nome do Arquivo" in df:
                    names = (
                        df["Nome do Arquivo"]
                        .dropna().astype(str).str.strip().str.upper().unique().tolist()
                    )
                    return set(names)
            except Exception as e:
                print(f"[aviso] Não consegui ler {p}: {e}")
    return set()

MASTER_NAMES = load_master_filenames()
if MASTER_NAMES and not FORCE_REDOWNLOAD:
    print(f"[skip-master] {len(MASTER_NAMES):,} nomes encontrados na base-mãe. "
          f"Arquivos com o mesmo nome serão pulados.", flush=True)
elif FORCE_REDOWNLOAD:
    print("[skip-master] FORÇADO a baixar tudo (FORCE_REDOWNLOAD=1).", flush=True)
else:
    print("[skip-master] Base-mãe não encontrada; baixando normalmente.", flush=True)

def should_skip_pdf(dest_name: str) -> bool:
    if FORCE_REDOWNLOAD or not MASTER_NAMES:
        return False
    return dest_name.strip().upper() in MASTER_NAMES

def download_binary(file_id: str, name: str, mime: str):
    dest = OUT_DIR / safe(name)
    if dest.suffix.lower() == ".pdf" and should_skip_pdf(dest.name):
        print(f"[pulei-base] {dest.name} já consta no OFERTAS.parquet", flush=True)
        return False

    request = service.files().get_media(fileId=file_id)
    with io.FileIO(dest, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    print(f"[ok] {dest.name} ({mime})", flush=True)
    return True

def export_google_file(file_id: str, name: str, mime: str):
    if mime == "application/vnd.google-apps.spreadsheet":
        export_mime, ext = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"
    elif mime == "application/vnd.google-apps.document":
        export_mime, ext = "application/pdf", ".pdf"
    elif mime == "application/vnd.google-apps.presentation":
        export_mime, ext = "application/pdf", ".pdf"
    else:
        print(f"[pulei] {name} (google-apps: {mime})", flush=True)
        return False

    dest = OUT_DIR / (safe(name) + ext)
    if dest.suffix.lower() == ".pdf" and should_skip_pdf(dest.name):
        print(f"[pulei-base] {dest.name} já consta no OFERTAS.parquet", flush=True)
        return False

    request = service.files().export_media(fileId=file_id, mimeType=export_mime)
    with io.FileIO(dest, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    print(f"[ok-export] {dest.name} ({mime} → {export_mime})", flush=True)
    return True

def run():
    q = f"'{FOLDER_ID}' in parents and trashed=false"
    if SINCE_HOURS > 0:
        since = datetime.utcnow() - timedelta(hours=SINCE_HOURS)
        q += f" and modifiedTime >= '{iso_utc(since)}'"

    downloaded = 0
    page_token = None
    while True:
        page_size = min(MAX_FILES - downloaded, 100) if MAX_FILES > 0 else 100
        if page_size <= 0:
            break

        resp = service.files().list(
            q=q,
            fields="nextPageToken, files(id, name, mimeType)",
            orderBy="modifiedTime desc",
            pageSize=page_size,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageToken=page_token,
        ).execute()

        files = resp.get("files", [])
        if not files:
            break

        for f in files:
            fid, name, mime = f["id"], f["name"], f.get("mimeType", "")
            ok = export_google_file(fid, name, mime) if mime.startswith("application/vnd.google-apps") else download_binary(fid, name, mime)
            if ok:
                downloaded += 1
                if MAX_FILES > 0 and downloaded >= MAX_FILES:
                    print(f"[fim] limite atingido: {downloaded} arquivo(s) salvos em {OUT_DIR}", flush=True)
                    return

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    print(f"[fim] {downloaded} arquivo(s) salvos em {OUT_DIR}", flush=True)

if __name__ == "__main__":
    run()

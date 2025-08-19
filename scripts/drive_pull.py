# scripts/drive_pull.py
import os, sys, re, io
from pathlib import Path
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID") or (sys.argv[1] if len(sys.argv) > 1 else None)
if not FOLDER_ID:
    print("Defina DRIVE_FOLDER_ID (secret) ou passe como argumento. Ex.: python scripts/drive_pull.py <FOLDER_ID>")
    sys.exit(1)

OUT_DIR = Path("./inbox")
OUT_DIR.mkdir(parents=True, exist_ok=True)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
creds = Credentials.from_service_account_file("sa.json", scopes=SCOPES)
service = build("drive", "v3", credentials=creds)

def safe(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', "_", name)

def download_binary(file_id: str, name: str, mime: str):
    dest = OUT_DIR / safe(name)
    request = service.files().get_media(fileId=file_id)
    with io.FileIO(dest, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    print(f"[ok] {dest.name} ({mime})")

def export_google_file(file_id: str, name: str, mime: str):
    # Exporta nativos do Google: Sheets→XLSX, Docs/Slides→PDF
    if mime == "application/vnd.google-apps.spreadsheet":
        export_mime, ext = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"
    elif mime == "application/vnd.google-apps.document":
        export_mime, ext = "application/pdf", ".pdf"
    elif mime == "application/vnd.google-apps.presentation":
        export_mime, ext = "application/pdf", ".pdf"
    else:
        print(f"[pulei] {name} (google-apps: {mime})")
        return

    dest = OUT_DIR / (safe(name) + ext)
    request = service.files().export_media(fileId=file_id, mimeType=export_mime)
    with io.FileIO(dest, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    print(f"[ok-export] {dest.name} ({mime} → {export_mime})")

def run():
    q = f"'{FOLDER_ID}' in parents and trashed=false"
    page_token = None
    total = 0
    while True:
        resp = service.files().list(
            q=q,
            fields="nextPageToken, files(id, name, mimeType)",
            pageSize=100,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageToken=page_token,
        ).execute()

        for f in resp.get("files", []):
            fid, name, mime = f["id"], f["name"], f.get("mimeType", "")
            if mime.startswith("application/vnd.google-apps"):
                export_google_file(fid, name, mime)
            else:
                download_binary(fid, name, mime)
            total += 1

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    print(f"[fim] {total} arquivo(s) salvos em {OUT_DIR}")

if __name__ == "__main__":
    run()

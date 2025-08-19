# scripts/drive_pull.py
import os, sys, re
from pathlib import Path
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials

FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID") or (sys.argv[1] if len(sys.argv) > 1 else None)
OUT_DIR = Path("./inbox")

if not FOLDER_ID:
    print("Defina DRIVE_FOLDER_ID (secret) ou passe como argumento. Ex.: python scripts/drive_pull.py <FOLDER_ID>")
    sys.exit(1)

OUT_DIR.mkdir(parents=True, exist_ok=True)

# Autentica com a Service Account (sa.json será criado pelo workflow)
gauth = GoogleAuth()
gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
    "sa.json", scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
drive = GoogleDrive(gauth)

# Lista e baixa (pula Docs/Sheets/Slides nativos)
query = f"'{FOLDER_ID}' in parents and trashed=false"
file_list = drive.ListFile({'q': query}).GetList()

baixados = 0
for f in file_list:
    name = f.get("title") or f.get("name") or f["id"]
    mime = f.get("mimeType", "")
    if mime.startswith("application/vnd.google-apps"):
        print(f"[pulei] {name} (google-apps: {mime})")
        continue
    safe = re.sub(r'[\\/:*?"<>|]+', "_", name)
    f.GetContentFile(str(OUT_DIR / safe))
    baixados += 1
    print(f"[ok] {safe} ({mime})")

print(f"[fim] {baixados} arquivo(s) em {OUT_DIR}")

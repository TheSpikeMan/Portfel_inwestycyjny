import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

app = FastAPI()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

@app.get("/", response_class=HTMLResponse)
async def get_home():
    path = os.path.join(BASE_DIR, "static", "index.html")
    print(f"Próba odczytu pliku: {path}")
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        print(f"Długość zawartości: {len(content)}")
        return HTMLResponse(content)
    except Exception as e:
        print(f"Błąd odczytu pliku: {e}")
        return HTMLResponse("<h1>Nie udało się wczytać strony</h1>", status_code=500)

@app.get("/test")
async def test():
    return {"message": "FastAPI działa poprawnie!"}

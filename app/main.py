from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from routers import stocks
import os

# Inicjalizacja aplikacji FastAPI
app = FastAPI()

app.include_router(stocks.router)

# Montowanie folderu 'static' na ścieżce '/static'
app.mount("/static", StaticFiles(directory="static"), name="static")

# Endpoint do strony głównej
@app.get("/", response_class=HTMLResponse)
async def get_home():
    # Zwrócenie pliku index.html z folderu static
    with open(os.path.join("static", "index.html")) as f:
        return HTMLResponse(content=f.read())
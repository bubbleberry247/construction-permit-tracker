from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path

from app.routers import companies, pages, images, dashboard

app = FastAPI(title="建設業許可証管理")

# Static files
app.mount(
    "/static",
    StaticFiles(directory=str(Path(__file__).parent / "static")),
    name="static",
)

# Templates (shared via app.state for routers)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
app.state.templates = templates

# Routers
app.include_router(companies.router)
app.include_router(pages.router)
app.include_router(images.router)
app.include_router(dashboard.router)


@app.get("/")
def root():
    return RedirectResponse(url="/dashboard")

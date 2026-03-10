"""
dashboard.py — FastAPI web dashboard for the Federal Disability Data Monitor.

Routes:
  GET /              — Summary statistics and recent critical alerts
  GET /changes       — Paginated, filterable change log
  GET /url/{id}      — Change history + diff viewer for one target
  GET /trends        — Time-series chart data (JSON) + chart page
  GET /export        — Download change log as CSV
  GET /health        — Health check (no auth required)

HTTP Basic Auth on all routes except /health.
Jinja2 templates in templates/ directory.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app(storage: Any, config: Any) -> FastAPI:
    """
    Create and configure the FastAPI application.

    Args:
        storage: StorageManager instance
        config: AppConfig (or dict) with dashboard settings
    """
    app = FastAPI(
        title="Federal Disability Data Monitor",
        description="Real-time monitoring of federal disability datasets",
        version="1.0.0",
        docs_url=None,  # Disable Swagger UI in production
        redoc_url=None,
    )

    # Store references on app state
    app.state.storage = storage
    app.state.config = config

    # Templates directory
    templates_dir = Path(__file__).parent / "templates"
    templates_dir.mkdir(exist_ok=True)
    templates = Jinja2Templates(directory=str(templates_dir))
    # Add enumerate as a global so templates can use {% for i, item in items | enumerate %}
    templates.env.globals["enumerate"] = enumerate
    app.state.templates = templates

    security = HTTPBasic()

    # ------------------------------------------------------------------
    # Auth dependency
    # ------------------------------------------------------------------

    def verify_auth(credentials: HTTPBasicCredentials = Depends(security)) -> str:
        dash_cfg = _get_dash_cfg(app)
        expected_user = (dash_cfg.get("auth_username") or "admin").encode("utf-8")
        expected_pass = (dash_cfg.get("auth_password") or "changeme").encode("utf-8")

        correct_user = secrets.compare_digest(
            credentials.username.encode("utf-8"), expected_user
        )
        correct_pass = secrets.compare_digest(
            credentials.password.encode("utf-8"), expected_pass
        )
        if not (correct_user and correct_pass):
            raise HTTPException(
                status_code=401,
                detail="Incorrect credentials",
                headers={"WWW-Authenticate": "Basic"},
            )
        return credentials.username

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    @app.get("/health")
    async def health() -> dict:
        """Health check — no auth required."""
        return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}

    @app.get("/", response_class=HTMLResponse)
    async def index(
        request: Request,
        _user: str = Depends(verify_auth),
    ) -> HTMLResponse:
        """Dashboard homepage with summary statistics."""
        storage = app.state.storage
        stats = await storage.get_dashboard_stats()
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "counts_24h": stats.get("counts_24h", {}),
                "total_targets": stats.get("total_targets", 0),
                "recent_changes": stats.get("recent_changes", []),
                "page_title": "Dashboard",
            },
        )

    @app.get("/changes", response_class=HTMLResponse)
    async def changes(
        request: Request,
        severity: Optional[str] = Query(None),
        agency: Optional[str] = Query(None),
        hours: int = Query(168),   # default 7 days
        page: int = Query(1, ge=1),
        _user: str = Depends(verify_auth),
    ) -> HTMLResponse:
        """Paginated, filterable change log."""
        storage = app.state.storage
        page_size = _get_dash_cfg(app).get("page_size", 50)

        rows = await storage.get_changes_since(
            hours=hours,
            severity=severity or None,
            agency=agency or None,
            page=page,
            page_size=page_size,
        )

        # Get distinct agencies for filter dropdown
        all_targets = await storage.get_all_targets()
        agencies = sorted({t.agency for t in all_targets})

        return templates.TemplateResponse(
            "changes.html",
            {
                "request": request,
                "changes": rows,
                "page": page,
                "page_size": page_size,
                "hours": hours,
                "selected_severity": severity or "",
                "selected_agency": agency or "",
                "agencies": agencies,
                "severities": ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
                "page_title": "Change Log",
                "has_next": len(rows) == page_size,
            },
        )

    @app.get("/url/{target_id}", response_class=HTMLResponse)
    async def url_detail(
        request: Request,
        target_id: str,
        _user: str = Depends(verify_auth),
    ) -> HTMLResponse:
        """Change history and diff viewer for one target URL."""
        storage = app.state.storage

        # Get all changes for this target
        all_changes = await storage.get_changes_since(hours=87600)  # ~10 years
        target_changes = [c for c in all_changes if c["target_id"] == target_id]

        # Get latest snapshot info
        latest = await storage.get_latest_snapshot(target_id)

        # Get all active targets for the name/agency lookup
        all_targets = await storage.get_all_targets()
        target_info = next((t for t in all_targets if t.id == target_id), None)

        if not target_info and not target_changes:
            raise HTTPException(status_code=404, detail="Target not found")

        return templates.TemplateResponse(
            "url_detail.html",
            {
                "request": request,
                "target_id": target_id,
                "target_info": target_info,
                "changes": target_changes,
                "latest_snapshot": latest,
                "page_title": f"Target: {target_id}",
            },
        )

    @app.get("/diff/{change_id}", response_class=HTMLResponse)
    async def diff_view(
        request: Request,
        change_id: int,
        _user: str = Depends(verify_auth),
    ) -> HTMLResponse:
        """Side-by-side diff view for a specific change."""
        storage = app.state.storage

        # Fetch changes (scan all to find by id)
        all_changes = await storage.get_changes_since(hours=87600)
        change = next((c for c in all_changes if c["change_id"] == change_id), None)

        if not change:
            raise HTTPException(status_code=404, detail="Change not found")

        before_snapshot = None
        after_snapshot = None
        if change.get("snapshot_before"):
            before_snapshot = await storage.get_snapshot_by_id(change["snapshot_before"])
        if change.get("snapshot_after"):
            after_snapshot = await storage.get_snapshot_by_id(change["snapshot_after"])

        diff_lines = _render_diff_html(change.get("diff_text") or "")

        return templates.TemplateResponse(
            "diff_view.html",
            {
                "request": request,
                "change": change,
                "before_snapshot": before_snapshot,
                "after_snapshot": after_snapshot,
                "diff_lines": diff_lines,
                "page_title": f"Diff #{change_id}",
            },
        )

    @app.get("/trends", response_class=HTMLResponse)
    async def trends(
        request: Request,
        _user: str = Depends(verify_auth),
    ) -> HTMLResponse:
        """Trend chart page."""
        return templates.TemplateResponse(
            "trends.html",
            {
                "request": request,
                "page_title": "Trends",
            },
        )

    @app.get("/api/trends")
    async def trends_data(
        days: int = Query(30, ge=1, le=365),
        _user: str = Depends(verify_auth),
    ) -> dict:
        """JSON endpoint for trend chart data (consumed by trends.html)."""
        storage = app.state.storage
        changes = await storage.get_changes_since(hours=days * 24)

        # Aggregate by day and severity
        day_counts: dict[str, dict[str, int]] = {}
        for c in changes:
            detected = c.get("detected_at", "")
            if isinstance(detected, str):
                day = detected[:10]
            elif hasattr(detected, "strftime"):
                day = detected.strftime("%Y-%m-%d")
            else:
                continue
            if day not in day_counts:
                day_counts[day] = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
            sev = c.get("severity", "LOW")
            day_counts[day][sev] = day_counts[day].get(sev, 0) + 1

        # Fill in missing days with zeros
        today = datetime.now(timezone.utc).date()
        labels = []
        critical_vals, high_vals, medium_vals, low_vals = [], [], [], []
        for i in range(days - 1, -1, -1):
            day = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            labels.append(day)
            counts = day_counts.get(day, {})
            critical_vals.append(counts.get("CRITICAL", 0))
            high_vals.append(counts.get("HIGH", 0))
            medium_vals.append(counts.get("MEDIUM", 0))
            low_vals.append(counts.get("LOW", 0))

        # Agency breakdown
        agency_counts: dict[str, int] = {}
        for c in changes:
            agency = c.get("target_agency", "Unknown")
            agency_counts[agency] = agency_counts.get(agency, 0) + 1
        agency_sorted = sorted(agency_counts.items(), key=lambda x: -x[1])[:10]

        return {
            "labels": labels,
            "datasets": {
                "CRITICAL": critical_vals,
                "HIGH": high_vals,
                "MEDIUM": medium_vals,
                "LOW": low_vals,
            },
            "agency_labels": [a[0] for a in agency_sorted],
            "agency_counts": [a[1] for a in agency_sorted],
        }

    @app.get("/export")
    async def export_csv(
        _user: str = Depends(verify_auth),
    ) -> StreamingResponse:
        """Download full change log as CSV."""
        storage = app.state.storage
        csv_text = await storage.export_csv()

        def generate():
            yield csv_text

        return StreamingResponse(
            generate(),
            media_type="text/csv",
            headers={
                "Content-Disposition": (
                    f"attachment; filename=disability_monitor_changes_"
                    f"{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv"
                )
            },
        )

    return app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_dash_cfg(app: FastAPI) -> dict:
    cfg = app.state.config
    if hasattr(cfg, "dashboard"):
        d = cfg.dashboard
        if hasattr(d, "__dict__"):
            return d.__dict__
        if isinstance(d, dict):
            return d
    return {}


def _render_diff_html(diff_text: str) -> list[dict]:
    """
    Parse unified diff text into a list of annotated lines for HTML rendering.

    Returns list of {"type": "add"|"remove"|"context"|"header", "text": str}.
    """
    lines = []
    for line in diff_text.splitlines():
        if line.startswith("+++") or line.startswith("---") or line.startswith("@@"):
            lines.append({"type": "header", "text": line})
        elif line.startswith("+"):
            lines.append({"type": "add", "text": line})
        elif line.startswith("-"):
            lines.append({"type": "remove", "text": line})
        else:
            lines.append({"type": "context", "text": line})
    return lines

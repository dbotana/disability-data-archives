"""
reporter.py — Alert dispatch and daily digest generation.

Channels:
  - Email via aiosmtplib (async SMTP — never use stdlib smtplib in async code)
  - Slack via httpx POST to webhook URL
  - RSS 2.0 feed written atomically to disk

Daily digest generated as Markdown → HTML via Jinja2 template rendering.
"""

from __future__ import annotations

import logging
import os
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Optional

import httpx
from jinja2 import Environment, PackageLoader, Template, select_autoescape

from storage import StorageManager

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration dataclasses
# ---------------------------------------------------------------------------

SEVERITY_ORDER = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}


@dataclass
class EmailConfig:
    enabled: bool = False
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    use_tls: bool = True
    smtp_user: str = ""
    smtp_password: str = ""
    from_address: str = ""
    to_addresses: list[str] = field(default_factory=list)
    min_severity: str = "HIGH"


@dataclass
class SlackConfig:
    enabled: bool = False
    webhook_url: str = ""
    channel: str = "#disability-data-alerts"
    min_severity: str = "HIGH"


@dataclass
class RSSConfig:
    enabled: bool = True
    output_path: str = "data/feed.xml"
    feed_title: str = "Federal Disability Data Monitor"
    feed_description: str = "Automated monitoring of federal disability datasets"
    feed_link: str = "http://localhost:8000"
    max_items: int = 200


@dataclass
class AlertsConfig:
    email: EmailConfig = field(default_factory=EmailConfig)
    slack: SlackConfig = field(default_factory=SlackConfig)
    rss: RSSConfig = field(default_factory=RSSConfig)


# ---------------------------------------------------------------------------
# Jinja2 templates (inline — no separate template files required for alerts)
# ---------------------------------------------------------------------------

_DIGEST_MARKDOWN_TEMPLATE = """\
# Federal Disability Data Monitor — Daily Digest
**Generated:** {{ generated_at }}

---

## Summary (Last 24 Hours)

| Severity | Count |
|----------|-------|
{% for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW"] -%}
| {{ sev }} | {{ counts.get(sev, 0) }} |
{% endfor %}

Total targets monitored: **{{ total_targets }}**

---

## Critical and High Changes

{% if critical_and_high %}
{% for change in critical_and_high %}
### [{{ change.severity }}] {{ change.target_name }} ({{ change.target_agency }})
- **URL:** {{ change.target_url }}
- **Detected:** {{ change.detected_at }}
- **Type:** {{ change.change_type }}
{% if change.pct_text_changed is not none %}- **Content changed:** {{ "%.1f"|format(change.pct_text_changed) }}%{% endif %}
{% if change.semantic_similarity is not none %}- **Semantic similarity:** {{ "%.3f"|format(change.semantic_similarity) }}{% endif %}
{% if change.semantic_labels %}- **Labels:** {{ change.semantic_labels | join(", ") }}{% endif %}

{% if change.diff_text %}
<details>
<summary>Diff preview</summary>

```diff
{{ change.diff_text[:2000] }}
```

</details>
{% endif %}
---
{% endfor %}
{% else %}
*No critical or high severity changes in the past 24 hours.*
{% endif %}

---

*This report is generated automatically by the Federal Disability Data Monitor,
a non-partisan public interest research tool. It records factual changes to
publicly available federal data without political commentary.*
"""

_EMAIL_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><style>
  body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; }
  .critical { background: #ffebee; border-left: 4px solid #f44336; padding: 8px; margin: 8px 0; }
  .high { background: #fff3e0; border-left: 4px solid #ff9800; padding: 8px; margin: 8px 0; }
  .medium { background: #e8f5e9; border-left: 4px solid #4caf50; padding: 8px; margin: 8px 0; }
  table { border-collapse: collapse; width: 100%; }
  th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
  th { background: #f5f5f5; }
  code { background: #f5f5f5; padding: 2px 4px; font-size: 0.9em; }
  pre { background: #f5f5f5; padding: 12px; overflow-x: auto; font-size: 0.8em; }
</style></head>
<body>
<h1>Federal Disability Data Monitor — Daily Digest</h1>
<p><strong>Generated:</strong> {{ generated_at }}</p>

<h2>Summary (Last 24 Hours)</h2>
<table>
<tr><th>Severity</th><th>Count</th></tr>
{% for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW"] %}
<tr><td><strong>{{ sev }}</strong></td><td>{{ counts.get(sev, 0) }}</td></tr>
{% endfor %}
</table>
<p>Total targets monitored: <strong>{{ total_targets }}</strong></p>

<h2>Critical and High Changes</h2>
{% if critical_and_high %}
{% for change in critical_and_high %}
<div class="{{ change.severity | lower }}">
  <h3>[{{ change.severity }}] {{ change.target_name }} ({{ change.target_agency }})</h3>
  <p><strong>URL:</strong> <a href="{{ change.target_url }}">{{ change.target_url }}</a></p>
  <p><strong>Detected:</strong> {{ change.detected_at }}</p>
  <p><strong>Type:</strong> {{ change.change_type }}</p>
  {% if change.pct_text_changed is not none %}
  <p><strong>Content changed:</strong> {{ "%.1f"|format(change.pct_text_changed) }}%</p>
  {% endif %}
  {% if change.semantic_labels %}
  <p><strong>Labels:</strong> {{ change.semantic_labels | join(", ") }}</p>
  {% endif %}
  {% if change.diff_text %}
  <details><summary>Diff preview</summary>
  <pre>{{ change.diff_text[:2000] | e }}</pre>
  </details>
  {% endif %}
</div>
{% endfor %}
{% else %}
<p><em>No critical or high severity changes in the past 24 hours.</em></p>
{% endif %}

<hr>
<p><small>This report is generated automatically by the Federal Disability Data Monitor,
a non-partisan public interest research tool that records factual changes to
publicly available federal data without political commentary.</small></p>
</body></html>
"""


# ---------------------------------------------------------------------------
# Reporter
# ---------------------------------------------------------------------------


class Reporter:
    """Handles all outbound alert channels and digest generation."""

    def __init__(self, config: AlertsConfig, storage: StorageManager) -> None:
        self._config = config
        self._storage = storage
        self._http_client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "Reporter":
        self._http_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0))
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._http_client:
            await self._http_client.aclose()

    # ------------------------------------------------------------------
    # Alert dispatch (called by scheduler immediately on detected change)
    # ------------------------------------------------------------------

    async def send_alert(self, change_data: dict[str, Any]) -> None:
        """Dispatch an alert through all configured channels."""
        severity = change_data.get("severity", "LOW")

        tasks = []

        if (
            self._config.email.enabled
            and self._config.email.smtp_user
            and _severity_gte(severity, self._config.email.min_severity)
        ):
            tasks.append(self._send_email_alert(change_data))

        if (
            self._config.slack.enabled
            and self._config.slack.webhook_url
            and _severity_gte(severity, self._config.slack.min_severity)
        ):
            tasks.append(self._send_slack_alert(change_data))

        if self._config.rss.enabled:
            tasks.append(self._append_rss_item(change_data))

        for task in tasks:
            try:
                await task
            except Exception as e:
                logger.error(f"Alert dispatch error: {e}")

        # Mark as sent in DB
        change_id = change_data.get("change_id")
        if change_id:
            await self._storage.mark_alert_sent(change_id)

    async def process_pending_alerts(self) -> int:
        """Send alerts for any unsent changes in the database. Returns count sent."""
        pending = await self._storage.get_unsent_alerts()
        count = 0
        for change_data in pending:
            await self.send_alert(change_data)
            count += 1
        return count

    # ------------------------------------------------------------------
    # Email (aiosmtplib — async, never blocking)
    # ------------------------------------------------------------------

    async def _send_email_alert(self, change_data: dict[str, Any]) -> None:
        try:
            import aiosmtplib  # type: ignore
        except ImportError:
            logger.error("aiosmtplib not installed; email alerts disabled")
            return

        cfg = self._config.email
        severity = change_data.get("severity", "")
        target_name = change_data.get("target_name", "Unknown")
        target_url = change_data.get("target_url", "")

        subject = f"[{severity}] Disability Data Monitor: {target_name}"
        body_html = _render_alert_html(change_data)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = cfg.from_address
        msg["To"] = ", ".join(cfg.to_addresses)
        msg.attach(MIMEText(body_html, "html", "utf-8"))

        try:
            await aiosmtplib.send(
                msg,
                hostname=cfg.smtp_host,
                port=cfg.smtp_port,
                start_tls=cfg.use_tls,
                username=cfg.smtp_user,
                password=cfg.smtp_password,
            )
            logger.info(f"Email alert sent for change {change_data.get('change_id')}")
            change_id = change_data.get("change_id")
            if change_id:
                await self._storage.save_alert_record(
                    change_id=change_id,
                    channel="email",
                    status="sent",
                    payload={"subject": subject, "to": cfg.to_addresses},
                )
        except Exception as e:
            logger.error(f"Email alert failed: {e}")
            change_id = change_data.get("change_id")
            if change_id:
                await self._storage.save_alert_record(
                    change_id=change_id,
                    channel="email",
                    status="failed",
                    error_message=str(e),
                )

    # ------------------------------------------------------------------
    # Slack
    # ------------------------------------------------------------------

    async def _send_slack_alert(self, change_data: dict[str, Any]) -> None:
        assert self._http_client is not None
        cfg = self._config.slack

        severity = change_data.get("severity", "")
        target_name = change_data.get("target_name", "Unknown")
        target_url = change_data.get("target_url", "")
        change_type = change_data.get("change_type", "")
        pct = change_data.get("pct_text_changed")

        severity_emoji = {
            "CRITICAL": ":rotating_light:",
            "HIGH": ":warning:",
            "MEDIUM": ":information_source:",
            "LOW": ":white_check_mark:",
        }.get(severity, "")

        text = (
            f"{severity_emoji} *[{severity}] {target_name}*\n"
            f"Type: `{change_type}`\n"
            f"URL: {target_url}\n"
        )
        if pct is not None:
            text += f"Content changed: {pct:.1f}%\n"

        payload = {
            "channel": cfg.channel,
            "text": text,
            "username": "DisabilityDataMonitor",
        }

        try:
            response = await self._http_client.post(cfg.webhook_url, json=payload)
            if response.status_code == 200:
                logger.info(f"Slack alert sent for change {change_data.get('change_id')}")
                change_id = change_data.get("change_id")
                if change_id:
                    await self._storage.save_alert_record(
                        change_id=change_id,
                        channel="slack",
                        status="sent",
                        payload=payload,
                    )
            else:
                raise ValueError(f"Slack returned {response.status_code}")
        except Exception as e:
            logger.error(f"Slack alert failed: {e}")
            change_id = change_data.get("change_id")
            if change_id:
                await self._storage.save_alert_record(
                    change_id=change_id,
                    channel="slack",
                    status="failed",
                    error_message=str(e),
                )

    # ------------------------------------------------------------------
    # RSS 2.0 feed
    # ------------------------------------------------------------------

    async def _append_rss_item(self, change_data: dict[str, Any]) -> None:
        """Atomically append a new item to the RSS feed file."""
        cfg = self._config.rss
        output_path = Path(cfg.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Load existing feed or create new one
        if output_path.exists():
            try:
                tree = ET.parse(output_path)
                root = tree.getroot()
                channel = root.find("channel")
            except ET.ParseError:
                root, channel = _create_rss_root(cfg)
        else:
            root, channel = _create_rss_root(cfg)

        assert channel is not None

        # Build new item
        item = ET.SubElement(channel, "item")
        severity = change_data.get("severity", "")
        target_name = change_data.get("target_name", "Unknown")
        target_url = change_data.get("target_url", "")
        change_type = change_data.get("change_type", "")
        detected_at = change_data.get("detected_at")
        agency = change_data.get("target_agency", "")

        if isinstance(detected_at, datetime):
            pub_date = detected_at.strftime("%a, %d %b %Y %H:%M:%S +0000")
            guid_ts = detected_at.strftime("%Y%m%d%H%M%S")
        else:
            pub_date = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
            guid_ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

        ET.SubElement(item, "title").text = f"[{severity}] {target_name} ({agency})"
        ET.SubElement(item, "link").text = target_url
        ET.SubElement(item, "guid").text = f"{target_url}#{guid_ts}"
        ET.SubElement(item, "pubDate").text = pub_date
        ET.SubElement(item, "description").text = (
            f"Change type: {change_type}. "
            f"Agency: {agency}. "
            f"Severity: {severity}."
        )
        ET.SubElement(item, "category").text = severity

        # Prune to max_items
        items = channel.findall("item")
        if len(items) > cfg.max_items:
            for old_item in items[cfg.max_items:]:
                channel.remove(old_item)

        # Atomic write via temp file + rename
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=output_path.parent,
                delete=False,
                suffix=".tmp",
            ) as tmp:
                tree_out = ET.ElementTree(root)
                ET.indent(tree_out, space="  ")
                tree_out.write(tmp, encoding="utf-8", xml_declaration=True)
                tmp_path = tmp.name

            import os
            os.replace(tmp_path, output_path)
            logger.debug(f"RSS feed updated: {output_path}")
        except Exception as e:
            logger.error(f"RSS feed write failed: {e}")

    # ------------------------------------------------------------------
    # Daily digest
    # ------------------------------------------------------------------

    async def generate_daily_digest(self) -> tuple[str, str]:
        """
        Generate a daily digest report.

        Returns (markdown_text, html_text).
        """
        stats = await self._storage.get_dashboard_stats()
        changes_24h = await self._storage.get_changes_since(hours=24)

        counts = stats.get("counts_24h", {})
        total_targets = stats.get("total_targets", 0)

        critical_and_high = [
            c for c in changes_24h if c["severity"] in ("CRITICAL", "HIGH")
        ]
        critical_and_high.sort(key=lambda x: SEVERITY_ORDER.get(x["severity"], 99))

        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        context = {
            "generated_at": generated_at,
            "counts": counts,
            "total_targets": total_targets,
            "critical_and_high": critical_and_high,
        }

        md_tmpl = Template(_DIGEST_MARKDOWN_TEMPLATE)
        html_tmpl = Template(_EMAIL_HTML_TEMPLATE)

        markdown = md_tmpl.render(**context)
        html = html_tmpl.render(**context)

        return markdown, html

    async def send_daily_digest(self) -> None:
        """Generate and email the daily digest if email is configured."""
        markdown, html = await self.generate_daily_digest()

        if self._config.email.enabled and self._config.email.smtp_user:
            try:
                import aiosmtplib  # type: ignore
            except ImportError:
                logger.error("aiosmtplib not installed")
                return

            cfg = self._config.email
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            subject = f"Disability Data Monitor — Daily Digest {today}"

            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = cfg.from_address
            msg["To"] = ", ".join(cfg.to_addresses)
            msg.attach(MIMEText(markdown, "plain", "utf-8"))
            msg.attach(MIMEText(html, "html", "utf-8"))

            try:
                await aiosmtplib.send(
                    msg,
                    hostname=cfg.smtp_host,
                    port=cfg.smtp_port,
                    start_tls=cfg.use_tls,
                    username=cfg.smtp_user,
                    password=cfg.smtp_password,
                )
                logger.info(f"Daily digest sent to {cfg.to_addresses}")
            except Exception as e:
                logger.error(f"Daily digest email failed: {e}")

        # Save digest to disk regardless
        digest_path = Path("data/digests")
        digest_path.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        (digest_path / f"{date_str}.md").write_text(markdown, encoding="utf-8")
        (digest_path / f"{date_str}.html").write_text(html, encoding="utf-8")
        logger.info(f"Daily digest saved to data/digests/{date_str}.*")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _severity_gte(severity: str, threshold: str) -> bool:
    """Return True if severity is >= threshold (CRITICAL > HIGH > MEDIUM > LOW)."""
    return SEVERITY_ORDER.get(severity, 99) <= SEVERITY_ORDER.get(threshold, 99)


def _create_rss_root(cfg: RSSConfig) -> tuple[ET.Element, ET.Element]:
    """Create a new RSS 2.0 root element with channel metadata."""
    root = ET.Element("rss", version="2.0")
    channel = ET.SubElement(root, "channel")
    ET.SubElement(channel, "title").text = cfg.feed_title
    ET.SubElement(channel, "description").text = cfg.feed_description
    ET.SubElement(channel, "link").text = cfg.feed_link
    ET.SubElement(channel, "language").text = "en-us"
    ET.SubElement(channel, "lastBuildDate").text = datetime.now(timezone.utc).strftime(
        "%a, %d %b %Y %H:%M:%S +0000"
    )
    return root, channel


def _render_alert_html(change_data: dict[str, Any]) -> str:
    """Render a simple HTML alert for email."""
    severity = change_data.get("severity", "")
    target_name = change_data.get("target_name", "Unknown")
    target_url = change_data.get("target_url", "")
    change_type = change_data.get("change_type", "")
    agency = change_data.get("target_agency", "")
    pct = change_data.get("pct_text_changed")
    diff_text = change_data.get("diff_text", "")

    pct_str = f"{pct:.1f}%" if pct is not None else "N/A"
    diff_html = f"<pre>{diff_text[:3000]}</pre>" if diff_text else ""

    color = {"CRITICAL": "#f44336", "HIGH": "#ff9800", "MEDIUM": "#4caf50", "LOW": "#9e9e9e"}.get(
        severity, "#9e9e9e"
    )

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family: Arial, sans-serif; max-width: 700px; margin: 0 auto;">
<div style="border-left: 4px solid {color}; padding: 12px; margin: 8px 0;">
  <h2 style="color: {color};">[{severity}] {target_name}</h2>
  <p><strong>Agency:</strong> {agency}</p>
  <p><strong>URL:</strong> <a href="{target_url}">{target_url}</a></p>
  <p><strong>Change type:</strong> {change_type}</p>
  <p><strong>Content changed:</strong> {pct_str}</p>
  {diff_html}
</div>
<p><small>Federal Disability Data Monitor — non-partisan public interest research tool</small></p>
</body></html>"""

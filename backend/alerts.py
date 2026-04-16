"""
alerts.py
---------
Sends a webhook payload to Zapier.
Zapier then routes it to Email + Slack.

The payload contains:
  - A plain-text summary of top picks
  - Individual signal data for each top pick
  - The disclaimer

In Zapier, you set up:
  1. Trigger: Catch Hook (this webhook)
  2. Action 1: Gmail → send email using {{summary}} and {{top_picks}}
  3. Action 2: Slack → post message using {{slack_message}}
"""

import logging
import os
import requests
from datetime import date
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

ZAPIER_WEBHOOK_URL = os.getenv("ZAPIER_WEBHOOK_URL")
ALERT_EMAIL_TO     = os.getenv("ALERT_EMAIL_TO", "")

DISCLAIMER = (
    "⚠️ NOT financial advice. For personal research only. "
    "Past performance ≠ future results. Always do your own due diligence."
)


def build_email_html(signals: list[dict]) -> str:
    """Build a clean, bulletproof HTML email table."""
    rows = ""
    for s in signals:
        tp = f"${s['take_profit']}" if s.get("take_profit") else "N/A"
        variant = s.get("variant", "Technical")
        rating = s.get("rating", f"Score: {s['total_score']}")
        
        rows += (
            f"<tr style='border-bottom:1px solid #eee;'>"
            f"<td style='padding:12px;font-weight:bold;color:#1a3c6e;'>{s['ticker']}</td>"
            f"<td style='padding:12px;'>{s['company']}</td>"
            f"<td style='padding:12px;color:#2e7d32;font-weight:bold;'>{rating}</td>"
            f"<td style='padding:12px;'>${s['entry_price']}</td>"
            f"<td style='padding:12px;'>${s['stop_loss']}</td>"
            f"<td style='padding:12px;'>{tp}</td>"
            f"<td style='padding:12px;font-size:12px;color:#666;'>{s['rationale']}</td>"
            "</tr>"
        )

    return f"""
    <div style="font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;max-width:800px;margin:auto;border:1px solid #e0e0e0;border-radius:8px;overflow:hidden;">
        <div style="background:#1a3c6e;color:white;padding:20px;text-align:center;">
            <h1 style="margin:0;font-size:24px;">📈 Market Signal Scan — {date.today()}</h1>
        </div>
        <div style="padding:20px;">
            <p>We found <b>{len(signals)}</b> high-conviction signals matching your criteria.</p>
            <table style="width:100%;border-collapse:collapse;text-align:left;font-size:14px;">
                <thead>
                    <tr style="background:#f8f9fa;color:#555;text-transform:uppercase;font-size:11px;letter-spacing:1px;">
                        <th style="padding:12px;border-bottom:2px solid #ddd;">Ticker</th>
                        <th style="padding:12px;border-bottom:2px solid #ddd;">Company</th>
                        <th style="padding:12px;border-bottom:2px solid #ddd;">Rating/Score</th>
                        <th style="padding:12px;border-bottom:2px solid #ddd;">Entry</th>
                        <th style="padding:12px;border-bottom:2px solid #ddd;">SL</th>
                        <th style="padding:12px;border-bottom:2px solid #ddd;">TP</th>
                        <th style="padding:12px;border-bottom:2px solid #ddd;">Rationale</th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
            <div style="background:#fff3e0;border-left:4px solid #ff9800;padding:15px;margin-top:20px;font-size:12px;color:#5d4037;">
                <b>Disclaimer:</b> {DISCLAIMER}
            </div>
            <div style="background:#e8f4f8;border-left:4px solid #0288d1;padding:15px;margin-top:10px;font-size:13px;color:#01579b;">
                <strong>💎 Early Signal:</strong> If you find this valuable, please consider sending any amount for donation to support this work!
            </div>
        </div>
    </div>
    """


def build_slack_message(signals: list[dict]) -> str:
    """Build a condensed, premium Slack message."""
    if not signals:
        return f"📊 Market Scan ({date.today()}): No qualifying signals today."

    lines = [f"🚀 *Market Analysis Scan — {date.today()}*"]
    
    for s in signals[:5]: # Only top 5 in Slack to avoid truncation
        tp = f"${s['take_profit']}" if s.get("take_profit") else "N/A"
        
        # Determine status/score
        if 'momentum_score' in s:
            header = f"📡 *{s['ticker']}* — MGPR: {s['total_score']}/100"
            details = f"RSI: {s['rsi']:.1f} | ATR: {s.get('atr_pct', 'N/A')}%"
        else:
            header = f"👤 *{s['ticker']}* — Insider Score: {s['total_score']}/100"
            details = f"Buy: ${s.get('total_value', 0):,.0f} | {s.get('insider_name', 'N/A')}"

        lines.append(f"\n{header}")
        lines.append(f"> :dart: Entry: *${s['entry_price']}* | SL: *${s['stop_loss']}* | TP: *{tp}*")
        lines.append(f"> 📊 {details}")
        lines.append(f"> 📝 _{s['rationale']}_")

    if len(signals) > 5:
        lines.append(f"\n_...and {len(signals) - 5} more on your Airtable dashboard._")

    lines.append(f"\n_{DISCLAIMER}_")
    lines.append("\n> 💎 *Early Signal:* If you find this valuable, please consider sending any amount for donation to support this work!")
    return "\n".join(lines)


def send_alert(signals: list[dict]) -> bool:
    """
    POST the alert payload to Zapier webhook.
    Zapier handles routing to Email + Slack.
    Returns True if successful.
    """
    if not ZAPIER_WEBHOOK_URL:
        logger.warning("ZAPIER_WEBHOOK_URL not set — skipping alert")
        return False

    if not signals:
        logger.info("No signals to alert on — skipping")
        return True

    # Top 10 signals sorted by score
    top = sorted(signals, key=lambda s: s["total_score"], reverse=True)[:10]

    payload = {
        # Meta
        "scan_date":     str(date.today()),
        "signals_count": len(signals),
        "recipient":     ALERT_EMAIL_TO,

        # For Email action in Zapier
        "email_subject": f"📈 Insider Scanner — {len(top)} signals — {date.today()}",
        "email_html":    build_email_html(top),

        # For Slack action in Zapier
        "slack_message": build_slack_message(top),

        # Raw data (Zapier can use individual fields)
        "top_signals": [
            {
                "ticker":       s["ticker"],
                "company":      s["company"],
                "score":        s["total_score"],
                "rating":       s.get("rating", "N/A"),
                "variant":      s.get("variant", "Technical"),
                "entry":        s["entry_price"],
                "stop_loss":    s["stop_loss"],
                "take_profit":  s["take_profit"],
                "rationale":    s["rationale"],
            }
            for s in top
        ],
        "disclaimer": DISCLAIMER,
    }

    try:
        resp = requests.post(ZAPIER_WEBHOOK_URL, json=payload, timeout=15)
        resp.raise_for_status()
        logger.info(f"Alert sent to Zapier → status {resp.status_code}")
        return True
    except Exception as e:
        logger.error(f"Failed to send Zapier alert: {e}")
        return False

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
    """Build a clean HTML email body for the top signals."""
    rows = ""
    for s in signals:
        tp = f"${s['take_profit']}" if s["take_profit"] else "Hold to Close"
        rows += f"""
        <tr>
          <td style="padding:8px;border:1px solid #ddd;"><b>{s['ticker']}</b></td>
          <td style="padding:8px;border:1px solid #ddd;">{s['company']}</td>
          <td style="padding:8px;border:1px solid #ddd;">{s.get('rating', 'N/A')} ({s['total_score']})</td>
          <td style="padding:8px;border:1px solid #ddd;">${s['entry_price']}</td>
          <td style="padding:8px;border:1px solid #ddd;">${s['stop_loss']}</td>
          <td style="padding:8px;border:1px solid #ddd;">{tp}</td>
          <td style="padding:8px;border:1px solid #ddd;">{s.get('variant', 'Technical')}</td>
          <td style="padding:8px;border:1px solid #ddd;">{s['rationale']}</td>
        </tr>"""

    return f"""
    <html><body style="font-family:Arial,sans-serif;color:#333;">
      <h2 style="color:#1a3c6e;">📈 Insider Scanner — {date.today()}</h2>
      <p>Found <b>{len(signals)}</b> qualifying insider signals today.</p>
      <table style="border-collapse:collapse;width:100%;">
        <thead>
          <tr style="background:#1a3c6e;color:white;">
            <th style="padding:8px;">Ticker</th>
            <th style="padding:8px;">Company</th>
            <th style="padding:8px;">Rating</th>
            <th style="padding:8px;">Entry</th>
            <th style="padding:8px;">Stop Loss</th>
            <th style="padding:8px;">Take Profit</th>
            <th style="padding:8px;">Variant</th>
            <th style="padding:8px;">Rationale</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <p style="color:#888;font-size:12px;margin-top:20px;">{DISCLAIMER}</p>
    </body></html>
    """


def build_slack_message(signals: list[dict]) -> str:
    """Build a Slack-formatted message matching the preferred detailed format."""
    if not signals:
        return f"📊 Insider Scanner ({date.today()}): No qualifying signals today."

    lines = [f":chart_with_upwards_trend: Insider Scanner — {date.today()}"]
    
    for s in signals:
        tp = f"{s['take_profit']}" if s["take_profit"] else "Hold to Close"
        
        # Determine star rating emoji
        if s['total_score'] >= 80:
            star = ":star2: STRONG BUY"
        elif s['total_score'] >= 60:
            star = ":star: BUY"
        else:
            star = ":white_check_mark: WATCH"
            
        same_day_text = str(s.get('same_day_insiders', 1))
        repeat_text = "Yes :white_check_mark:" if s.get('is_repeat_buy') else "No"
        
        lines.append("")
        lines.append(f"*{s['ticker']}* — {s['company']}")
        # Technical-specific fields
        if 'momentum_score' in s:
            lines.append(f"📡 *Technical MGPR Score: {s['total_score']}/100*")
            lines.append(f"📊 RSI: {s['rsi']} | MACD: {s['macd_signal']}")
            lines.append(f"📐 Breakdown → Trend: {s['trend_score']} | Mom: {s['momentum_score']} | Vol: {s['volatility_score']} | Volm: {s['volume_score']}")
        else:
            # Insider-specific fields
            lines.append(f"{star}  |  Score: {s['total_score']}/100")
            lines.append("")
            lines.append(f":bust_in_silhouette: {s['insider_name']} ({s['title']}) bought {s['shares']:,.0f} shares @ ${s['price_paid']:,.2f}")
            lines.append(f":moneybag: Total Value: ${s['total_value']:,.0f}")
            lines.append(f":bar_chart: ATR: {s['atr_pct']}%  |  Variant: {s.get('variant', 'N/A')}")
            lines.append(f":busts_in_silhouette: Same-day insiders: {s.get('same_day_insiders', 1)}  |  Repeat buy: {'Yes :white_check_mark:' if s.get('is_repeat_buy') else 'No'}")

        lines.append(f":dart: Entry: ${s['entry_price']}  |  SL: ${s['stop_loss']}  |  TP: {tp}")
        
        if s.get('spy_gap_note'):
            lines.append(f":chart_with_downwards_trend: {s['spy_gap_note']}")
            
        lines.append("")
        lines.append(f"{s['rationale']}")
        lines.append("────────────────────────────────────────")

    lines.append(f"\n_{DISCLAIMER}_")
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
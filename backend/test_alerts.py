import logging
from alerts import send_alert

logging.basicConfig(level=logging.INFO)

dummy_signals = [
    {
        "ticker": "TEST",
        "company": "Test Company Inc.",
        "total_score": 95,
        "entry_price": 4.50,
        "stop_loss": 4.00,
        "take_profit": 6.00,
        "rationale": "This is a test signal to verify the new donation message formatting.",
        "momentum_score": 30,
        "rsi": 65.5,
        "atr_pct": 5.2
    }
]

print("Testing Zapier Alert webhook...")
success = send_alert(dummy_signals)

if success:
    print("✅ Alert sent successfully! Check your Email and Slack.")
else:
    print("❌ Failed to send alert. Check if ZAPIER_WEBHOOK_URL is set in your .env file.")

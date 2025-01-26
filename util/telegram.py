import requests

def send_to_telegram(chat_id, message, bot_token):
    """
    Sends a message to a Telegram chat.
    """
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }
    response = requests.post(url, json=payload)
    return response.status_code == 200
import requests
import json

def send_push_notification(to: [str], title, body, data_url):
    url = "https://api.expo.dev/v2/push/send"
    headers = {'Content-Type': 'application/json'}
    responses = []

    # Split 'to' into chunks of 100
    for i in range(0, len(to), 100):
        chunk = to[i:i + 100]
        payload = json.dumps({
            "to": chunk,
            "title": title,
            "body": body,
            "sound": "default",
            "data": {
                "url": data_url
            }
        })

        response = requests.post(url, headers=headers, data=payload)
        responses.append(response.text)

    return responses


if __name__ == "__main__":
    send_push_notification(to=["ExponentPushToken[foFY-DLpiHc9EhTNDwwR8G]"] * 212,
                           title="New Listing in Upper West Side",
                           body="$4,600 | 2 Bed | 1 Bath",
                           data_url="https://streeteasy.com/building/101-macombs-place-new_york/3g?featured=1")

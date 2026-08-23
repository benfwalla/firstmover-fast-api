# FirstMover FastAPI

Ernie's matching-listing webhook is configured with `ERNIE_WEBHOOK_URL` and
`ERNIE_WEBHOOK_SECRET` in Vercel. It sends the secret in a header, uses a stable
listing-based delivery ID, and tries transient failures up to three times.

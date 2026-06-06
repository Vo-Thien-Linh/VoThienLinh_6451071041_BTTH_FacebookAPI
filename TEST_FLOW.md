# Test flow

## Chay he thong de test

```powershell
cd C:\Users\ADMIN\Facebook_API
$env:DOCKER_CONFIG="$PWD\.docker-config"
docker compose up --build --force-recreate kafka kafka-ui webhook-service core-service api-service retry-service
```

Kafka UI:

```text
http://localhost:8080
```

## Test luu database

Comment tren Facebook, sau do kiem tra SQL Server:

```sql
SELECT TOP 20 *
FROM comments
ORDER BY created_at DESC;

SELECT TOP 20 *
FROM idempotency_keys
ORDER BY processed_at DESC;
```

## Test spam

Comment:

```text
Spam
```

Ky vong:

- `moderation_results`: `hideComment = true`, reason co `spam_keyword`
- `moderation_commands`: co `delete_comment` neu `FB_SPAM_ACTION=delete`, hoac `hide_comment` neu `FB_SPAM_ACTION=hide`
- `data/blacklist.json`: user vao blacklist sau khi lap spam du `SPAM_BLACKLIST_REPEAT_THRESHOLD`

Config dang dung:

```env
FB_SPAM_ACTION=delete
SPAM_TREAT_LINK_AS_LIGHT=true
SPAM_KEYWORDS=spam,scam,quang cao,quang cao,kiem tien,kiem tien,inbox ngay
SPAM_BLACKLIST_REPEAT_THRESHOLD=3
RATE_LIMIT_ACTION=moderate
```

## Test retry

Sai token la loi khong tu khoi phuc, nen mac dinh se vao `dead_letter`, khong vao `send_retry`.

Muon test `send_retry`, tao loi retryable bang cach them tam thoi vao `service/webhook-service/.env`:

```env
FB_TIMEOUT_MS=1
```

Restart `api-service`, tao comment de he thong goi Facebook, sau do xem:

- `send_failed`
- `send_retry`
- `dead_letter`

Xong nho xoa `FB_TIMEOUT_MS=1`.

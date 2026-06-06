# Facebook API Backend System

Hệ thống backend xử lý sự kiện Facebook Page theo kiến trúc nhiều service, dùng Kafka để truyền sự kiện, SQL Server để lưu trạng thái, và Prometheus + Alertmanager để cảnh báo khi có lỗi nghiêm trọng.

## 1. Mục tiêu hệ thống

Hệ thống được xây dựng để:

- Nhận sự kiện comment từ Facebook Webhook.
- Phân tích comment: spam, intent, sentiment.
- Tự động quyết định reply, ẩn/xóa comment, hoặc đưa sang review.
- Lưu comment và trạng thái xử lý vào SQL Server.
- Đảm bảo không xử lý trùng bằng idempotency key.
- Xử lý lỗi bằng Retry Service, exponential backoff, Dead Letter Queue.
- Cảnh báo khi có message vào `dead_letter` qua Prometheus, Alertmanager và Gmail.

## 2. Kiến trúc tổng quan

```text
Facebook Webhook
      |
      v
webhook-service
      |
      v
Kafka topic: raw_events
      |
      v
core-service
      |
      +--> moderation_results
      +--> moderation_commands
      +--> reply_commands
      |
      v
api-service
      |
      +--> Facebook Graph API
      +--> SQL Server: comments, idempotency_keys
      +--> send_failed
              |
              v
        retry-service
              |
              +--> send_retry
              +--> dead_letter
                         |
                         v
       kafka-exporter -> Prometheus -> Alertmanager -> Gmail
```

## 3. Các service chính

Hệ thống có 4 service ứng dụng chính:
--------------------------------------------------------------------------------------------------------------------------------------------------------------
| Service           | Chức năng                                                                                                                              |
| ------------------| ---------------------------------------------------------------------------------------------------------------------------------------|
| `webhook-service` | Nhận webhook từ Facebook, xác thực request và publish event vào Kafka topic `raw_events`.                                              |
| `core-service`    | Consume `raw_events`, phát hiện spam, phân loại intent/sentiment, quyết định reply hoặc moderation, lưu comment vào SQL Server.        |
| `api-service`     | Consume `reply_commands`, `moderation_commands`, `send_retry`; gọi Facebook Graph API; lưu idempotency; publish lỗi vào `send_failed`. |
| `retry-service`   | Consume `send_failed`, retry theo exponential backoff, publish lại vào `send_retry`, hoặc đưa vào `dead_letter` khi quá số lần retry.  |
--------------------------------------------------------------------------------------------------------------------------------------------------------------

Các thành phần hạ tầng:
-------------------------------------------------------------------------------------
| Thành phần       | Chức năng                                                      |
| -----------------| ---------------------------------------------------------------|
| `kafka`          | Redpanda Kafka broker.                                         |
| `kafka-ui`       | Giao diện xem topic/message Kafka tại `http://localhost:8080`. |
| `kafka-exporter` | Export metric Kafka cho Prometheus.                            |
| `prometheus`     | Thu thập metric và đánh giá alert rule.                        |
| `alertmanager`   | Nhận alert từ Prometheus và gửi email cảnh báo.                |
-------------------------------------------------------------------------------------

## 4. Kafka topics
-----------------------------------------------------------------------------------
| Topic                 | Ý nghĩa                                                 |
| ----------------------| --------------------------------------------------------|
| `raw_events`          | Event thô nhận từ Facebook Webhook.                     |
| `moderation_results`  | Kết quả phân tích spam/intent/sentiment.                |
| `reply_commands`      | Lệnh tự động trả lời comment.                           |
| `moderation_commands` | Lệnh ẩn hoặc xóa comment spam.                          |
| `send_failed`         | Lỗi khi api-service gọi Facebook API thất bại.          |
| `send_retry`          | Lệnh được retry-service publish để api-service gửi lại. |
| `dead_letter`         | Message lỗi quá số lần retry hoặc không thể xử lý tiếp. |
-----------------------------------------------------------------------------------

## 5. Database

Hệ thống dùng SQL Server database `Facebook_API`.

Bảng `comments` lưu comment và trạng thái xử lý:

```sql
comments(
  id,
  comment_id,
  post_id,
  sender_id,
  message,
  intent,
  sentiment,
  status,
  reply_text,
  command_id,
  created_at,
  replied_at
)
```

Bảng `idempotency_keys` lưu trạng thái từng command:

```sql
idempotency_keys(
  command_id,
  status,
  retry_count,
  last_error,
  fb_message_id,
  processed_at,
  expires_at
)
```

`command_id` là khóa idempotency, được tạo theo dạng:

```text
type:commentId:eventId
```

Ví dụ:

```text
delete_comment:122113720580738411_123456789:fb-event-id
```

Nhờ đó, nếu cùng một command được gửi lại, hệ thống có thể phát hiện command đã xử lý và tránh gửi trùng lên Facebook.

## 6. Xử lý spam và blacklist

`core-service` phát hiện spam dựa trên:

- Từ khóa spam như `spam`, `scam`, `quang cao`, `kiem tien`.
- Link trong comment.
- Nội dung lặp lại nhiều lần.
- Tần suất comment cao trong thời gian ngắn.
- Domain độc hại nếu được cấu hình trong `MALICIOUS_DOMAINS`.

Luồng xử lý:

```text
Spam nhẹ -> publish moderation_commands để hide/delete comment
Spam lặp lại >= SPAM_BLACKLIST_REPEAT_THRESHOLD -> đưa sender vào blacklist nội bộ
Scam/link độc hại -> đưa vào moderation/manual review tùy cấu hình
```

Blacklist nội bộ được lưu ở:

```text
data/blacklist.json
```

## 7. Retry, Dead Letter Queue và cảnh báo

Khi `api-service` gọi Facebook API thất bại:

```text
api-service -> send_failed
retry-service -> send_retry
api-service -> thử gửi lại
```

Retry Service dùng exponential backoff:

```text
1s -> 2s -> 4s
```

Cấu hình mặc định trong `docker-compose.yml`:

```env
MAX_RETRY_COUNT=3
SEND_RETRY_BASE_DELAY_MS=1000
RETRY_NON_RETRYABLE_ERRORS=true
```

Khi retry quá số lần:

```text
send_failed -> retry-service -> dead_letter
```

Prometheus rule theo dõi topic `dead_letter`:

```promql
kafka_topic_partition_current_offset{topic="dead_letter"} > 0
```

Khi topic `dead_letter` có message, alert `DeadLetterQueueReceived` chuyển sang `FIRING`, Alertmanager nhận alert và gửi Gmail.

## 8. Circuit Breaker

`api-service` có Circuit Breaker bảo vệ khi Facebook API lỗi liên tục.

Cấu hình:

```env
FB_CIRCUIT_FAILURE_THRESHOLD=5
FB_CIRCUIT_RESET_TIMEOUT_MS=30000
```

Ý nghĩa:

- Sau 5 lần gọi Facebook API thất bại liên tiếp, circuit chuyển sang `open`.
- Khi circuit đang `open`, service không gọi Facebook API tiếp mà fail nhanh.
- Sau 30 giây, circuit cho thử lại ở trạng thái `half_open`.

Log chứng minh circuit breaker hoạt động:

```text
facebook circuit threshold=5 resetMs=30000
Facebook API DELETE ... failed: 401
Facebook API DELETE ... failed: 401
Facebook API DELETE ... failed: 401
Facebook API DELETE ... failed: 401
Facebook API DELETE ... failed: 401
facebook-api circuit breaker is open
```

## 9. Chạy hệ thống

Chạy các service chính:

```powershell
cd C:\Users\ADMIN\Facebook_API
$env:DOCKER_CONFIG="$PWD\.docker-config"
docker compose up --build -d kafka kafka-ui webhook-service core-service api-service retry-service
```

Chạy thêm monitoring:

```powershell
docker compose --profile monitoring up -d kafka-exporter prometheus alertmanager
```

Các URL quan trọng:
------------------------------------------------------
| URL                            | Chức năng         |
| ---                            | ---               |
| `http://localhost:8080`        | Kafka UI          |
| `http://localhost:9090/alerts` | Prometheus Alerts |
| `http://localhost:9093`        | Alertmanager      |
------------------------------------------------------

## 10. Cấu hình môi trường

File cấu hình chính:

```text
service/webhook-service/.env
service/api-service/.env
alertmanager/alertmanager.yml
```

Ví dụ cấu hình SQL Server:

```env
DB_HOST=host.docker.internal
DB_PORT=1433
DB_NAME=Facebook_API
DB_USER=facebook_user
DB_PASSWORD=your_password
SQL_LOG_ENABLED=true
```

Ví dụ cấu hình Gmail trong `alertmanager/alertmanager.yml`:

```yaml
global:
  smtp_smarthost: "smtp.gmail.com:587"
  smtp_from: "your_email@gmail.com"
  smtp_auth_username: "your_email@gmail.com"
  smtp_auth_password: "your_gmail_app_password"
  smtp_require_tls: true
```

Lưu ý: Gmail bắt buộc dùng App Password, không dùng mật khẩu Gmail thường.

`alertmanager/alertmanager.yml` là file cấu hình local có chứa thông tin SMTP, không nên push lên GitHub.

## 11. Kịch bản demo đề xuất

### 11.1. Demo spam

1. Comment nhiều lần nội dung:

```text
Spam
```

2. Mở Kafka UI kiểm tra:

```text
moderation_results
moderation_commands
```

3. Kỳ vọng:

```text
core-service phát hiện spam
api-service gọi Facebook API để hide/delete comment
sender bị đưa vào blacklist nếu lặp đủ số lần
```

### 11.2. Demo lưu database

Chạy SQL:

```sql
SELECT TOP 20 *
FROM comments
ORDER BY created_at DESC;

SELECT TOP 20 *
FROM idempotency_keys
ORDER BY processed_at DESC;
```

Kỳ vọng:

```text
comments có comment mới
idempotency_keys có command_id, status, retry_count, last_error
```

### 11.3. Demo retry và dead letter

1. Cấu hình token sai hoặc tạo lỗi Facebook API.
2. Tạo comment để hệ thống phát sinh command.
3. Xem Kafka UI:

```text
send_failed
send_retry
dead_letter
```

Kỳ vọng:

```text
send_failed -> send_retry -> send_failed -> dead_letter
```

### 11.4. Demo Prometheus + Alertmanager + Gmail

1. Mở Prometheus:

```text
http://localhost:9090/alerts
```

2. Kiểm tra alert:

```text
DeadLetterQueueReceived = FIRING
```

3. Mở Alertmanager:

```text
http://localhost:9093
```

4. Kiểm tra Gmail:

```text
[FIRING] DeadLetterQueueReceived
```

### 11.5. Demo Circuit Breaker

1. Đặt `ACCESS_TOKEN` có giá trị sai nhưng không rỗng.
2. Restart `api-service`.
3. Gửi nhiều command lỗi liên tiếp.
4. Kiểm tra log:

```text
facebook-api circuit breaker is open
```

### 11.6. Demo Idempotency

Chạy SQL:

```sql
SELECT TOP 20 *
FROM idempotency_keys
ORDER BY processed_at DESC;
```

Kiểm tra không có `command_id` trùng:

```sql
SELECT command_id, COUNT(*) AS total
FROM idempotency_keys
GROUP BY command_id
HAVING COUNT(*) > 1;
```

Kết quả trống chứng minh `command_id` không bị trùng.

## 12. Bằng chứng demo nên chụp

- Kafka UI topic `send_failed`.
- Kafka UI topic `send_retry`.
- Kafka UI topic `dead_letter`.
- Prometheus alert `DeadLetterQueueReceived` ở trạng thái `FIRING`.
- Alertmanager có alert active.
- Gmail nhận mail `[FIRING] DeadLetterQueueReceived`.
- SQL Server bảng `comments`.
- SQL Server bảng `idempotency_keys`.
- Log `facebook-api circuit breaker is open`.

## 13. Ghi chú bảo mật

- Không push `.env`.
- Không push `alertmanager/alertmanager.yml` nếu có Gmail App Password.
- Không commit token Facebook, Gemini API key, SQL password hoặc Gmail App Password.
- Nếu App Password bị lộ, cần revoke trong Google Account và tạo mật khẩu mới.


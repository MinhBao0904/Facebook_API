# Facebook API Integration, Webhook, Kafka, AI Automation

Du an gom 3 service .NET 9:

- `backend-api` port `3002`: proxy Facebook Graph API, API quan tri, automation reply/hide comment.
- `webhook-service` port `3001`: nhan webhook Facebook, verify HMAC-SHA256, normalize event, publish vao Kafka topic `raw_events`.
- `core-service`: consume `raw_events`, idempotency, rate limiting, sentiment/intent analysis, automation decision, publish loi sang `dead_letter`.

## Dap ung yeu cau

### Bai 1: Facebook API backend

- `GET /posts`: lay bai viet cua Page qua backend proxy.
- `POST /post`: tao bai viet qua backend, bao ve bang `X-API-KEY`.
- `GET /comments?post_id=...`: lay comment qua backend proxy.
- `POST /automation/reply`: core-service goi backend de reply comment.
- `POST /automation/hide`: core-service goi backend de an comment spam.
- Response dung format `ApiResponse<T>` gom `success`, `errorCode`, `message`, `data`.
- Facebook request co log, retry exponential backoff, circuit breaker, va DLQ cho loi automation.

### Bai 2: Webhook va Kafka

- Webhook verify endpoint: `GET /webhook`.
- Webhook event endpoint: `POST /webhook`.
- Neu co `Facebook:AppSecret`, moi request `POST /webhook` phai co header `X-Hub-Signature-256` hop le.
- Payload Facebook duoc normalize ve schema chung:

```json
{
  "event_id": "comment-id",
  "event_type": "feed",
  "page_id": "page-id",
  "comment_id": "comment-id",
  "user_id": "user-id",
  "message": "noi dung comment",
  "verb": "add",
  "received_at": "2026-06-06T00:00:00Z"
}
```

- Event duoc publish vao Kafka topic `raw_events`.
- Core consumer tat auto-commit va chi commit offset sau khi xu ly xong.

### Bai 3: AI sentiment va automation

- Rule-based AI phan loai `positive`, `neutral`, `negative`, `spam`.
- Co intent: `pricing_question`, `complaint_or_support`, `positive_feedback`, `promotion_spam`, `scam_or_malicious_link`.
- Automation:
  - Positive -> reply cam on.
  - Negative -> reply xin loi/ho tro.
  - Pricing question -> reply tu van.
  - Spam nhe -> hide comment.
  - Scam/link doc hai -> hide va log can review thu cong.

### Cac co che bat buoc

- Retry: Facebook calls retry 3 lan voi backoff 1s, 2s, 4s cho timeout, 429 va 5xx.
- Circuit breaker: mo mach sau 5 loi lien tiep, nghi 30s, sau do half-open.
- Idempotent consumer: core-service dung `event_id/comment_id` de bo qua event trung lap trong tien trinh dang chay.
- Rate limiting: neu 1 user gui tren 20 event trong 1 phut, event duoc dua ve trang thai pending review bang log va khong auto action.
- DLQ: loi automation sau retry/circuit breaker duoc publish vao topic `dead_letter`.
- Alert: Prometheus dung Kafka exporter va rule `DeadLetterQueueReceivedMessages` de bao khi `dead_letter` co message moi.

## Cau hinh

### Backend API

File `services/backend-api/BackendApi/appsettings.json`:

```json
{
  "FacebookApi": {
    "BaseUrl": "https://graph.facebook.com/v19.0",
    "PageId": "YOUR_PAGE_ID",
    "AccessToken": "YOUR_PAGE_ACCESS_TOKEN"
  },
  "AdminApiKey": "SECRET_KEY_123",
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "TopicDeadLetter": "dead_letter"
  }
}
```

### Webhook Service

File `services/webhook-service/WebhookService/appsettings.json`:

```json
{
  "Facebook": {
    "VerifyToken": "YOUR_VERIFY_TOKEN",
    "AppSecret": "YOUR_APP_SECRET"
  },
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "TopicRawEvents": "raw_events"
  }
}
```

### Core Service

File `services/core-service/appsettings.json`:

```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "GroupId": "core-service-group",
    "TopicRawEvents": "raw_events",
    "TopicDeadLetter": "dead_letter"
  },
  "BackendApi": {
    "BaseUrl": "http://localhost:3002/",
    "ApiKey": "SECRET_KEY_123"
  }
}
```

## Chay du an

```bash
cd fb_api
docker compose up -d
```

Terminal 1:

```bash
cd services/backend-api/BackendApi
dotnet run
```

Terminal 2:

```bash
cd services/webhook-service/WebhookService
dotnet run
```

Terminal 3:

```bash
cd services/core-service
dotnet run
```

Dung ngrok de public webhook:

```bash
ngrok http 3001
```

Facebook Webhook callback URL:

```text
https://<ngrok-url>/webhook
```

## Kiem tra nhanh

Build:

```bash
dotnet build services/backend-api/BackendApi
dotnet build services/webhook-service/WebhookService
dotnet build services/core-service
```

Kiem tra webhook verify:

```bash
curl "http://localhost:3001/webhook?hub.mode=subscribe&hub.challenge=test123&hub.verify_token=YOUR_VERIFY_TOKEN"
```

Goi backend proxy:

```bash
curl http://localhost:3002/posts
curl "http://localhost:3002/comments?post_id=POST_ID"
curl -X POST http://localhost:3002/post -H "Content-Type: application/json" -H "X-API-KEY: SECRET_KEY_123" -d "{\"message\":\"Test post from backend\"}"
```

Monitoring:

- Prometheus: `http://localhost:9090`
- Alertmanager: `http://localhost:9093`
- Kafka exporter: `http://localhost:9308/metrics`

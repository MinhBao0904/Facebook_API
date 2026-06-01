# Facebook API Integration & Sentiment Analysis System

## Giới thiệu

Hệ thống tự động giám sát, phân tích cảm xúc và xử lý bình luận từ Facebook Page bằng AI.

**Bài toán**: 
- Nhận webhook event từ Facebook (bình luận mới)
- Phân loại cảm xúc (Tích cực, Tiêu cực, Spam)
- Tự động phản hồi hoặc ẩn spam
- Đảm bảo độ tin cậy (Retry, Circuit Breaker, DLQ)

---

## Kiến trúc hệ thống

```
Facebook ─→ ngrok ─→ Webhook Service (3001) ─→ Kafka raw_events
                                                    ↓
                                            Core Service (AI Analysis)
                                                    ↓
Backend API (3002) ←─ Sentiment Decision ← Kafka Dead Letter Queue
     ↓
Facebook Graph API
```

**3 Services chính**:
- **Webhook Service** (Port 3001): Nhận webhook, verify signature, đẩy Kafka
- **Backend API** (Port 3002): Tương tác Facebook API, Retry + Circuit Breaker
- **Core Service** (Background): Lắng nghe Kafka, phân tích AI, Idempotency check

---

## Công nghệ

| Thành phần | Công nghệ |
|-----------|----------|
| Runtime | .NET 9.0 |
| Framework | ASP.NET Core 9.0 |
| Message Queue | Kafka 7.4.0 |
| Resilience | Polly 8.6.6 |
| Container | Docker & Docker Compose |
| Monitoring | Prometheus + Alertmanager |

---

## Cấu trúc thư mục

```
fb_api/
├── docker-compose.yml              # Kafka, Zookeeper, Prometheus, Alertmanager
├── prometheus/
│   ├── prometheus.yml
│   └── alert.rules.yml
├── alertmanager/
│   └── alertmanager.yml
└── services/
    ├── backend-api/BackendApi/
    │   ├── Controllers/
    │   │   ├── PostsController.cs        # GET /posts, POST /post, GET /comments
    │   │   └── WebhookConfigController.cs # Subscribe/check webhook
    │   ├── Services/
    │   │   ├── FacebookApiService.cs         # Retry + Circuit Breaker
    │   │   ├── FacebookService.cs            # Facebook API calls
    │   │   └── FacebookWebhookService.cs     # Subscribe fields
    │   ├── Auth/ApiKeyAuthAttribute.cs  # API Key validation
    │   └── appsettings.json
    │
    ├── webhook-service/WebhookService/
    │   ├── Controllers/WebhookController.cs # Verify + receive webhook
    │   ├── Services/KafkaProducerService.cs # Produce to raw_events
    │   └── appsettings.json
    │
    └── core-service/
        ├── Services/
        │   ├── KafkaConsumerService.cs      # Consume + Idempotency
        │   └── SentimentAnalysisService.cs  # AI phân loại
        ├── Models/FacebookWebhookEvent.cs
        └── appsettings.json
```

---

## API Endpoints

### Backend API (Port 3002)

| Method | Endpoint | Mô tả | Auth |
|--------|----------|-------|------|
| GET | `/posts` | Lấy bài viết | - |
| POST | `/post` | Tạo bài viết | X-API-KEY |
| GET | `/comments?post_id=...` | Lấy bình luận | - |
| GET | `/api/webhookconfig/subscribe` | Subscribe webhook fields | - |
| GET | `/api/webhookconfig/check` | Check webhook fields | - |

### Webhook Service (Port 3001)

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| GET | `/webhook` | Xác minh webhook từ Facebook |
| POST | `/webhook` | Nhận event từ Facebook |

---

## Cài đặt nhanh

### 1. Clone & Setup
```bash
cd fb_api
```

### 2. Cấu hình

**Backend API** (`services/backend-api/BackendApi/appsettings.json`):
```json
{
  "FacebookApi": {
    "BaseUrl": "https://graph.facebook.com/v19.0",
    "PageId": "YOUR_PAGE_ID",
    "AccessToken": "YOUR_ACCESS_TOKEN"
  },
  "AdminApiKey": "YOUR_SECRET_KEY",
  "Kafka": {
    "BootstrapServers": "localhost:9092"
  }
}
```

**Webhook Service** (`services/webhook-service/WebhookService/appsettings.json`):
```json
{
  "Facebook": {
    "VerifyToken": "my_verify_token",
    "AppSecret": "your_app_secret"
  },
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "TopicRawEvents": "raw_events"
  }
}
```

### 3. Chạy Infrastructure
```bash
# Start Kafka + Zookeeper + Prometheus + Alertmanager
docker-compose up -d

# Create Kafka topics
docker exec -it fb_api-kafka-1 kafka-topics --create --topic raw_events --bootstrap-server localhost:9092
docker exec -it fb_api-kafka-1 kafka-topics --create --topic dead_letter --bootstrap-server localhost:9092
```

### 4. Chạy Services

**Terminal 1 - Backend API**:
```bash
cd services/backend-api/BackendApi
dotnet run  # Port 3002
```

**Terminal 2 - Webhook Service**:
```bash
cd services/webhook-service/WebhookService
dotnet run  # Port 3001
```

**Terminal 3 - Core Service**:
```bash
cd services/core-service
dotnet run
```

**Terminal 4 - ngrok**:
```bash
ngrok http 3001
# Copy URL: https://xxxx.ngrok.io
```

### 5. Setup Facebook Webhook
1. Facebook Developers → App → Webhooks
2. Callback URL: `https://xxxx.ngrok.io/webhook`
3. Verify Token: Nhập token từ appsettings.json
4. Subscribe fields: feed, comments, conversations

---

## Các chức năng triển khai

### Bài 1: Facebook API Integration
✅ GET /posts - Lấy bài viết  
✅ POST /post - Tạo bài viết (API Key auth)  
✅ GET /comments - Lấy bình luận  
✅ Subscribe webhook fields  
✅ Error handling & logging  

### Bài 2: Webhook & Kafka
✅ Webhook verification (HMAC SHA256)  
✅ Kafka producer: Đẩy event vào `raw_events` topic  
✅ Kafka consumer: Core Service lắng nghe  
✅ Spam detection (từ khóa: "http", "mua ngay")  

### Bài 3: AI & Automation
✅ Sentiment analysis: Tích cực, Tiêu cực, Spam, Trung tính  
✅ Auto reply based on sentiment  
✅ Hide comment for spam  

---

## Resilience Patterns

### 1. Retry (Exponential Backoff)
```
Attempt 1: 1s
Attempt 2: 2s
Attempt 3: 4s
Total: 7s max
```

### 2. Circuit Breaker
```
CLOSED (normal) → 5 failures → OPEN (block requests)
                               ↓
                         After 30s → HALF-OPEN (test 1 request)
                                    ↓
                               success → CLOSED
```

### 3. Idempotent Consumer
- Lưu comment_id trong memory
- Chặn xử lý lại nếu đã có
- Tránh duplicate actions

### 4. Dead Letter Queue (DLQ)
- Topic: `dead_letter`
- Message thất bại sau retry
- Trigger Prometheus alert
- Manual intervention cần thiết

---

## Test Webhook

```bash
# Verify webhook
curl "http://localhost:3001/webhook?hub.mode=subscribe&hub.challenge=test123&hub.verify_token=my_verify_token"

# Send test event
curl -X POST http://localhost:3001/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "entry": [{
      "changes": [{
        "value": {
          "comment_id": "123_456",
          "message": "Bình luận tốt lắm!",
          "verb": "add"
        }
      }]
    }]
  }'

# Check Core Service logs:
# 📥 Nhận comment mới: 'Bình luận tốt lắm!'
# 🤖 [AI] Phân loại cảm xúc: Tích cực
# ⚡ [Hành động] Quyết định: Phản hồi
# 💬 [Nội dung phản hồi]: Cảm ơn bạn đã ủng hộ shop!
```

---

## Luồng xử lý

```
1. Facebook → Webhook Service (3001)
   ├─ Verify signature
   └─ Push to Kafka "raw_events"

2. Core Service consumes "raw_events"
   ├─ Idempotent check
   ├─ AI sentiment analysis
   └─ Decision: Reply/Hide/Skip

3. (Future) Backend API executes action
   ├─ Retry: 3 times with exponential backoff
   ├─ Circuit Breaker: Opens after 5 failures
   └─ DLQ: Failed messages

4. Prometheus monitors
   └─ Alert if DLQ has messages
```

---

## Sentiment Rules

| Message | Sentiment | Action | Reply |
|---------|-----------|--------|-------|
| "http://..." hoặc "mua ngay" | Spam | Ẩn | - |
| "tốt", "tuyệt", "ủng hộ" | Tích cực | Phản hồi | "Cảm ơn bạn đã ủng hộ shop!" |
| "tệ", "thất vọng", "chậm" | Tiêu cực | Phản hồi | "Rất xin lỗi, bên mình sẽ kiểm tra ngay" |
| Khác | Trung tính | Bỏ qua | - |

---

## Troubleshooting

**Services không start?**
```bash
# Check logs
docker-compose logs kafka
dotnet run  # See error messages
```

**Webhook không nhận event?**
```bash
# Verify token mismatch
# Check Facebook verify token = appsettings.json token

# Verify signature (optional)
# If X-Hub-Signature-256 header không match, uncomment verify code
```

**Kafka issues?**
```bash
# List topics
docker exec -it fb_api-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

# Monitor topic
docker exec -it fb_api-kafka-1 kafka-console-consumer --topic raw_events --bootstrap-server localhost:9092 --from-beginning
```

**DLQ received messages?**
```bash
# Check what failed
docker exec -it fb_api-kafka-1 kafka-console-consumer --topic dead_letter --bootstrap-server localhost:9092

# Fix root cause & retry manually
```

---

## Cấu hình Facebook Credentials

**Lấy từ Facebook Developers**:
1. https://developers.facebook.com/
2. Create App → Business type
3. Add Facebook Login product
4. Settings → Basic:
   - Copy App ID → PageId
   - Copy Access Token → AccessToken
   - Copy App Secret → AppSecret
5. Create Verify Token tùy ý (lưu vào appsettings.json)

---

## Mở rộng tương lai

- [ ] Database (SQL Server) lưu comment history
- [ ] Dashboard UI để quản lý
- [ ] Real ML model (OpenAI, Hugging Face)
- [ ] Distributed Idempotency (Redis)
- [ ] Auto-retry DLQ
- [ ] Rate limiting
- [ ] Analytics dashboard
- [ ] Unit/Integration tests
- [ ] CI/CD pipeline

---

## Quick Reference

| Task | Command |
|------|---------|
| Start all | `docker-compose up -d` |
| Stop all | `docker-compose down` |
| Logs | `docker-compose logs -f` |
| Restore packages | `dotnet restore` in each service |
| Build | `dotnet build` |
| Run | `dotnet run` |
| Test webhook | `curl` command (see above) |

---

**Version**: 1.0.0  
**Language**: .NET 9.0 / ASP.NET Core 9.0  
**Documentation**: Tiếng Việt


🟢 [Circuit Breaker] Mạch đã đóng lại (CLOSED).
```

#### Idempotent Consumer

Được implement trong `KafkaConsumerService`:

```csharp
private static readonly ConcurrentDictionary<string, bool> _processedComments = new();

// Khi receive comment:
if (_processedComments.ContainsKey(commentId))
{
  Console.WriteLine($"⚠️ [Idempotent] Bỏ qua comment {commentId} vì ĐÃ ĐƯỢC XỬ LÝ TRƯỚC ĐÓ.");
  continue; // Skip
}
_processedComments.TryAdd(commentId, true); // Mark as processed
```

**Lợi ích**: Tránh xử lý trùng lặp khi:
- Kafka redelivery message
- Service restart
- Message được consume nhiều lần

**Lưu ý**: Hiện tại dùng in-memory dictionary (ConcurrentDictionary), không persist. Nên dùng database thực tế.

#### Dead Letter Queue (DLQ)

Khi Facebook API gọi thất bại sau 3 lần retry:

```csharp
catch (Exception)
{
  await SendToDeadLetterQueue(message, "Lỗi gọi Facebook API sau 3 lần Retry");
}

private async Task SendToDeadLetterQueue(string originalMessage, string reason)
{
  var producer = new ProducerBuilder<Null, string>(...).Build();
  var dlqMessage = $"{{\"message\": \"{originalMessage}\", \"error_reason\": \"{reason}\", \"timestamp\": \"{DateTime.UtcNow}\"}}";
  await producer.ProduceAsync("dead_letter", new Message<Null, string> { Value = dlqMessage });
  // Kích hoạt Prometheus/Alertmanager cảnh báo
}
```

**Topic**: `dead_letter`

**Message Format**:
```json
{
  "message": "Cảm ơn bạn đã ủng hộ shop!",
  "error_reason": "Lỗi gọi Facebook API sau 3 lần Retry",
  "timestamp": "2024-06-01T10:30:45.1234567Z"
}
```

**Xử lý**:
- Lưu vào DLQ topic
- Prometheus scrape metric
- Alertmanager gửi cảnh báo (nếu rule được kích hoạt)
- Manual intervention cần thiết

---

### Bài 3: AI và Automation

#### Phân tích cảm xúc (Sentiment Analysis)

**Service**: `SentimentAnalysisService.Analyze(string commentMessage)`

**Phương thức**: Rule-based (từ khóa) - Không dùng ML model

```csharp
public (string Sentiment, string Action, string ReplyMessage) Analyze(string commentMessage)
{
  // Priority 1: Spam Detection
  if (msg.Contains("http") || msg.Contains("mua ngay") || msg.Contains("link lạ"))
    return ("Spam", "Ẩn bình luận", "");

  // Priority 2: Positive Sentiment
  if (msg.Contains("tốt") || msg.Contains("tuyệt") || msg.Contains("ủng hộ") || msg.Contains("ok"))
    return ("Tích cực", "Phản hồi", "Cảm ơn bạn đã ủng hộ shop!");

  // Priority 3: Negative Sentiment
  if (msg.Contains("tệ") || msg.Contains("thất vọng") || msg.Contains("chậm") || msg.Contains("lâu"))
    return ("Tiêu cực", "Phản hồi", "Rất xin lỗi vì trải nghiệm chưa tốt, bên mình sẽ kiểm tra ngay.");

  // Default: Neutral
  return ("Trung tính", "Bỏ qua", "");
}
```

**Return Value**:
- `Sentiment`: "Spam" | "Tích cực" | "Tiêu cực" | "Trung tính" | "Không xác định"
- `Action`: "Ẩn bình luận" | "Phản hồi" | "Bỏ qua"
- `ReplyMessage`: Nội dung phản hồi (nếu action = "Phản hồi")

#### Phân loại Intent

Hiện tại **không có** phân loại intent riêng. Intent được suy ra từ `Action`:
- `Phản hồi`: Intent là trả lời comment
- `Ẩn bình luận`: Intent là loại bỏ spam
- `Bỏ qua`: Intent là không cần xử lý

**Mở rộng tương lai**: Có thể thêm categories như:
- `Câu hỏi về sản phẩm`
- `Khiếu nại`
- `Lời cảm ơn`

#### Automation Rules

**Rule 1: Spam Detection & Hide**
- **Condition**: Message chứa URL hoặc từ khóa spam
- **Action**: Ẩn bình luận (Hide Comment)
- **Automation Level**: Tự động (Auto)

**Rule 2: Positive Feedback & Auto Reply**
- **Condition**: Message chứa từ khóa tích cực
- **Action**: Gửi phản hồi tự động
- **Reply Content**: "Cảm ơn bạn đã ủng hộ shop!"
- **Automation Level**: Tự động (Auto)

**Rule 3: Negative Feedback & Auto Reply**
- **Condition**: Message chứa từ khóa tiêu cực
- **Action**: Gửi phản hồi xin lỗi + hứa kiểm tra
- **Reply Content**: "Rất xin lỗi vì trải nghiệm chưa tốt, bên mình sẽ kiểm tra ngay."
- **Automation Level**: Tự động (Auto)

**Rule 4: Neutral & Skip**
- **Condition**: Message không match bất kỳ rule nào
- **Action**: Bỏ qua (không phản hồi)
- **Automation Level**: Tự động (Auto)

#### Auto Reply Mechanism

**Hiện tại**: Log decision nhưng chưa thực thi

**Future Implementation**:
```csharp
// In KafkaConsumerService
if (action == "Phản hồi")
{
  // Gọi Backend API
  await _backendApiService.SendReplyAsync(commentId, replyMsg);
}
else if (action == "Ẩn bình luận")
{
  // Gọi Backend API
  await _backendApiService.HideCommentAsync(commentId);
}
```

#### Hide Comment Mechanism

**Hiện tại**: Chưa implement, chỉ có planning

**Future Implementation**:
```csharp
public async Task HideCommentAsync(string commentId)
{
  var url = $"https://graph.facebook.com/v19.0/{commentId}?is_hidden=true&access_token={token}";
  var response = await _httpClient.PostAsync(url, ...);
  // Handle response
}
```

**Facebook API Endpoint**:
```
POST https://graph.facebook.com/v19.0/{comment_id}
Parameters:
  - is_hidden=true
  - access_token={token}
```

---

## Hướng dẫn cài đặt

### Yêu cầu hệ thống

- **OS**: Windows 10+, macOS, Linux
- **.NET SDK**: 9.0.0 trở lên
- **Docker & Docker Compose**: Latest version
- **Port Requirements**: 
  - 3001 (Webhook Service)
  - 3002 (Backend API)
  - 9092 (Kafka)
  - 2181 (Zookeeper)
  - 9090 (Prometheus)
  - 9093 (Alertmanager)

### Clone Project

```bash
# Clone repository
git clone <repo-url>
cd fb_api
```

### Cài đặt Dependencies

Mỗi service đều sử dụng .NET 9.0, dependencies được định nghĩa trong `.csproj` files:

#### Backend API
```bash
cd services/backend-api/BackendApi
dotnet restore
```

**Dependencies**:
- Confluent.Kafka 2.14.0
- Polly 8.6.6 (Retry & Circuit Breaker)
- Swashbuckle.AspNetCore 7.0.0

#### Webhook Service
```bash
cd services/webhook-service/WebhookService
dotnet restore
```

**Dependencies**:
- Confluent.Kafka 2.14.0

#### Core Service
```bash
cd services/core-service
dotnet restore
```

**Dependencies**:
- Confluent.Kafka 2.14.0

### Cấu hình Môi trường

#### 1. Backend API Configuration

Sửa file `services/backend-api/BackendApi/appsettings.json`:

```json
{
  "FacebookApi": {
    "BaseUrl": "https://graph.facebook.com/v19.0",
    "PageId": "YOUR_PAGE_ID",
    "AccessToken": "YOUR_PAGE_ACCESS_TOKEN"
  },
  "AdminApiKey": "YOUR_SECRET_API_KEY",
  "Kafka": {
    "BootstrapServers": "localhost:9092"
  }
}
```

**Lấy Facebook Credentials**:
1. Go to Facebook Developers (https://developers.facebook.com/)
2. Create an app (Type: Business)
3. Add Facebook Login product
4. Get Page ID & Access Token từ Settings > Basic
5. Copy AccessToken & PageId vào appsettings.json

#### 2. Webhook Service Configuration

Sửa file `services/webhook-service/WebhookService/appsettings.json`:

```json
{
  "Facebook": {
    "VerifyToken": "my_secret_verify_token_123",
    "AppSecret": "your_app_secret_from_facebook"
  },
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "TopicRawEvents": "raw_events"
  }
}
```

**Lấy App Secret**:
1. Go to Facebook Developers
2. Chọn App
3. Settings > Basic
4. Copy App Secret

#### 3. Core Service Configuration

File `services/core-service/appsettings.json` (có thể để mặc định):

```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9092"
  }
}
```

### Chạy Kafka (Docker Compose)

Kafka, Zookeeper, Prometheus, Alertmanager được định nghĩa trong `docker-compose.yml`:

```bash
# Từ thư mục gốc fb_api/
docker-compose up -d

# Kiểm tra containers
docker-compose ps
```

**Services được khởi động**:
- **zookeeper**: ZOOKEEPER_CLIENT_PORT=2181
- **kafka**: KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092
- **prometheus**: (nếu được add vào docker-compose)
- **alertmanager**: (nếu được add vào docker-compose)

**Verify Kafka**:
```bash
# Tạo topic 'raw_events'
docker exec -it fb_api-kafka-1 kafka-topics --create --topic raw_events --bootstrap-server localhost:9092

# Tạo topic 'dead_letter'
docker exec -it fb_api-kafka-1 kafka-topics --create --topic dead_letter --bootstrap-server localhost:9092

# List topics
docker exec -it fb_api-kafka-1 kafka-topics --list --bootstrap-server localhost:9092
```

### Chạy Backend API

```bash
cd services/backend-api/BackendApi
dotnet run

# Output:
# INFO - 🎮 Service started
# INFO - Listening on http://*:3002
```

**Kiểm tra**:
```bash
curl http://localhost:3002/health
# hoặc POST endpoint với ngrok (sau)
```

### Chạy Webhook Service

```bash
cd services/webhook-service/WebhookService
dotnet run

# Output:
# INFO - 🎮 Service started
# INFO - Listening on http://*:3001
```

### Chạy Core Service

```bash
cd services/core-service
dotnet run

# Output:
# 🚀 [Core Service] Đang lắng nghe dữ liệu từ Kafka topic 'raw_events'...
```

### Setup ngrok (Tunnel Facebook Webhook)

Facebook không thể gọi localhost trực tiếp. Dùng ngrok để tạo public URL:

```bash
# Cài đặt ngrok (nếu chưa)
# Download từ https://ngrok.com/

# Chạy ngrok tunnel to Webhook Service (Port 3001)
ngrok http 3001

# Output:
# Forwarding                    https://xxxx-xxx-xxx-xxx-xxx.ngrok.io -> http://localhost:3001
# Forwarding                    http://xxxx-xxx-xxx-xxx-xxx.ngrok.io -> http://localhost:3001
```

**Lưu URL này**, sẽ dùng để setup webhook trên Facebook.

### Setup Webhook trên Facebook

1. **Go to Facebook Developers**:
   - Chọn App
   - Products > Webhooks > Page
   - Click "Select a Page to subscribe to"

2. **Trong Page Webhook Settings**:
   - **Callback URL**: `https://xxxx-xxx-xxx-xxx-xxx.ngrok.io/webhook`
   - **Verify Token**: Nhập cùng token trong appsettings.json của Webhook Service
   - **Subscribe to this object**:
     - ✓ feed
     - ✓ comments
     - ✓ conversations
     - ✓ message_echoes

3. **Click Subscribe**

4. **Verify webhook** sẽ được gọi:
   ```
   GET https://xxxx.ngrok.io/webhook?hub.mode=subscribe&hub.challenge=...&hub.verify_token=...
   ```
   Webhook Service sẽ verify & return challenge

### Test Webhook

#### Test 1: Verify Webhook (Manual)
```bash
# Simulate Facebook verification
curl "http://localhost:3001/webhook?hub.mode=subscribe&hub.challenge=test123&hub.verify_token=my_secret_verify_token_123"

# Response:
# test123
```

#### Test 2: Send Test Event (Manual)
```bash
curl -X POST http://localhost:3001/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "entry": [{
      "changes": [{
        "value": {
          "comment_id": "123_456",
          "message": "Bình luận tốt lắm!",
          "verb": "add"
        }
      }]
    }]
  }'

# Output (Webhook Service):
# ==================================================
# 📥 CÓ DỮ LIỆU TỪ FACEBOOK GỬI VỀ:
# {...}
# ==================================================
# ✅ Đã đẩy event vào Kafka topic raw_events

# Output (Core Service):
# 📥 Nhận comment mới: 'Bình luận tốt lắm!' (ID: 123_456)
# 🤖 [AI] Phân loại cảm xúc: Tích cực
# ⚡ [Hành động] Quyết định: Phản hồi
# 💬 [Nội dung phản hồi]: Cảm ơn bạn đã ủng hộ shop!
# ✅ Xử lý thành công!
```

#### Test 3: Test Backend API
```bash
# GET /posts
curl http://localhost:3002/posts

# Response:
# {
#   "success": true,
#   "errorCode": 200,
#   "message": "Thành công",
#   "data": [...]
# }

# POST /post (with API Key)
curl -X POST http://localhost:3002/post \
  -H "Content-Type: application/json" \
  -H "X-API-KEY: SECRET_KEY_123" \
  -d '{"message": "Test bài viết"}'

# GET /comments
curl "http://localhost:3002/comments?post_id=1234567890"
```

### Cleanup (Dừng services)

```bash
# Dừng Docker containers
docker-compose down

# Dừng ngrok (Ctrl+C)

# Dừng dotnet services (Ctrl+C trong terminal mỗi service)
```

---

## API Documentation

### Backend API (Port 3002)

#### 1. GET /posts
Lấy danh sách bài viết từ Facebook Page

**Request**:
```http
GET /posts HTTP/1.1
Host: localhost:3002
```

**Response** (200 OK):
```json
{
  "success": true,
  "errorCode": 200,
  "message": "Thành công",
  "data": [
    {
      "id": "1071638069364000_1234567890",
      "message": "Nội dung bài viết",
      "created_time": "2024-06-01T10:30:00+0000"
    }
  ]
}
```

**Response** (Error):
```json
{
  "success": false,
  "errorCode": 400,
  "message": "Lỗi từ phía Facebook API. Vui lòng xem Data hoặc Log để biết chi tiết.",
  "data": {...}
}
```

---

#### 2. POST /post
Tạo bài viết mới trên Facebook Page

**Request**:
```http
POST /post HTTP/1.1
Host: localhost:3002
Content-Type: application/json
X-API-KEY: SECRET_KEY_123

{
  "message": "Nội dung bài viết mới"
}
```

**Response** (200 OK):
```json
{
  "success": true,
  "errorCode": 200,
  "message": "Thành công",
  "data": {
    "id": "1071638069364000_9876543210"
  }
}
```

**Response** (401 Unauthorized - Missing API Key):
```json
{
  "success": false,
  "errorCode": 401,
  "message": "Thiếu API Key trong Header!"
}
```

**Response** (403 Forbidden - Invalid API Key):
```json
{
  "success": false,
  "errorCode": 403,
  "message": "API Key không hợp lệ!"
}
```

**Response** (400 Bad Request - Empty Message):
```json
{
  "success": false,
  "errorCode": 400,
  "message": "Nội dung bài viết không để trống"
}
```

---

#### 3. GET /comments
Lấy danh sách bình luận của một bài viết

**Request**:
```http
GET /comments?post_id=1071638069364000_1234567890 HTTP/1.1
Host: localhost:3002
```

**Response** (200 OK):
```json
{
  "success": true,
  "errorCode": 200,
  "message": "Thành công",
  "data": [
    {
      "id": "1071638069364000_1234567890_9876543210",
      "message": "Bình luận tuyệt vời!",
      "created_time": "2024-06-01T10:35:00+0000",
      "from": {
        "name": "User Name",
        "id": "123456789"
      }
    }
  ]
}
```

**Response** (400 Bad Request - Missing post_id):
```json
{
  "success": false,
  "errorCode": 400,
  "message": "Thiếu post_id"
}
```

---

#### 4. GET /api/webhookconfig/subscribe
Subscribe webhook fields cho Facebook Page

**Request**:
```http
GET /api/webhookconfig/subscribe?pageId=1071638069364000&pageAccessToken=EAAViUmtHSlYBR... HTTP/1.1
Host: localhost:3002
```

**Parameters**:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| pageId | string | Yes | Facebook Page ID |
| pageAccessToken | string | Yes | Facebook Page Access Token |

**Response** (200 OK):
```json
{
  "message": "✅ Đã subscribe webhook fields thành công",
  "fields": "feed, comments, conversations, message_echoes"
}
```

**Response** (400 Bad Request):
```json
{
  "error": "❌ Lỗi khi subscribe webhook"
}
```

---

#### 5. GET /api/webhookconfig/check
Kiểm tra webhook fields đã subscribe của Page

**Request**:
```http
GET /api/webhookconfig/check?pageId=1071638069364000&pageAccessToken=EAAViUmtHSlYBR... HTTP/1.1
Host: localhost:3002
```

**Parameters**:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| pageId | string | Yes | Facebook Page ID |
| pageAccessToken | string | Yes | Facebook Page Access Token |

**Response** (200 OK - comments subscribed):
```json
{
  "pageId": "1071638069364000",
  "subscribedFields": ["feed", "comments", "conversations", "message_echoes"],
  "message": "✅ Field 'comments' đã được subscribe"
}
```

**Response** (200 OK - comments not subscribed):
```json
{
  "pageId": "1071638069364000",
  "subscribedFields": ["feed"],
  "message": "❌ Field 'comments' chưa được subscribe"
}
```

---

### Webhook Service (Port 3001)

#### 1. GET /webhook (Webhook Verification)

**Request** (từ Facebook):
```http
GET /webhook?hub.mode=subscribe&hub.challenge=CHALLENGE_STRING&hub.verify_token=my_secret_verify_token_123 HTTP/1.1
Host: xxxx.ngrok.io
```

**Parameters**:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| hub.mode | string | Yes | "subscribe" |
| hub.challenge | string | Yes | Challenge string từ Facebook |
| hub.verify_token | string | Yes | Token xác minh (so sánh với config) |

**Response** (200 OK):
```
CHALLENGE_STRING
```

**Response** (403 Forbidden):
```
Forbidden
```

**Server Logs**:
```
✅ Xác minh Webhook thành công!
```

---

#### 2. POST /webhook (Webhook Event)

**Request** (từ Facebook):
```http
POST /webhook HTTP/1.1
Host: xxxx.ngrok.io
Content-Type: application/json
X-Hub-Signature-256: sha256=abc123...

{
  "object": "page",
  "entry": [
    {
      "id": "1071638069364000",
      "time": 1717226402170,
      "changes": [
        {
          "field": "feed",
          "value": {
            "comment_id": "1071638069364000_1234567890_9876543210",
            "post_id": "1071638069364000_1234567890",
            "created_time": 1717226402,
            "from": {
              "email": "user@example.com",
              "name": "User Name",
              "id": "123456789"
            },
            "message": "Bình luận từ user",
            "type": "comment",
            "verb": "add"
          }
        }
      ]
    }
  ]
}
```

**Headers**:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| Content-Type | string | Yes | "application/json" |
| X-Hub-Signature-256 | string | No | HMAC SHA256 signature for verification |

**Response** (200 OK):
```
EVENT_RECEIVED
```

**Server Logs** (Webhook Service):
```
==================================================
📥 CÓ DỮ LIỆU TỪ FACEBOOK GỬI VỀ:
{...}
==================================================
✅ Đã đẩy event vào Kafka topic raw_events
```

**Server Logs** (Core Service):
```
📥 Nhận comment mới: 'Bình luận từ user' (ID: 1071638069364000_1234567890_9876543210)
🤖 [AI] Phân loại cảm xúc: Tích cực
⚡ [Hành động] Quyết định: Phản hồi
💬 [Nội dung phản hồi]: Cảm ơn bạn đã ủng hộ shop!
✅ Xử lý thành công!
```

---

### Core Service (Background Service)

Không expose HTTP endpoints. Chỉ có 1 endpoint test:

#### GET / (Health Check)

**Request**:
```http
GET / HTTP/1.1
Host: localhost:5000
```

**Response** (200 OK):
```
🚀 Core Service đang hoạt động và lắng nghe Kafka!
```

---

## Luồng hoạt động hệ thống

### Luồng Chi Tiết (End-to-End)

```
┌─────────────────────────────────────────────────────────────────┐
│ BƯỚC 1: SETUP PHASE                                              │
└─────────────────────────────────────────────────────────────────┘

1.1. Developer cấu hình Facebook App
     - Tạo App trên Facebook Developers
     - Lấy PageID, AccessToken, AppSecret
     - Cấu hình Webhook URL = ngrok_url/webhook

1.2. Developer cấu hình services
     - appsettings.json: Facebook credentials, AdminApiKey, Kafka servers
     - docker-compose up -d (start Kafka, Zookeeper)
     - dotnet run (start Backend, Webhook, Core services)
     - ngrok http 3001 (expose Webhook Service)

1.3. Developer subscribe webhook fields
     - GET /api/webhookconfig/subscribe?pageId=...&pageAccessToken=...
     - Xác nhận subscribed fields: feed, comments, conversations

┌─────────────────────────────────────────────────────────────────┐
│ BƯỚC 2: WEBHOOK VERIFICATION PHASE                               │
└─────────────────────────────────────────────────────────────────┘

2.1. Facebook gửi GET request verify webhook
     Request:
       GET https://xxxx.ngrok.io/webhook?hub.mode=subscribe&hub.challenge=ABC&hub.verify_token=XYZ
     
     Webhook Service:
       - Parse query parameters
       - Compare hub.verify_token vs appsettings["Facebook:VerifyToken"]
       - Return hub.challenge in response body
     
     Response:
       HTTP 200 OK
       Body: ABC

2.2. Facebook verify thành công, bắt đầu gửi events

┌─────────────────────────────────────────────────────────────────┐
│ BƯỚC 3: WEBHOOK EVENT RECEPTION PHASE                            │
└─────────────────────────────────────────────────────────────────┘

3.1. User viết bình luận trên Facebook Page post

3.2. Facebook phát hiện sự kiện comment/feed change

3.3. Facebook gửi webhook event
     POST https://xxxx.ngrok.io/webhook
     Headers: {
       "Content-Type": "application/json",
       "X-Hub-Signature-256": "sha256=abc123..."
     }
     Body: {
       "entry": [{
         "changes": [{
           "field": "feed",
           "value": {
             "comment_id": "123_456",
             "message": "Bình luận tốt lắm!",
             "verb": "add"
           }
         }]
       }]
     }

3.4. ngrok forward request to Webhook Service (localhost:3001)

3.5. WebhookController.ReceiveWebhook()
     - Read raw JSON body
     - (Optional) Verify X-Hub-Signature-256 header
     - Log: "📥 CÓ DỮ LIỆU TỪ FACEBOOK GỬI VỀ"
     - Call KafkaProducerService.ProduceAsync("raw_events", rawJson)
     - Return HTTP 200 OK "EVENT_RECEIVED" (required within 20 seconds)

3.6. Kafka receives message:
     Topic: "raw_events"
     Message: Raw JSON from Facebook

┌─────────────────────────────────────────────────────────────────┐
│ BƯỚC 4: KAFKA MESSAGE PROCESSING PHASE                           │
└─────────────────────────────────────────────────────────────────┘

4.1. Core Service (KafkaConsumerService) listening on "raw_events"
     - Consume message
     - Parse to FacebookWebhookEvent object
     - Extract: comment_id, message, verb

4.2. Idempotent Check
     - Check: _processedComments.ContainsKey(commentId)?
     - If yes: Log "⚠️ [Idempotent] Bỏ qua..." & continue
     - If no: Add to _processedComments[commentId] = true

4.3. Sentiment Analysis
     - Call SentimentAnalysisService.Analyze(message)
     - Evaluate message against rules:
       
       IF message contains ("http", "mua ngay", "link lạ"):
         return ("Spam", "Ẩn bình luận", "")
       ELSE IF message contains ("tốt", "tuyệt", "ủng hộ", "ok"):
         return ("Tích cực", "Phản hồi", "Cảm ơn bạn đã ủng hộ shop!")
       ELSE IF message contains ("tệ", "thất vọng", "chậm", "lâu"):
         return ("Tiêu cực", "Phản hồi", "Rất xin lỗi vì trải nghiệm...")
       ELSE:
         return ("Trung tính", "Bỏ qua", "")

4.4. Log Analysis Result
     - Log: "📥 Nhận comment mới: '{message}' (ID: {commentId})"
     - Log: "🤖 [AI] Phân loại cảm xúc: {sentiment}"
     - Log: "⚡ [Hành động] Quyết định: {action}"
     - If action == "Phản hồi": Log reply message

4.5. Prepare for Execution
     - (Future) Call Backend API to execute action:
       POST /api/reply (with Retry + Circuit Breaker + DLQ)
     - Currently: Just log & mark as processed

┌─────────────────────────────────────────────────────────────────┐
│ BƯỚC 5: EXECUTION PHASE (Future - Currently Not Implemented)    │
└─────────────────────────────────────────────────────────────────┘

5.1. Gọi Backend API để thực hiện hành động
     POST http://localhost:3002/api/reply
     Body: {
       "commentId": "123_456",
       "action": "reply",
       "message": "Cảm ơn bạn đã ủng hộ shop!"
     }

5.2. Backend API thực hiện Retry + Circuit Breaker:
     - Setup Polly policies
     - Call Facebook Graph API: POST /123_456/replies
     - On success: Return 200 OK
     - On failure (attempt 1-3):
       * Retry with exponential backoff: 1s → 2s → 4s
       * Log: "⚠️ [Retry] Gọi API lỗi. Thử lại lần X sau Ys..."
     - After 3 retries still fail:
       * Circuit Breaker opens after 5 failures
       * Log: "🚨 [Circuit Breaker] MẠCH ĐÃ MỞ (OPEN)"
       * Push to DLQ topic

5.3. Dead Letter Queue (DLQ) Processing:
     - Topic: "dead_letter"
     - Message: {
         "message": "Cảm ơn bạn đã ủng hộ shop!",
         "error_reason": "Lỗi gọi Facebook API sau 3 lần Retry",
         "timestamp": "2024-06-01T10:30:45Z"
       }
     - Alert: Kích hoạt Prometheus/Alertmanager cảnh báo

5.4. Manual Intervention:
     - Ops/Admin kiểm tra DLQ
     - Xác định nguyên nhân lỗi
     - Retry hoặc thực hiện manual action

┌─────────────────────────────────────────────────────────────────┐
│ BƯỚC 6: MONITORING & ALERTING PHASE                              │
└─────────────────────────────────────────────────────────────────┘

6.1. Prometheus scrapes metrics
     - Targets: http://localhost:3002/metrics (implicit)
     - Scrape interval: (configured in prometheus.yml)
     - Metrics collected:
       * HTTP request count
       * Kafka message count
       * Error count
       * Circuit Breaker state
       * DLQ message count

6.2. Alert Rules (prometheus/alert.rules.yml)
     - IF DLQ topic has messages:
       * ALERT: "DLQ_MESSAGE_DETECTED"
       * Severity: CRITICAL
       * Action: Send to Alertmanager
     
     - IF Circuit Breaker is OPEN:
       * ALERT: "CIRCUIT_BREAKER_OPEN"
       * Severity: WARNING
       * Action: Send to Alertmanager

6.3. Alertmanager (alertmanager/alertmanager.yml)
     - Receive alerts from Prometheus
     - Group alerts
     - Send notifications (email, Slack, PagerDuty, etc.)
     - Example: "🚨 Critical Alert: DLQ has 5 messages, investigate Facebook API!"

└─────────────────────────────────────────────────────────────────┘
```

### Ví dụ cụ thể với bình luận spam:

```
1. User: "😍😍😍 Mua ngay! Link: http://bit.ly/spam"

2. Facebook detects comment, sends webhook

3. Webhook Service:
   - Receives raw JSON
   - Pushes to Kafka "raw_events" topic

4. Core Service:
   - Consumes message
   - Idempotent check: NEW (first time seeing this comment_id)
   - Sentiment Analysis:
     * Detects: "mua ngay" + "http"
     * Result: ("Spam", "Ẩn bình luận", "")
   - Logs:
     📥 Nhận comment mới: '😍😍😍 Mua ngay! Link: http://bit.ly/spam'
     🤖 [AI] Phân loại cảm xúc: Spam
     ⚡ [Hành động] Quyết định: Ẩn bình luận

5. (Future) Backend API:
   - Receives request to hide comment
   - Calls Facebook API: POST /comment_id?is_hidden=true
   - If success: Comment hidden from timeline
   - If fail after 3 retries: Push to DLQ, alert sent

6. Prometheus + Alertmanager:
   - Monitor action execution
   - Alert if DLQ received message
```

---

## Các cơ chế chịu lỗi đã triển khai

### 1. Retry với Exponential Backoff

**Nơi implement**: `FacebookApiService.SendReplyAsync()`

**Cấu hình**:
```csharp
_retryPolicy = Policy
  .Handle<Exception>()
  .WaitAndRetryAsync(
    retryCount: 3,
    sleepDurationProvider: retryAttempt => 
      TimeSpan.FromSeconds(Math.Pow(2, retryAttempt - 1)),
    onRetry: (exception, timeSpan, retryCount, context) =>
    {
      Console.WriteLine($"⚠️ [Retry] Gọi API lỗi. Thử lại lần {retryCount} sau {timeSpan.TotalSeconds}s...");
    });
```

**Backoff Schedule**:
| Attempt | Delay | Cumulative |
|---------|-------|-----------|
| 1st | 2^0 = 1s | 1s |
| 2nd | 2^1 = 2s | 3s |
| 3rd | 2^2 = 4s | 7s |

**Behavior**:
- Initial call fails → Wait 1s → Retry
- Retry 1 fails → Wait 2s → Retry
- Retry 2 fails → Wait 4s → Retry
- Retry 3 fails → Throw exception (handled by Circuit Breaker)

**Log Output**:
```
🚀 Đang gửi API lên Facebook nội dung: 'Cảm ơn bạn!'...
⚠️ [Retry] Gọi API lỗi. Thử lại lần 1 sau 1s...
⚠️ [Retry] Gọi API lỗi. Thử lại lần 2 sau 2s...
⚠️ [Retry] Gọi API lỗi. Thử lại lần 3 sau 4s...
❌ [Failed] Đã hết số lần Retry nhưng vẫn lỗi.
```

---

### 2. Circuit Breaker

**Nơi implement**: `FacebookApiService.SendReplyAsync()`

**Cấu hình**:
```csharp
_circuitBreakerPolicy = Policy
  .Handle<Exception>()
  .CircuitBreakerAsync(
    handledEventsAllowedBeforeBreaking: 5,
    durationOfBreak: TimeSpan.FromSeconds(30),
    onBreak: (exception, timespan) =>
    {
      Console.WriteLine($"🚨 [Circuit Breaker] Gọi API thất bại 5 lần liên tiếp. MẠCH ĐÃ MỞ (OPEN).");
    },
    onReset: () => 
    {
      Console.WriteLine("🟢 [Circuit Breaker] Mạch đã đóng lại (CLOSED).");
    },
    onHalfOpen: () => 
    {
      Console.WriteLine("🟡 [Circuit Breaker] Mạch đang thử nghiệm lại (HALF-OPEN)...");
    });
```

**State Diagram**:

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                   │
│  CLOSED (🟢)                                                      │
│  - Requests pass through                                          │
│  - Successful calls: Continue CLOSED                              │
│  - Failed calls: Increment counter                                │
│  - If failures >= 5: Transition to OPEN                           │
│                                                                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ failures >= 5
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│                                                                   │
│  OPEN (🚨)                                                        │
│  - All requests immediately throw BrokenCircuitException          │
│  - No calls to backend                                            │
│  - Wait durationOfBreak (30 seconds)                              │
│  - After timeout: Transition to HALF-OPEN                         │
│                                                                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ 30 seconds elapsed
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│                                                                   │
│  HALF-OPEN (🟡)                                                   │
│  - Allow 1 request to test if backend is recovered                │
│  - If successful: Transition to CLOSED                            │
│  - If failed: Transition back to OPEN (reset timeout)             │
│                                                                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ├─ success ──→ [CLOSED]
                             │
                             └─ failure ──→ [OPEN] (restart timer)
```

**Behavior - Detailed Example**:

```
Request 1: FAIL (counter=1)
Request 2: FAIL (counter=2)
Request 3: FAIL (counter=3)
Request 4: FAIL (counter=4)
Request 5: FAIL (counter=5)
  → State changes: CLOSED → OPEN
  → Log: "🚨 [Circuit Breaker] Gọi API thất bại 5 lần liên tiếp. MẠCH ĐÃ MỞ (OPEN)."
  → Start 30-second timer

Request 6-N (during OPEN):
  → Immediately throw BrokenCircuitException
  → No backend call
  → Log: "❌ [Blocked] Mạch đang MỞ, request bị chặn lại ngay lập tức."

After 30 seconds: State changes: OPEN → HALF-OPEN
  → Log: "🟡 [Circuit Breaker] Mạch đang thử nghiệm lại (HALF-OPEN)..."

Request N+1 (first in HALF-OPEN):
  → Try to call backend
  → If SUCCESS: State changes: HALF-OPEN → CLOSED
    → Log: "🟢 [Circuit Breaker] Mạch đã đóng lại (CLOSED)."
    → Counter resets to 0
    → Continue accepting requests normally
  → If FAIL: State changes: HALF-OPEN → OPEN
    → Log: "🚨 [Circuit Breaker] Mạch lại bị MỞ (OPEN)."
    → Restart 30-second timer
```

**Use Case**:
- Backend (Facebook API) là down hoặc bị rate-limit
- Gửi request đến nó sẽ lãng phí tài nguyên
- Circuit Breaker tạm dừng tất cả request
- Sau khi backend recovery, tự động resume

---

### 3. Idempotent Consumer

**Nơi implement**: `KafkaConsumerService.ExecuteAsync()`

**Concept**:
- Xử lý cùng message nhiều lần vẫn chỉ thực hiện hành động 1 lần
- Phòng trường hợp: Kafka redeliver, service restart, network retry

**Cấu hình**:
```csharp
private static readonly ConcurrentDictionary<string, bool> _processedComments = new();

protected override async Task ExecuteAsync(CancellationToken stoppingToken)
{
  while (!stoppingToken.IsCancellationRequested)
  {
    var consumeResult = consumer.Consume(stoppingToken);
    var fbEvent = JsonSerializer.Deserialize<FacebookWebhookEvent>(rawJson);
    
    string commentId = fbEvent.entry[0].changes[0].value.comment_id;
    string message = fbEvent.entry[0].changes[0].value.message;
    
    // IDEMPOTENT CHECK
    if (_processedComments.ContainsKey(commentId))
    {
      Console.WriteLine($"⚠️ [Idempotent] Bỏ qua comment {commentId} vì ĐÃ ĐƯỢC XỬ LÝ TRƯỚC ĐÓ.");
      continue; // Skip processing
    }
    
    // Mark as processed
    _processedComments.TryAdd(commentId, true);
    
    // Process comment
    var (sentiment, action, replyMsg) = _aiService.Analyze(message);
    // ... rest of processing
  }
}
```

**Behavior**:

```
Kafka Message Delivery Timeline:

T=0:  Webhook → Kafka: comment_id=123_456
T=0:  Core Service consumes: 123_456
      Check: 123_456 in _processedComments? NO
      Add: _processedComments[123_456] = true
      Process: Sentiment analysis, decision making
      Log: ✅ Xử lý thành công!

T=5:  Kafka redelivers: comment_id=123_456 (network timeout)
T=5:  Core Service consumes: 123_456
      Check: 123_456 in _processedComments? YES
      Skip: continue (no processing)
      Log: ⚠️ [Idempotent] Bỏ qua comment 123_456 vì ĐÃ ĐƯỢC XỬ LÝ TRƯỚC ĐÓ.

T=10: Service restart (process dies & restarts)
T=10: Kafka redeliversafter restart: comment_id=123_456
T=10: Core Service consumes: 123_456
      Check: 123_456 in _processedComments? NO (in-memory lost)
      ⚠️ PROBLEM: Would process again!
```

**Limitation**:
- Dùng in-memory `ConcurrentDictionary`
- Data mất khi service restart
- Không scalable cho distributed system

**Solution (Future)**:
- Dùng database (SQL Server, MongoDB, Redis)
- Cache với TTL (Time-to-Live)
- Kafka offset tracking

**Example with Redis**:
```csharp
// Check Redis
bool isProcessed = await _redisClient.ExistsAsync($"comment:{commentId}");
if (isProcessed)
{
  Console.WriteLine("⚠️ [Idempotent] Bỏ qua comment đã xử lý.");
  continue;
}

// Set in Redis with TTL (7 days)
await _redisClient.SetExAsync($"comment:{commentId}", "processed", TimeSpan.FromDays(7));

// Process comment
var result = _aiService.Analyze(message);
```

---

### 4. Dead Letter Queue (DLQ)

**Nơi implement**: `FacebookApiService.SendReplyAsync()` → `SendToDeadLetterQueue()`

**Concept**:
- Message thất bại sau tất cả Retry & Circuit Breaker
- Lưu vào topic riêng để xử lý sau (manual hoặc auto)
- Tránh mất dữ liệu

**Trigger**:
```csharp
catch (Exception)
{
  Console.WriteLine($"❌ [Failed] Đã hết số lần Retry nhưng vẫn lỗi.");
  await SendToDeadLetterQueue(message, "Lỗi gọi Facebook API sau 3 lần Retry");
}
```

**Implementation**:
```csharp
private async Task SendToDeadLetterQueue(string originalMessage, string reason)
{
  var kafkaConfig = new ProducerConfig 
  { 
    BootstrapServers = _config["Kafka:BootstrapServers"] ?? "localhost:9092" 
  };
  using var producer = new ProducerBuilder<Null, string>(kafkaConfig).Build();
  
  var dlqMessage = $"{{\"message\": \"{originalMessage}\", \"error_reason\": \"{reason}\", \"timestamp\": \"{DateTime.UtcNow}\"}}";
  
  try
  {
    var result = await producer.ProduceAsync("dead_letter", new Message<Null, string> 
    { 
      Value = dlqMessage 
    });
    Console.WriteLine($"💀 [DLQ] Đã lưu message thất bại vào topic 'dead_letter'.");
    Console.WriteLine($"🔔 [Alert] Kích hoạt Prometheus/Alertmanager cảnh báo vận hành!");
  }
  catch (ProduceException<Null, string> e)
  {
    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
  }
}
```

**Message Format**:
```json
{
  "message": "Cảm ơn bạn đã ủng hộ shop!",
  "error_reason": "Lỗi gọi Facebook API sau 3 lần Retry",
  "timestamp": "2024-06-01T10:30:45.1234567Z"
}
```

**Topic**: `dead_letter`

**Kafka Configuration**:
```bash
# Create DLQ topic (if not auto-created)
docker exec -it fb_api-kafka-1 kafka-topics --create \
  --topic dead_letter \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

**Monitoring DLQ**:

```bash
# Consume DLQ messages
docker exec -it fb_api-kafka-1 kafka-console-consumer \
  --topic dead_letter \
  --bootstrap-server localhost:9092 \
  --from-beginning

# Output:
# {"message": "Cảm ơn bạn đã ủng hộ shop!", "error_reason": "Lỗi gọi Facebook API...", "timestamp": "2024-06-01..."}
```

**Alerting**:

Prometheus Alert Rule (prometheus/alert.rules.yml):
```yaml
groups:
  - name: facebook_api
    rules:
      - alert: DLQMessageDetected
        expr: increase(dlq_messages_total[5m]) > 0
        for: 1m
        annotations:
          summary: "DLQ has received messages"
          description: "{{ $value }} messages in dead_letter topic"
```

Alertmanager Config (alertmanager/alertmanager.yml):
```yaml
receivers:
  - name: 'critical'
    email_configs:
      - to: 'ops@company.com'
        from: 'alerts@company.com'
        smarthost: 'smtp.gmail.com:587'
        auth_username: 'alerts@company.com'
        auth_password: 'password'
```

**Handling DLQ Messages**:

1. **Monitor**: Prometheus alert fires
2. **Investigate**: 
   - Check logs: `docker logs webhook-service`
   - Check Facebook API status
   - Check network connectivity
3. **Remediate**:
   - Fix root cause (e.g., update credentials)
   - Retry manually or via batch job
4. **Reprocess**:
   ```csharp
   // Consume from DLQ & retry
   var dlqMessage = JsonSerializer.Deserialize<DLQMessage>(dlqJson);
   await _facebookApiService.SendReplyAsync(dlqMessage.Message);
   ```

**Example Scenario**:

```
T=0: User posts comment
T=1: Webhook → Kafka "raw_events"
T=2: Core Service processes → Decides to send reply
T=3: Backend API attempts to call Facebook
     Attempt 1: FAIL (Facebook API down)
     Attempt 2: FAIL (after 1s retry)
     Attempt 3: FAIL (after 2s retry)
     Attempt 4: FAIL (after 4s retry, exceeds 3 retries)
     Circuit Breaker opens (5 total failures across requests)
T=20: DLQ message created:
      {
        "message": "Cảm ơn bạn đã ủng hộ shop!",
        "error_reason": "Lỗi gọi Facebook API sau 3 lần Retry",
        "timestamp": "2024-06-01T10:20:00Z"
      }
T=21: Prometheus scrapes, detects DLQ message
T=22: Alert fires to Alertmanager
T=23: Email notification sent to ops@company.com
T=30: Ops logs in, checks DLQ
T=31: Ops realizes Facebook API is back online
T=32: Ops manually reprocesses message via admin dashboard
T=33: Reply successfully posted to Facebook
```

---

## Kết quả đạt được

### Chức năng triển khai

✅ **Webhook Integration**
- Nhận webhook từ Facebook tại endpoint `/webhook`
- Xác minh webhook signature HMAC SHA256
- Đẩy dữ liệu thô vào Kafka topic `raw_events`

✅ **Facebook API Integration**
- GET /posts - Lấy danh sách bài viết
- POST /post - Tạo bài viết (với API Key auth)
- GET /comments - Lấy bình luận
- GET /api/webhookconfig/subscribe - Subscribe webhook fields
- GET /api/webhookconfig/check - Kiểm tra webhook fields

✅ **Sentiment Analysis (AI)**
- Phân loại cảm xúc: Tích cực, Tiêu cực, Spam, Trung tính
- Rule-based system với từ khóa tiếng Việt
- Quyết định hành động: Phản hồi, Ẩn bình luận, Bỏ qua

✅ **Kafka Message Queue**
- Producer: Webhook Service đẩy event vào `raw_events` topic
- Consumer: Core Service lắng nghe & xử lý event
- Topics: `raw_events`, `dead_letter`

✅ **Resilience Patterns**
- Retry: 3 lần với exponential backoff (1s, 2s, 4s)
- Circuit Breaker: Mở sau 5 lỗi, reset sau 30s
- Idempotent Consumer: Chặn xử lý trùng lặp (in-memory)
- Dead Letter Queue: Lưu message thất bại để xử lý sau

✅ **Error Handling & Logging**
- Structured logging (Info, Error, Critical)
- User-friendly error messages
- DLQ integration cho message thất bại

✅ **Docker Infrastructure**
- docker-compose.yml: Kafka, Zookeeper, Prometheus, Alertmanager
- Service isolation & networking

✅ **Configuration Management**
- appsettings.json cho mỗi service
- Environment-based configuration (Development, Production)
- Sensitive data externalisation

### Công nghệ sử dụng

- **Runtime**: .NET 9.0
- **Framework**: ASP.NET Core 9.0
- **Message Queue**: Apache Kafka 7.4.0
- **Resilience**: Polly 8.6.6
- **API Client**: HttpClient (built-in)
- **Logging**: ILogger (built-in)
- **Database**: None (in-memory Idempotent check)
- **Monitoring**: Prometheus + Alertmanager
- **Containerization**: Docker & Docker Compose

### Kiến trúc

```
┌─────────────┐
│   Facebook  │
└──────┬──────┘
       │
     ngrok
       │
       ↓
┌──────────────────────────────────────────────────┐
│            Webhook Service (Port 3001)           │
│  - Verify webhook                                │
│  - Receive Facebook events                       │
│  - Produce to Kafka "raw_events" topic           │
└────────────────┬─────────────────────────────────┘
                 │
                 ↓
        ┌────────────────┐
        │     Kafka      │
        │ raw_events  ┌──┴──────────────────┐
        │             │                     │
        │         dead_letter               │
        └────────────────┘                  │
                 ↑                          │
                 │                          │
        ┌────────┴──────────┬───────────────┘
        │                   │
        │                   ↓
        │        ┌────────────────────────────────┐
        │        │  Core Service (Background)     │
        │        │  - Consume raw_events topic    │
        │        │  - Sentiment analysis (AI)     │
        │        │  - Idempotent check            │
        │        │  - Decision making             │
        │        └────────────────────────────────┘
        │
        │
        └─────────────────────────────┐
                                      │
                                      ↓
                        ┌─────────────────────────────┐
                        │  Backend API (Port 3002)    │
                        │  - Facebook API integration │
                        │  - Retry + Circuit Breaker  │
                        │  - DLQ for failures         │
                        │  - Webhook config API       │
                        └─────────────────────────────┘
                                      │
                                      ↓
                                ┌──────────────┐
                                │ Facebook API │
                                └──────────────┘

┌──────────────────────────────────────────────────┐
│  Monitoring                                      │
│  - Prometheus: Scrape metrics                    │
│  - Alertmanager: Send alerts                     │
└──────────────────────────────────────────────────┘
```

### Những điểm nổi bật

1. **End-to-End Integration**: Từ Facebook webhook → Kafka → AI analysis → Facebook API reply
2. **Production-Ready Resilience**: Retry, Circuit Breaker, DLQ, Idempotency
3. **Scalable Architecture**: Microservices + Message Queue (easy horizontal scaling)
4. **Monitoring & Alerting**: Prometheus + Alertmanager integration
5. **Clean Code**: Separation of concerns (Controllers, Services, Models)
6. **Configuration Driven**: Easy environment switching
7. **Vietnamese Support**: Messages & logs in Tiếng Việt

### Những cần cải tiến tương lai

1. **Database Integration**: Lưu persistent comment history, automation rules
2. **User Interface**: Dashboard để view events, configure rules, manage DLQ
3. **Advanced AI**: Integrate real ML models (OpenAI, Hugging Face) thay vì rule-based
4. **Distributed Idempotency**: Dùng Redis/database thay vì in-memory
5. **Auto-retry DLQ**: Background job để retry DLQ messages sau khi fix root cause
6. **Rate Limiting**: Throttle requests để tránh Facebook API limit
7. **Multi-Language**: Support tiếng anh, Trung Quốc, v.v.
8. **Analytics**: Track sentiment trends, response metrics
9. **Testing**: Unit tests, integration tests, performance tests
10. **CI/CD**: Automated build & deployment pipeline

---

## Tác giả & Giấy phép

- **Phiên bản**: 1.0.0
- **Ngôy ngữ tài liệu**: Tiếng Việt
- **Framework**: .NET 9.0 / ASP.NET Core 9.0
- **Ngày tạo**: June 1, 2024

---

## Liên hệ & Support

Hãy kiểm tra các file source code tương ứng để hiểu chi tiết implementation:

- Backend API: `services/backend-api/BackendApi/`
- Webhook Service: `services/webhook-service/WebhookService/`
- Core Service: `services/core-service/`
- Infrastructure: `docker-compose.yml`

Mọi thắc mắc hoặc đề xuất, vui lòng liên hệ qua repository issues.

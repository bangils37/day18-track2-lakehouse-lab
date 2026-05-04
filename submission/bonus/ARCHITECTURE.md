# Architecture Brief: LLM Observability at Scale (1B req/day)

**Topic:** A. LLM observability ở quy mô 1B requests/ngày
**Architect:** Antigravity (AI Assistant)

---

## 1. Problem Statement

Chúng ta cần xây dựng hệ thống observability cho một LLM API gateway xử lý **1 tỷ requests mỗi ngày**. 
- **Quy mô dữ liệu:** ~5 KB/req × 1B req = **5 TB dữ liệu thô/ngày**.
- **Yêu cầu kinh doanh:** 
    1. Dashboard chi phí & latency theo tenant, cập nhật mỗi 5 phút.
    2. Giữ log đầy đủ (prompt/response) trong 7 ngày để điều tra sự cố (incident review).
    3. Lưu trữ dữ liệu tổng hợp (aggregates) trong 1 năm.
    4. Tuân thủ bảo mật: PII (thông tin cá nhân) phải được redact trước khi lưu vào Silver/Gold.
    5. **Ngân sách:** ≤ **$5,000/tháng** cho toàn bộ chi phí lưu trữ (storage).

---

## 2. Architecture Diagram

```text
[ LLM API Gateway ]
       |
       v (Streaming: Kafka / Kinesis)
       |
[ Spark Streaming Ingestion ]
       |
       +-----> BRONZE (S3 Standard - Partition by Date/Hour)
       |       - Raw JSON logs (Full PII)
       |       - TTL: 24 hours (for re-processing)
       |
[ Redaction & Parsing Job (Micro-batch 5m) ]
       |
       +-----> SILVER (S3 Standard/IA - Partition by Date, Z-ORDER by tenant_id)
       |       - Cleaned Schema, PII Redacted
       |       - TTL: 7 days (Incident Review path)
       |
[ Daily/Hourly Aggregator ]
       |
       +-----> GOLD (S3 Standard - Partition by Month)
               - Hourly Aggregates (cost_usd, latency_p95, tokens) by Tenant
               - Retention: 1 year
```

---

## 3. Quyết định kiến trúc & Alternatives

### Quyết định 1: Chọn Table Format là Delta Lake
- **Lựa chọn:** Delta Lake.
- **Tại sao:** Chúng ta cần `Z-ORDER` để tối ưu hóa việc query theo `tenant_id` trên hàng tỷ dòng dữ liệu mà không bị over-partitioning (do số lượng tenant có thể lên tới hàng chục nghìn). Delta Lake cũng hỗ trợ `Deletion Vectors` giúp xử lý yêu cầu xóa dữ liệu PII/GDPR nhanh chóng mà không cần rewrite toàn bộ Parquet file.
- **Loại bỏ:** 
    - **Apache Iceberg:** Dù hỗ trợ partition evolution tốt, nhưng `Z-ORDER` của Delta Lake hiện tại vẫn có performance vượt trội hơn cho các point-query theo tenant trong các dashboard thực tế.
    - **Hudi:** Phù hợp hơn cho các CDC workload phức tạp, quá overkill cho log-append only.

### Quyết định 2: Chiến lược Partitioning & Clustering
- **Lựa chọn:** Partition theo `date` + `Z-ORDER` theo `tenant_id`.
- **Tại sao:** Nếu partition theo `tenant_id`, chúng ta sẽ gặp lỗi "Small File Problem" trầm trọng (1B file/ngày). Sử dụng `Z-ORDER` cho phép gộp dữ liệu của cùng một tenant vào cùng một (hoặc ít) file Parquet, giúp File Pruning hiệu quả khi dashboard lọc theo tenant.
- **Loại bỏ:** 
    - **Partition theo tenant_id:** Gây ra metadata overhead khổng lồ cho Catalog (Glue/Unity).

### Quyết định 3: FinOps & Storage Lifecycle
- **Lựa chọn:** 
    - Bronze: Lưu 24h (S3 Standard), sau đó xóa.
    - Silver: Lưu 7 ngày (S3 Standard), move sang IA (Infrequent Access) nếu cần audit dài hơn, sau đó xóa.
    - Gold: Lưu 1 năm (S3 Standard).
- **Tại sao:** 5 TB/ngày = 150 TB/tháng. Nếu giữ Bronze 30 ngày, chi phí storage riêng S3 Standard sẽ là ~$3,500/tháng (vượt budget khi cộng thêm compute). Việc xóa Bronze sớm và chỉ giữ Silver 7 ngày là bắt buộc để duy trì budget $5K.
- **Loại bỏ:** Giữ toàn bộ raw logs trong 30 ngày (Chi phí S3 Standard sẽ là ~$7,000+, vỡ budget).

### Quyết định 4: PII Redaction tại Ingestion
- **Lựa chọn:** Redact tại bước chuyển từ Bronze sang Silver.
- **Tại sao:** Đảm bảo dữ liệu "nhạy cảm" chỉ tồn tại ở lớp Bronze (quyền truy cập cực kỳ hạn chế) và biến mất sau 24h. Silver là lớp để Analyst/SRE làm việc, phải sạch PII.
- **Loại bỏ:** Redact tại source (API Gateway). Trade-off: Làm tăng latency của API chính.

### Quyết định 5: Catalog Choice
- **Lựa chọn:** AWS Glue Catalog (hoặc Unity Catalog nếu dùng Databricks).
- **Tại sao:** Serverless, chi phí thấp, tích hợp sẵn với Athena/Trino để query dashboard mà không cần duy trì cluster 24/7.

---

## 4. Failure Modes (Kịch bản 3 giờ sáng)

1. **Schema Evolution Failure:** Một model mới update log thêm field lồng nhau khiến parsing job bị crash.
    - **Detection:** CloudWatch Alarm trên Spark Streaming failure rate.
    - **Rollback:** Sử dụng Delta Lake **Schema Merging** (`mergeSchema=true`) để tự động thích ứng hoặc redirect record lỗi sang một "Dead Letter Table" để fix tay mà không dừng pipeline.

2. **Z-ORDER Bottleneck:** Lượng dữ liệu quá lớn khiến việc chạy `OPTIMIZE` hàng giờ gây tốn chi phí compute quá mức.
    - **Detection:** Monitor chi phí compute theo tag `job:optimize`.
    - **Action:** Chuyển sang **Liquid Clustering** (Delta 3.0+) để tránh rewrite file quá nhiều lần mà vẫn giữ được clustering theo `tenant_id`.

3. **Storage Budget Spike:** Log tăng đột biến (ví dụ bị DDoS) khiến S3 cost vượt $5K.
    - **Detection:** S3 Storage Lens + Budget Alarm.
    - **Action:** Dùng Time Travel của Delta để xác định thời điểm log tăng ảo, thực hiện xóa hàng loạt (Vacuum) sớm hơn dự kiến cho lớp Bronze.

---

## 5. Ước tính chi phí (Back-of-envelope)

Giả định: 5 TB/ngày thô, sau khi nén (Snappy/Parquet) còn 2 TB/ngày.

- **Storage (S3):**
    - Bronze (1 ngày): 2 TB.
    - Silver (7 ngày): 14 TB.
    - Gold (1 năm): 1 TB (aggregates rất nhỏ).
    - Tổng: ~17 TB hot storage.
    - Chi phí: 17 TB × $23/TB = **~$400/tháng**.
- **Compute (Spot Instances):**
    - Ingestion & Redaction: $4/hour × 24h × 30 ngày = **~$2,880/tháng**.
    - Optimize & Vacuum: $2/hour × 24h × 30 ngày = **~$1,440/tháng**.
- **Tổng cộng:** ~$4,720/tháng (Nằm trong budget $5,000).

---

## 6. MVP (One-week slice)

Xây dựng pipeline thu nhỏ xử lý 1 triệu request/ngày:
1. Script generate mock LLM logs có chứa email/phone (PII).
2. Notebook thực hiện:
    - Ghi vào Bronze (Delta).
    - Chạy hàm Redaction (Regex/Presidio).
    - Ghi vào Silver với Z-ORDER theo `tenant_id`.
    - Query dashboard: So sánh tốc độ lọc theo tenant có Z-ORDER và không có Z-ORDER.

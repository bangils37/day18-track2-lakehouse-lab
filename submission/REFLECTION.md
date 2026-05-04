# Reflection - Day 18 Lakehouse Lab
**Học viên:** Nguyễn Bằng Anh
**Mã học viên:** 2A202600136

### Anti-pattern team dễ vướng nhất: Small-file problem

Trong quá trình xây dựng pipeline thực tế, team tôi dễ vướng nhất vào anti-pattern **Small-file problem**. 

**Lý do:**
Khi triển khai các hệ thống Streaming hoặc CDC (Change Data Capture) với tần suất ghi dữ liệu liên tục (micro-batch), mỗi lần commit sẽ tạo ra một file Parquet mới. Theo thời gian, số lượng file nhỏ sẽ tích tụ lên tới hàng nghìn, hàng triệu file. Điều này gây ra overhead cực lớn cho metadata và làm chậm quá trình quét dữ liệu (I/O) khi thực hiện các truy vấn phân tích.

**Giải pháp từ bài Lab:**
Qua bài Lab này, tôi đã thấy rõ sức mạnh của lệnh `OPTIMIZE` và `Z-ORDER`. Từ 200 file nhỏ ban đầu, sau khi nén lại còn 55 file và sắp xếp theo `user_id`, tốc độ truy vấn đã cải thiện tới **15.9 lần** (như kết quả trong NB2). Việc lên lịch định kỳ chạy compaction là bài học quan trọng nhất để duy trì hiệu năng cho Lakehouse của team.

namespace CoreService.Services
{
    public class SentimentAnalysisService
    {
        public (string Sentiment, string Action, string ReplyMessage) Analyze(string commentMessage)
        {
            if (string.IsNullOrEmpty(commentMessage)) 
                return ("Không xác định", "Bỏ qua", "");

            string lowerMsg = commentMessage.ToLower();

            // 1. Phân tích Spam (Ưu tiên cao nhất)
            if (lowerMsg.Contains("http") || lowerMsg.Contains("mua ngay") || lowerMsg.Contains("link lạ"))
            {
                return ("Spam", "Ẩn bình luận", "");
            }

            // 2. Phân tích Tích cực
            if (lowerMsg.Contains("tốt") || lowerMsg.Contains("tuyệt") || lowerMsg.Contains("ủng hộ") || lowerMsg.Contains("ok"))
            {
                return ("Tích cực", "Phản hồi", "Cảm ơn bạn đã ủng hộ shop!");
            }

            // 3. Phân tích Tiêu cực
            if (lowerMsg.Contains("tệ") || lowerMsg.Contains("thất vọng") || lowerMsg.Contains("chậm") || lowerMsg.Contains("lâu"))
            {
                return ("Tiêu cực", "Phản hồi", "Rất xin lỗi vì trải nghiệm chưa tốt, bên mình sẽ kiểm tra ngay.");
            }

            // 4. Trung tính
            return ("Trung tính", "Bỏ qua", "");
        }
    }
}
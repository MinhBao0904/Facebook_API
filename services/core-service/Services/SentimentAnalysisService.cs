namespace CoreService.Services
{
    public record AnalysisResult(string Sentiment, string Intent, string Action, string ReplyMessage, bool IsSevereSpam);

    public class SentimentAnalysisService
    {
        public AnalysisResult Analyze(string? commentMessage)
        {
            if (string.IsNullOrWhiteSpace(commentMessage))
            {
                return new AnalysisResult("unknown", "empty", "skip", "", false);
            }

            var message = commentMessage.ToLowerInvariant();

            if (ContainsAny(message, "http://", "https://", "bit.ly", "tinyurl", "scam", "lừa đảo", "lua dao", "kiếm tiền", "kiem tien"))
            {
                return new AnalysisResult("spam", "scam_or_malicious_link", "review", "", true);
            }

            if (ContainsAny(message, "mua ngay", "inbox mình", "ib mình", "link lạ", "spam", "quảng cáo", "quang cao"))
            {
                return new AnalysisResult("spam", "promotion_spam", "hide", "", false);
            }

            if (ContainsAny(message, "giá bao nhiêu", "bao nhiêu", "gia bao nhieu", "price", "tư vấn", "tu van"))
            {
                return new AnalysisResult("neutral", "pricing_question", "reply", "Cảm ơn bạn đã quan tâm. Bên mình sẽ tư vấn chi tiết ngay nhé!", false);
            }

            if (ContainsAny(message, "tệ", "te", "thất vọng", "that vong", "chậm", "cham", "lâu", "lau", "chưa nhận", "chua nhan", "khiếu nại", "khieu nai"))
            {
                return new AnalysisResult("negative", "complaint_or_support", "reply", "Rất xin lỗi vì trải nghiệm chưa tốt, bên mình sẽ kiểm tra và hỗ trợ bạn ngay.", false);
            }

            if (ContainsAny(message, "tốt", "tot", "tuyệt", "tuyet", "ủng hộ", "ung ho", "hay quá", "hay qua", "ok", "nhanh", "hài lòng", "hai long"))
            {
                return new AnalysisResult("positive", "positive_feedback", "reply", "Cảm ơn bạn đã ủng hộ shop!", false);
            }

            return new AnalysisResult("neutral", "general_comment", "skip", "", false);
        }

        private static bool ContainsAny(string value, params string[] keywords)
        {
            return keywords.Any(value.Contains);
        }
    }
}

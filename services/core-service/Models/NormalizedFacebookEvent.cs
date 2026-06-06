namespace CoreService.Models
{
    public class NormalizedFacebookEvent
    {
        public string? event_id { get; set; }
        public string? event_type { get; set; }
        public string? page_id { get; set; }
        public string? comment_id { get; set; }
        public string? user_id { get; set; }
        public string? message { get; set; }
        public string? verb { get; set; }
        public DateTimeOffset received_at { get; set; }
    }
}

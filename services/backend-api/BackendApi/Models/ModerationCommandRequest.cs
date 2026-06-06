namespace BackendApi.Models
{
    public class ModerationCommandRequest
    {
        public string? CommandId { get; set; }
        public string? CommentId { get; set; }
        public string? Action { get; set; }
        public string? ReplyMessage { get; set; }
        public string? Reason { get; set; }
    }
}

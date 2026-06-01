using System.Text.Json.Serialization;

namespace CoreService.Models
{
    public class FacebookWebhookEvent
    {
        public List<Entry> entry { get; set; }
    }

    public class Entry
    {
        public List<Change> changes { get; set; }
    }

    public class Change
    {
        public string field { get; set; }
        public Value value { get; set; }
    }

    public class Value
    {
        // Facebook ID của comment
        public string comment_id { get; set; } 
        // Nội dung comment
        public string message { get; set; }    
        // Hành động (ví dụ: "add", "remove")
        public string verb { get; set; }       
    }
}
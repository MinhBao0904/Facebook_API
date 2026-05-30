namespace BackendApi.Models
{
    public class ApiResponse<T>
    {
        public bool Success { get; set; }
        public int ErrorCode { get; set; }
        public string? Message { get; set; }
        public T? Data { get; set; }
    }
}
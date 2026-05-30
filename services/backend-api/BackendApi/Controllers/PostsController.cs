using BackendApi.Auth;
using BackendApi.Models;
using BackendApi.Services;
using Microsoft.AspNetCore.Mvc;

namespace BackendApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class PostsController : ControllerBase
    {
        private readonly IFacebookService _facebookService;

        public PostsController(IFacebookService facebookService)
        {
            _facebookService = facebookService;
        }

        [HttpGet("/posts")]
        public async Task<IActionResult> GetPosts()
        {
            var result = await _facebookService.GetPostsAsync();
            return StatusCode(result.ErrorCode == 0 ? 200 : result.ErrorCode, result);
        }

        [HttpPost("/post")]
        [ApiKeyAuth] 
        public async Task<IActionResult> CreatePost([FromBody] PostCreateRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.Message))
                return BadRequest(new ApiResponse<object> { Success = false, ErrorCode = 400, Message = "Nội dung bài viết không để trống" });

            var result = await _facebookService.CreatePostAsync(request.Message);
            return StatusCode(result.ErrorCode == 0 ? 200 : result.ErrorCode, result);
        }

        [HttpGet("/comments")]
        public async Task<IActionResult> GetComments([FromQuery] string post_id)
        {
            if (string.IsNullOrWhiteSpace(post_id))
                return BadRequest(new ApiResponse<object> { Success = false, ErrorCode = 400, Message = "Thiếu post_id" });

            var result = await _facebookService.GetCommentsAsync(post_id);
            return StatusCode(result.ErrorCode == 0 ? 200 : result.ErrorCode, result);
        }
    }
}
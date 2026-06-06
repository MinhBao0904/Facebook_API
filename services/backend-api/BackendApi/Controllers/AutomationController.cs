using BackendApi.Auth;
using BackendApi.Models;
using BackendApi.Services;
using Microsoft.AspNetCore.Mvc;

namespace BackendApi.Controllers
{
    [ApiController]
    [Route("automation")]
    [ApiKeyAuth]
    public class AutomationController : ControllerBase
    {
        private readonly IFacebookService _facebookService;

        public AutomationController(IFacebookService facebookService)
        {
            _facebookService = facebookService;
        }

        [HttpPost("reply")]
        public async Task<IActionResult> Reply([FromBody] ModerationCommandRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.CommandId) ||
                string.IsNullOrWhiteSpace(request.CommentId) ||
                string.IsNullOrWhiteSpace(request.ReplyMessage))
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    ErrorCode = 400,
                    Message = "CommandId, CommentId and ReplyMessage are required."
                });
            }

            var result = await _facebookService.ReplyToCommentAsync(request.CommandId, request.CommentId, request.ReplyMessage);
            return StatusCode(result.Success ? 200 : result.ErrorCode, result);
        }

        [HttpPost("hide")]
        public async Task<IActionResult> Hide([FromBody] ModerationCommandRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.CommandId) ||
                string.IsNullOrWhiteSpace(request.CommentId))
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    ErrorCode = 400,
                    Message = "CommandId and CommentId are required."
                });
            }

            var result = await _facebookService.HideCommentAsync(request.CommandId, request.CommentId, request.Reason ?? "automation");
            return StatusCode(result.Success ? 200 : result.ErrorCode, result);
        }
    }
}

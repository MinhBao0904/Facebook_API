using BackendApi.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;

namespace BackendApi.Auth
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Method)]
    public class ApiKeyAuthAttribute : Attribute, IAsyncActionFilter
    {
        private const string ApiKeyHeaderName = "X-API-KEY";

        public async Task OnActionExecutionAsync(ActionExecutingContext context, ActionExecutionDelegate next)
        {
            if (!context.HttpContext.Request.Headers.TryGetValue(ApiKeyHeaderName, out var extractedApiKey))
            {
                context.Result = new UnauthorizedObjectResult(new ApiResponse<object>
                {
                    Success = false,
                    ErrorCode = 401,
                    Message = "Missing API key."
                });
                return;
            }

            var configuration = context.HttpContext.RequestServices.GetRequiredService<IConfiguration>();
            var apiKey = configuration.GetValue<string>("AdminApiKey");

            if (string.IsNullOrWhiteSpace(apiKey) || apiKey != extractedApiKey)
            {
                context.Result = new ObjectResult(new ApiResponse<object>
                {
                    Success = false,
                    ErrorCode = 403,
                    Message = "Invalid API key."
                })
                {
                    StatusCode = 403
                };
                return;
            }

            await next();
        }
    }
}

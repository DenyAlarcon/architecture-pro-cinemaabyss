using System.Net.Http.Headers;

var builder = WebApplication.CreateBuilder(args);

var app = builder.Build();

var port = Environment.GetEnvironmentVariable("PORT");
if (string.IsNullOrWhiteSpace(port))
{
    port = "8000";
}
app.Urls.Add($"http://0.0.0.0:{port}");

var monolithUrl = Environment.GetEnvironmentVariable("MONOLITH_URL") ?? "http://localhost:8080";
var moviesServiceUrl = Environment.GetEnvironmentVariable("MOVIES_SERVICE_URL") ?? "http://localhost:8081";

var gradualMigration = bool.TryParse(Environment.GetEnvironmentVariable("GRADUAL_MIGRATION"), out var gradual) && gradual;
var migrationPercent = 0;
if (!int.TryParse(Environment.GetEnvironmentVariable("MOVIES_MIGRATION_PERCENT"), out migrationPercent))
{
    migrationPercent = 0;
}
if (migrationPercent < 0) migrationPercent = 0;
if (migrationPercent > 100) migrationPercent = 100;

var httpClient = new HttpClient
{
    Timeout = TimeSpan.FromSeconds(30)
};

app.MapGet("/health", () => Results.Text("Strangler Fig Proxy is healthy", "text/plain"));

app.MapMethods("/{**catchall}", new[] { "GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD" }, async (HttpContext context) =>
{
    var path = context.Request.Path.Value ?? string.Empty;
    var targetBase = monolithUrl;

    if (path.StartsWith("/api/movies", StringComparison.OrdinalIgnoreCase))
    {
        if (gradualMigration)
        {
            var roll = Random.Shared.Next(0, 100);
            targetBase = roll < migrationPercent ? moviesServiceUrl : monolithUrl;
        }
        else
        {
            targetBase = moviesServiceUrl;
        }
    }

    var targetUri = new Uri($"{targetBase.TrimEnd('/')}{path}{context.Request.QueryString}");

    using var requestMessage = new HttpRequestMessage(new HttpMethod(context.Request.Method), targetUri);

    if (context.Request.ContentLength > 0 || context.Request.Headers.ContainsKey("Transfer-Encoding"))
    {
        requestMessage.Content = new StreamContent(context.Request.Body);
        if (!string.IsNullOrWhiteSpace(context.Request.ContentType))
        {
            requestMessage.Content.Headers.ContentType = MediaTypeHeaderValue.Parse(context.Request.ContentType);
        }
    }

    foreach (var header in context.Request.Headers)
    {
        if (header.Key.Equals("Host", StringComparison.OrdinalIgnoreCase))
        {
            continue;
        }

        if (!requestMessage.Headers.TryAddWithoutValidation(header.Key, header.Value.ToArray()) && requestMessage.Content != null)
        {
            requestMessage.Content.Headers.TryAddWithoutValidation(header.Key, header.Value.ToArray());
        }
    }

    using var responseMessage = await httpClient.SendAsync(requestMessage, HttpCompletionOption.ResponseHeadersRead, context.RequestAborted);

    context.Response.StatusCode = (int)responseMessage.StatusCode;

    foreach (var header in responseMessage.Headers)
    {
        context.Response.Headers[header.Key] = header.Value.ToArray();
    }
    foreach (var header in responseMessage.Content.Headers)
    {
        context.Response.Headers[header.Key] = header.Value.ToArray();
    }

    context.Response.Headers.Remove("transfer-encoding");

    await responseMessage.Content.CopyToAsync(context.Response.Body);
});

app.Run();

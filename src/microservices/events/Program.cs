using System.Text.Json.Serialization;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IProducer<Null, string>>(sp =>
{
    var brokers = Environment.GetEnvironmentVariable("KAFKA_BROKERS") ?? "localhost:9092";
    var config = new ProducerConfig
    {
        BootstrapServers = brokers
    };
    return new ProducerBuilder<Null, string>(config).Build();
});

builder.Services.AddHostedService<KafkaConsumerService>();

var app = builder.Build();

var port = Environment.GetEnvironmentVariable("PORT");
if (string.IsNullOrWhiteSpace(port))
{
    port = "8082";
}
app.Urls.Add($"http://0.0.0.0:{port}");

app.MapGet("/api/events/health", () => Results.Json(new { status = true }));

app.MapPost("/api/events/movie", async (MovieEvent request, IProducer<Null, string> producer, CancellationToken ct) =>
{
    return await ProduceEvent("movie-events", "movie", request, producer, ct);
});

app.MapPost("/api/events/user", async (UserEvent request, IProducer<Null, string> producer, CancellationToken ct) =>
{
    return await ProduceEvent("user-events", "user", request, producer, ct);
});

app.MapPost("/api/events/payment", async (PaymentEvent request, IProducer<Null, string> producer, CancellationToken ct) =>
{
    return await ProduceEvent("payment-events", "payment", request, producer, ct);
});

app.Run();

static async Task<IResult> ProduceEvent<T>(string topic, string type, T payload, IProducer<Null, string> producer, CancellationToken ct)
{
    var evt = new EventEnvelope
    {
        Id = $"{type}-{Guid.NewGuid():N}",
        Type = type,
        Timestamp = DateTime.UtcNow,
        Payload = payload
    };

    var message = new Message<Null, string>
    {
        Value = System.Text.Json.JsonSerializer.Serialize(evt)
    };

    var result = await producer.ProduceAsync(topic, message, ct);

    var response = new EventResponse
    {
        Status = "success",
        Partition = result.Partition.Value,
        Offset = result.Offset.Value,
        Event = evt
    };

    return Results.Json(response, statusCode: StatusCodes.Status201Created);
}

sealed class KafkaConsumerService : BackgroundService
{
    private readonly ILogger<KafkaConsumerService> _logger;

    public KafkaConsumerService(ILogger<KafkaConsumerService> logger)
    {
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);
    }

    private void ConsumeLoop(CancellationToken stoppingToken)
    {
        var brokers = Environment.GetEnvironmentVariable("KAFKA_BROKERS") ?? "localhost:9092";
        var config = new ConsumerConfig
        {
            BootstrapServers = brokers,
            GroupId = "events-service",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe(new[] { "movie-events", "user-events", "payment-events" });

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(stoppingToken);
                    if (result?.Message?.Value != null)
                    {
                        _logger.LogInformation("Consumed from {Topic}: {Message}", result.Topic, result.Message.Value);
                    }
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Kafka consume error");
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            consumer.Close();
        }
    }
}

sealed class EventEnvelope
{
    [JsonPropertyName("id")]
    public string Id { get; set; } = string.Empty;

    [JsonPropertyName("type")]
    public string Type { get; set; } = string.Empty;

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; }

    [JsonPropertyName("payload")]
    public object? Payload { get; set; }
}

sealed class EventResponse
{
    [JsonPropertyName("status")]
    public string Status { get; set; } = "success";

    [JsonPropertyName("partition")]
    public int Partition { get; set; }

    [JsonPropertyName("offset")]
    public long Offset { get; set; }

    [JsonPropertyName("event")]
    public EventEnvelope Event { get; set; } = new();
}

sealed class MovieEvent
{
    [JsonPropertyName("movie_id")]
    public int MovieId { get; set; }

    [JsonPropertyName("title")]
    public string Title { get; set; } = string.Empty;

    [JsonPropertyName("action")]
    public string Action { get; set; } = string.Empty;

    [JsonPropertyName("user_id")]
    public int? UserId { get; set; }

    [JsonPropertyName("rating")]
    public double? Rating { get; set; }

    [JsonPropertyName("genres")]
    public string[]? Genres { get; set; }

    [JsonPropertyName("description")]
    public string? Description { get; set; }
}

sealed class UserEvent
{
    [JsonPropertyName("user_id")]
    public int UserId { get; set; }

    [JsonPropertyName("username")]
    public string? Username { get; set; }

    [JsonPropertyName("email")]
    public string? Email { get; set; }

    [JsonPropertyName("action")]
    public string Action { get; set; } = string.Empty;

    [JsonPropertyName("timestamp")]
    public DateTime? Timestamp { get; set; }
}

sealed class PaymentEvent
{
    [JsonPropertyName("payment_id")]
    public int PaymentId { get; set; }

    [JsonPropertyName("user_id")]
    public int UserId { get; set; }

    [JsonPropertyName("amount")]
    public double Amount { get; set; }

    [JsonPropertyName("status")]
    public string Status { get; set; } = string.Empty;

    [JsonPropertyName("timestamp")]
    public DateTime? Timestamp { get; set; }

    [JsonPropertyName("method_type")]
    public string? MethodType { get; set; }
}

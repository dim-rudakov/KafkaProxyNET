using Confluent.Kafka;
using System.Collections.Concurrent;

var queue = new ConcurrentQueue<string>();
var builder = WebApplication.CreateBuilder(args);

var configConsumer = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "debug-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true
};

builder.Host.UseWindowsService();

// Kafka singleton producer
builder.Services.AddSingleton<IProducer<string, string>>(_ =>
{
    var config = new ProducerConfig
    {
        BootstrapServers = "localhost:9092"
    };

    return new ProducerBuilder<string, string>(config).Build();
});

var app = builder.Build();

app.MapGet("/health", () => Results.Ok("ok"));

app.MapPost("/send", async (
    KafkaMessage dto,
    IProducer<string, string> producer) =>
{
    var result = await producer.ProduceAsync(
        "exchange1c",
        new Message<string, string>
        {
            Key = dto.Key,
            Value = dto.Value
        });

    return Results.Ok(new
    {
        result.Topic,
        result.Offset,
        result.Partition
    });
});

app.MapGet("/consume", async () =>
{
    var items = new List<string>();

    while (queue.TryDequeue(out var msg))
    {
        items.Add(msg);
    }
    return Results.Ok(items);
});

var consumer = new ConsumerBuilder<string, string>(configConsumer).Build();
consumer.Subscribe("exchange1c");

Task.Run(() =>
{
    while (true)
    {
        var msg = consumer.Consume();

        if (msg != null)
        {
            queue.Enqueue(msg.Message.Value);
        }
    }
});

app.Run();


record KafkaMessage(string Key, string Value);
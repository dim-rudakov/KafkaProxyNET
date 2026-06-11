using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = WebApplication.CreateBuilder(args);

// 👇 ключевое — запуск как Windows Service
builder.Host.UseWindowsService();

// добавляем HTTP
builder.Services.AddControllers();

var app = builder.Build();

app.MapGet("/", () => "KafkaProxy is running");

// пример endpoint
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.Run();
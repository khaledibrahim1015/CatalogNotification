using CatalogNotification.Api.Data;
using CatalogNotification.Api.Extensions;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Register DbContext with Npgsql provider
builder.Services.AddDbContext<CatalogDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Postgres")));

builder.Services.AddNatsCatalogServices(builder.Configuration);


builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    
    using var scope = app.Services.CreateScope();
    var dbContext = scope.ServiceProvider.GetRequiredService<CatalogDbContext>();
    await dbContext.SeedDataAsync();
    
    app.UseSwagger();
    app.UseSwaggerUI();
    
}





app.MapGet("/weatherforecast", () =>
    {

    })
    .WithName("GetWeatherForecast")
    .WithOpenApi();




app.Run();


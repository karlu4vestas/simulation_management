using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using Microsoft.FluentUI.AspNetCore.Components;
using System.Net.Http.Json;
using VSM.Client;
using VSM.Client.SharedAPI;
using VSM.Client.Datamodel;

// 1. Create the Blazor WebAssembly host with DI container and default services
var builder = WebAssemblyHostBuilder.CreateDefault(args);

// 2. Register root components
//    - <App /> mounts into <div id="app">
//    - <HeadOutlet /> allows dynamic <head> updates (titles, meta tags)
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

// 3. Load runtime configuration from wwwroot/config.json
//    - Allows dynamic backend URL without recompiling
// Start with the host environment base address as a safe default (may be empty).
string backendUrl = builder.HostEnvironment.BaseAddress ?? string.Empty; // start with fallback (may be empty)
try
{
    var hostBase = builder.HostEnvironment.BaseAddress;
    if (!string.IsNullOrEmpty(hostBase))
    {
        using var tempClient = new HttpClient { BaseAddress = new Uri(hostBase) };
        // If config.json or BackendUrl is missing, keep the default value.
        backendUrl = (await tempClient.GetFromJsonAsync<AppConfig>("config.json"))?.BackendUrl ?? backendUrl;
    }
}
catch
{
    // keep default backendUrl
}

// 4. Register services for DI
var finalBackendUrl = !string.IsNullOrEmpty(backendUrl)
    ? backendUrl
    : (builder.HostEnvironment.BaseAddress ?? string.Empty);
builder.Services.AddSingleton(new AppConfig { BackendUrl = finalBackendUrl });            // app-wide configuration
builder.Services.AddScoped(sp =>
{
    return !string.IsNullOrEmpty(finalBackendUrl)
        ? new HttpClient { BaseAddress = new Uri(finalBackendUrl) }
        : new HttpClient();
});


// Add FluentUI components (JS/CSS interop for UI)
builder.Services.AddFluentUIComponents();

// register typed API client if needed
builder.Services.AddScoped<API>();

builder.Services.AddScoped<NavigationDataService>();


// 5. Build and run the WASM app
await builder.Build().RunAsync();

// Runtime configuration class
public class AppConfig
{
    public string? BackendUrl { get; set; }
}
public class NavigationDataService
{
    public RootFolder? CurrentRootFolder { get; set; }
}

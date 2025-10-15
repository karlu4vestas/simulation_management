# Debugging JSON Deserialization Issues

## Problem Identified
The `RootFoldersByDomainUser` was returning zero items even though the Python API was returning data. The root cause was a property name mismatch between the Python DTOs and C# DTOs.

## Changes Made

### 1. Added JSON Inspection Logging in API (`VSM.Client/API/api.cs`)
The `RootFoldersByDomainUser` method now:
- Fetches the raw JSON response first
- Logs it to the console with clear markers
- Then deserializes it
- Reports the count of deserialized objects

This allows you to inspect the exact JSON before deserialization to identify naming mismatches.

**Look for output like:**
```
=== RAW JSON RESPONSE FOR RootFoldersByDomainUser ===
[{"id": 1, "simulationdomain_id": 1, ...}]
=== END RAW JSON ===
Deserialized 0 RootFolderDTO objects
```

### 2. Fixed Property Name Mismatches in `RootFolderDTO` (`VSM.Client/Models/DTOs.cs`)

**Before:**
```csharp
public int SimulationDomain_Id { get; set; }
public int CycleTime { get; set; }
public int CleanupFrequency { get; set; }
```

**After:**
```csharp
public int Simulationdomain_Id { get; set; }  // Matches Python: simulationdomain_id
public int Cycletime { get; set; }            // Matches Python: cycletime
public int Cleanupfrequency { get; set; }     // Matches Python: cleanupfrequency
public DateTime? Cleanup_Round_Start_Date { get; set; }  // Added missing property
```

The property names now match the Python DTO fields with `PropertyNameCaseInsensitive = true` handling the casing.

### 3. Updated References in `RootFolder` class (`VSM.Client/Models/rootfolder.cs`)

Updated property accessors to use the new property names:
- `dto.CleanupFrequency` → `dto.Cleanupfrequency`
- `dto.CycleTime` → `dto.Cycletime`

## How to Debug Similar Issues in the Future

1. **Check Console Output**: When the API call returns 0 items unexpectedly, check the console for the raw JSON output
2. **Compare Property Names**: Match the JSON field names with the C# DTO property names
3. **Remember Case Insensitivity**: `PropertyNameCaseInsensitive = true` helps, but underscores and word boundaries still need to match
4. **Add Similar Logging**: Use the same pattern for other problematic endpoints:

```csharp
HttpResponseMessage response = await httpClient.GetAsync(requestUrl);
response.EnsureSuccessStatusCode();

string jsonResponse = await response.Content.ReadAsStringAsync();
Console.WriteLine($"=== RAW JSON FOR {endpointName} ===");
Console.WriteLine(jsonResponse);
Console.WriteLine("=== END RAW JSON ===");

var result = System.Text.Json.JsonSerializer.Deserialize<YourType>(jsonResponse, options);
Console.WriteLine($"Deserialized {result?.Count ?? 0} objects");
```

## Testing
To verify the fix works:
1. Run the application
2. Navigate to a page that calls `RootFoldersByDomainUser`
3. Check the console output - you should see the raw JSON and a non-zero deserialized count
4. Verify that root folders are now displayed correctly

## Python DTO Reference
For reference, the Python `RootFolderDTO` has these fields:
- `simulationdomain_id: int`
- `folder_id: int | None`
- `owner: str`
- `approvers: str`
- `path: str`
- `cycletime: int`
- `cleanupfrequency: int`
- `cleanup_round_start_date: date | None`

C# properties must match these names (with case insensitivity applied).

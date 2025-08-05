#!/bin/bash
# Alternative HTTP Launch Script - Port 5002
echo "🚀 Starting VSM on Alternative HTTP (Port 5002)..."
echo "📍 URL: http://localhost:5002 or http://127.0.0.1:5002"
echo ""

# Kill any existing processes
./kill_process.sh

echo ""
echo "🔄 Starting application..."
dotnet run --project VSM/VSM.csproj --launch-profile VSM_HTTP_Alt

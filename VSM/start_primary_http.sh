#!/bin/bash
# Primary HTTP Launch Script - Port 5000
echo "🚀 Starting VSM on Primary HTTP (Port 5000)..."
echo "📍 URL: http://localhost:5000 or http://127.0.0.1:5000"
echo ""

# Kill any existing processes
./kill_process.sh

echo ""
echo "🔄 Starting application..."
dotnet run --project VSM/VSM.csproj --launch-profile VSM_HTTP

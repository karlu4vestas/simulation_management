#!/bin/bash
# Manual HTTP Launch Script - Port 5003
echo "🚀 Starting VSM on Manual HTTP (Port 5003)..."
echo "📍 URL: http://localhost:5003 or http://127.0.0.1:5003"
echo ""

# Kill any existing processes
./kill_process.sh

echo ""
echo "🔄 Starting application with manual URL configuration..."
dotnet run --project VSM/VSM.csproj --urls "http://localhost:5003;http://127.0.0.1:5003"

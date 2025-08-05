#!/bin/bash
# Kill any dotnet processes running on development ports
echo "🔍 Checking for processes on development ports (5000-5010, 7000)..."

# Check for processes on common development ports
PORTS="5000,5001,5002,5003,5004,5005,7000"
PIDS=$(lsof -ti :$PORTS 2>/dev/null)

if [ ! -z "$PIDS" ]; then
    echo "🔥 Found processes using ports: $PIDS"
    echo "💀 Killing processes..."
    
    # First try graceful termination
    echo $PIDS | xargs kill 2>/dev/null
    sleep 2
    
    # Check if any are still running and force kill them
    REMAINING_PIDS=$(lsof -ti :$PORTS 2>/dev/null)
    if [ ! -z "$REMAINING_PIDS" ]; then
        echo "🔨 Force killing remaining processes: $REMAINING_PIDS"
        echo $REMAINING_PIDS | xargs kill -9 2>/dev/null
    fi
    
    echo "✅ All processes killed successfully."
else
    echo "✅ No processes found on development ports."
fi

# Show current port usage for verification
echo ""
echo "📊 Current port usage on development ports:"
lsof -i :5000-5010,7000 2>/dev/null || echo "No processes found on development ports"

#!/bin/bash
# Quick Start Menu for VSM HTTP Options
echo "🌐 VSM HTTP Launch Options"
echo "=========================="
echo ""
echo "Choose your preferred HTTP option:"
echo ""
echo "1) Primary HTTP    - Port 5000  (Launch Profile: VSM_HTTP)"
echo "2) Alternative HTTP - Port 5002  (Launch Profile: VSM_HTTP_Alt)"
echo "3) Manual HTTP     - Port 5003  (Manual URL configuration)"
echo "4) HTTPS           - Port 5001  (Launch Profile: VSM_HTTPS)"
echo "5) Kill all processes and exit"
echo ""
read -p "Enter your choice (1-5): " choice

case $choice in
    1)
        echo "🚀 Starting Primary HTTP..."
        ./start_primary_http.sh
        ;;
    2)
        echo "🚀 Starting Alternative HTTP..."
        ./start_alt_http.sh
        ;;
    3)
        echo "🚀 Starting Manual HTTP..."
        ./start_manual_http.sh
        ;;
    4)
        echo "🔒 Starting HTTPS..."
        ./kill_process.sh
        echo ""
        echo "🔄 Starting application..."
        dotnet run --project VSM/VSM.csproj --launch-profile VSM_HTTPS
        ;;
    5)
        echo "🛑 Killing all processes..."
        ./kill_process.sh
        echo "✅ Done!"
        ;;
    *)
        echo "❌ Invalid choice. Please run the script again and choose 1-5."
        ;;
esac

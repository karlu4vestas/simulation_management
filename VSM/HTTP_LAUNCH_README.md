# VSM HTTP Launch Options

This document explains the different ways to start the VSM application using HTTP.

## 🚀 Quick Start Options

### Option 1: Primary HTTP (Port 5000)
```bash
# Command line
dotnet run --launch-profile VSM_HTTP

# Or use the script
./start_primary_http.sh

# Or use VS Code Task: "Primary HTTP - Port 5000"
```
**URLs:** `http://localhost:5000` or `http://127.0.0.1:5000`

### Option 2: Alternative HTTP (Port 5002)
```bash
# Command line
dotnet run --launch-profile VSM_HTTP_Alt

# Or use the script
./start_alt_http.sh

# Or use VS Code Task: "Alternative HTTP - Port 5002"
```
**URLs:** `http://localhost:5002` or `http://127.0.0.1:5002`

### Option 3: Manual HTTP (Port 5003)
```bash
# Command line
dotnet run --urls "http://localhost:5003;http://127.0.0.1:5003"

# Or use the script
./start_manual_http.sh

# Or use VS Code Task: "Manual HTTP - Port 5003"
```
**URLs:** `http://localhost:5003` or `http://127.0.0.1:5003`

## 🎯 All Launch Methods Summary

### **Method 1: Command Line**
```bash
dotnet run --launch-profile VSM_HTTP          # Port 5000 (localhost + 127.0.0.1)
dotnet run --launch-profile VSM_HTTP_Alt      # Port 5002 (localhost + 127.0.0.1)
dotnet run --urls "http://localhost:5003;http://127.0.0.1:5003"  # Port 5003 (explicit binding)
```

### **Method 2: Shell Scripts**
```bash
./start_primary_http.sh    # Port 5000
./start_alt_http.sh        # Port 5002
./start_manual_http.sh     # Port 5003
./start_vsm.sh            # Interactive menu
```

### **Method 3: VS Code Tasks** (`Ctrl+Shift+P` → `Tasks: Run Task`)
- Primary HTTP - Port 5000
- Alternative HTTP - Port 5002
- Manual HTTP - Port 5003

### **Method 4: VS Code Launch/Debug** (`F5` or `Ctrl+Shift+D`)
- 🌐 Primary HTTP - Port 5000
- 🔄 Alternative HTTP - Port 5002
- ⚙️ Manual HTTP - Port 5003
- 🔒 HTTPS - Port 5001

### **Method 5: Interactive Menu**
```bash
./start_vsm.sh
```
Then choose option 1, 2, or 3!

## 🔧 VS Code Integration

### Auto Port Forwarding 🔗
VS Code will automatically forward and open the following ports:
- **Port 5000** - VSM Development Server (opens in browser automatically)
- **Port 5001** - VSM HTTPS Server (opens in browser automatically)  
- **Port 5002** - VSM Staging/Test Server (opens in browser automatically)
- **Port 5003** - VSM Debug/Custom Server (opens in browser automatically)

*Note: Port forwarding works automatically in VS Code, GitHub Codespaces, and other remote development environments.*

### VS Code Tasks
Use `Ctrl+Shift+P` → `Tasks: Run Task` and choose:
- **Primary HTTP - Port 5000**
- **Alternative HTTP - Port 5002**
- **Manual HTTP - Port 5003**
- **Run VSM HTTPS - Port 5001**
- **Kill Development Processes**

### VS Code Launch Configurations (F5 Debugging)
Use `F5` or `Ctrl+Shift+D` → Run and Debug, then select:

**Individual Configurations:**
- **🌐 Primary HTTP - Port 5000** (with debugging)
- **🔄 Alternative HTTP - Port 5002** (with debugging)
- **⚙️ Manual HTTP - Port 5003** (with debugging)
- **🔒 HTTPS - Port 5001** (with debugging)

**Compound Configurations (Server + Browser Debugging):**
- **🚀 Launch Primary HTTP + Browser Debug**
- **🚀 Launch Alternative HTTP + Browser Debug**
- **🚀 Launch Manual HTTP + Browser Debug**

### VS Code Browser Debugging
Launch browser debugging configurations:
- **Debug Blazor WebAssembly in Chrome (Port 5000)**
- **Debug Blazor WebAssembly in Chrome (Port 5002)**
- **Debug Blazor WebAssembly in Chrome (Port 5003)**

## 🛠️ Troubleshooting

### Port Already in Use
If you get a port binding error, run:
```bash
./kill_process.sh
```

### Which Option to Choose?
- **Primary HTTP (5000)**: Standard development port, use this most of the time
- **Alternative HTTP (5002)**: Use when port 5000 is busy
- **Manual HTTP (5003)**: Use when you need complete control over the URL configuration

## 📁 File Structure
```
VSM/
├── start_primary_http.sh    # Primary HTTP launcher
├── start_alt_http.sh        # Alternative HTTP launcher  
├── start_manual_http.sh     # Manual HTTP launcher
├── start_vsm.sh            # Interactive menu
├── kill_process.sh         # Process killer utility
└── .vscode/
    └── tasks.json          # VS Code tasks
```

## 🔍 Verification
After starting, verify the application is running:
```bash
# Check if the port is listening
lsof -i :5000  # or :5002, :5003 depending on your choice

# Check the application logs
# (Look for "Now listening on: http://localhost:XXXX")
```

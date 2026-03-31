@echo off
echo Starting Howard Stock Analysis Web App...

echo [1/2] Launching Python Backend API (Port 8000)...
start "Backend API" cmd /c "cd backend && ..\.venv\Scripts\python -m uvicorn server:app --host 127.0.0.1 --port 8000"

echo [2/2] Launching Nuxt Dashboard (Port 3000)...
start "Nuxt Dashboard" cmd /c "cd frontend && npm run dev"

echo Both services are booting up in separate terminal windows!
echo Once they load, you can access the dashboard at: http://127.0.0.1:3000
pause

@echo off
chcp 65001 >nul
echo ===================================================
echo   CarrotQuant.Data Fullstack Dev Server
echo   Backend : http://127.0.0.1:8888 (Python Reload)
echo   Frontend: http://localhost:5173  (Vite HMR)
echo ===================================================
echo.

echo [1/2] Starting Python REST API Server (Reload)...
start "CQData-Backend" cmd /k "uv run cqdata server --port 8888 --reload"

echo [2/2] Starting React Web Frontend (Vite HMR)...
start "CQData-Frontend" cmd /k "cd /d "%~dp0web" && bun dev"

echo.
echo [+] Fullstack dev environment started successfully!
echo.

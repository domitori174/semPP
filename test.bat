@echo off
chcp 65001 > nul
echo ===================================================
echo   Запуск автоматического тестирования масштабируемости
echo ===================================================

echo.
echo [1/3] Переключение на 1 lookup-worker...
docker compose up -d --scale lookup-worker=1
echo Ожидание 3 секунды для стабилизации контейнеров...
timeout /t 3 > nul
echo Запуск теста для 1 воркера:
python loadtest\loadtest.py

echo.
echo [2/3] Переключение на 2 lookup-worker...
docker compose up -d --scale lookup-worker=2
echo Ожидание 3 секунды...
timeout /t 3 > nul
echo Запуск теста для 2 воркеров:
python loadtest\loadtest.py

echo.
echo [3/3] Переключение на 4 lookup-worker...
docker compose up -d --scale lookup-worker=4
echo Ожидание 3 секунды...
timeout /t 3 > nul
echo Запуск теста для 4 воркеров:
python loadtest\loadtest.py

echo.
echo Восстановление исходного состояния (2 воркера)...
docker compose up -d --scale lookup-worker=2

echo.
echo Тестирование завершено! Соберите данные из вывода выше для вашей таблицы.
pause
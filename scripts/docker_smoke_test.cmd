@echo off
rem Docker smoke test for the pipeline scaffold (owner: Ege).
rem Double-click this file, or run it from a terminal:  scripts\docker_smoke_test.cmd
rem It builds the images, starts the stack, loads the SYNTHETIC fixture into bronze,
rem runs the dbt DAG once and writes everything to docker_smoke_test.log in the repo root.
rem The stack is left running afterwards. Airflow UI: http://localhost:8080
rem Reset everything with:  docker compose down -v
setlocal
cd /d "%~dp0\.."
set LOG=docker_smoke_test.log
set ENVFILE=.env
if not exist .env set ENVFILE=.env.example
set DC=docker compose --env-file %ENVFILE%
set FAILED=0
set TRIES=0

echo Writing output to %CD%\%LOG%
echo The first run can take 10 to 20 minutes.
echo === smoke test started %DATE% %TIME% env file %ENVFILE% > %LOG%

echo Running: docker version
echo === STEP docker version >> %LOG%
docker version >> %LOG% 2>&1
if errorlevel 1 goto :fail_early
docker compose version >> %LOG% 2>&1

echo Running: compose config
echo === STEP compose config >> %LOG%
%DC% config -q >> %LOG% 2>&1
if errorlevel 1 goto :fail_early

echo Running: build
echo === STEP build >> %LOG%
%DC% build >> %LOG% 2>&1
if errorlevel 1 goto :fail_early

echo Running: up
echo === STEP up >> %LOG%
%DC% up -d >> %LOG% 2>&1
if errorlevel 1 goto :fail_diag

echo Waiting for the Airflow scheduler
echo === STEP wait for scheduler >> %LOG%
:waitloop
%DC% exec -T airflow-scheduler airflow db check >nul 2>&1
if not errorlevel 1 goto :ready
set /a TRIES+=1
if %TRIES% GEQ 40 goto :notready
ping -n 11 127.0.0.1 >nul
goto :waitloop
:notready
echo scheduler not ready after 40 tries >> %LOG%
goto :fail_diag
:ready
echo scheduler ready after %TRIES% retries >> %LOG%

echo Running: bronze tables check
echo === STEP bronze tables after init >> %LOG%
%DC% exec -T airflow-scheduler sh -c "python $SCRIPTS_DIR/smoke_check.py" >> %LOG% 2>&1
if errorlevel 1 set FAILED=1

echo Running: load synthetic fixture
echo === STEP load synthetic fixture >> %LOG%
if not exist data\_smoke mkdir data\_smoke
copy /Y tests\fixtures\rik_synthetic_bundle.txt data\_smoke\rik_synthetic_bundle.txt >nul
%DC% exec -T airflow-scheduler sh -c "python $SCRIPTS_DIR/load_bronze.py --bundle $DATA_DIR/_smoke/rik_synthetic_bundle.txt" >> %LOG% 2>&1
if errorlevel 1 set FAILED=1

echo === STEP bronze row counts >> %LOG%
%DC% exec -T airflow-scheduler sh -c "python $SCRIPTS_DIR/smoke_check.py" >> %LOG% 2>&1
if errorlevel 1 set FAILED=1

echo Running: dbt debug
echo === STEP dbt debug >> %LOG%
%DC% exec -T airflow-scheduler sh -c "dbt debug --project-dir $DBT_PROJECT_DIR --profiles-dir $DBT_PROFILES_DIR" >> %LOG% 2>&1
if errorlevel 1 set FAILED=1

echo Running: airflow dags list
echo === STEP airflow dags list >> %LOG%
%DC% exec -T airflow-scheduler airflow dags list >> %LOG% 2>&1
if errorlevel 1 set FAILED=1
echo === STEP airflow import errors >> %LOG%
%DC% exec -T airflow-scheduler airflow dags list-import-errors >> %LOG% 2>&1

echo Running: airflow dags test dbt_refresh_gold
echo === STEP dags test dbt_refresh_gold >> %LOG%
%DC% exec -T airflow-scheduler airflow dags test dbt_refresh_gold >> %LOG% 2>&1
if errorlevel 1 set FAILED=1

echo Running: dbt service from the tools profile
echo === STEP dbt tools profile >> %LOG%
%DC% --profile tools run --rm dbt test >> %LOG% 2>&1
if errorlevel 1 set FAILED=1

echo === STEP compose ps >> %LOG%
%DC% ps -a >> %LOG% 2>&1
if %FAILED%==1 goto :fail_diag
echo === RESULT: ALL STEPS PASSED >> %LOG%
echo ALL STEPS PASSED
goto :done

:fail_diag
echo === compose ps >> %LOG%
%DC% ps -a >> %LOG% 2>&1
echo === last container logs >> %LOG%
%DC% logs --tail 80 >> %LOG% 2>&1
:fail_early
echo === RESULT: AT LEAST ONE STEP FAILED >> %LOG%
echo AT LEAST ONE STEP FAILED

:done
echo === finished %DATE% %TIME% >> %LOG%
echo Log: %CD%\%LOG%
pause

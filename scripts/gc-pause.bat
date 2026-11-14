@echo off
rem Runs GcPauseBenchmark once per collector and saves the output and the JVM's own GC log.
rem Usage (from the project folder, after "mvn -q compile"):   scripts\gc-pause.bat [heap] [entries] [threads]
rem   heap     JVM heap size, default 2g (used for both -Xms and -Xmx)
rem   entries  cache entries, default 2000000
rem   threads  worker threads, default 4
rem Results go to gc-results\ ; nothing is a measurement until you have run it on your machine.
setlocal
set HEAP=%1
if "%HEAP%"=="" set HEAP=2g
set ENTRIES=%2
if "%ENTRIES%"=="" set ENTRIES=2000000
set THREADS=%3
if "%THREADS%"=="" set THREADS=4
if not exist gc-results mkdir gc-results
for %%C in (G1GC ParallelGC ZGC) do (
  echo ==== %%C, heap %HEAP%, %ENTRIES% entries
  java -Xms%HEAP% -Xmx%HEAP% -XX:+Use%%C "-Xlog:gc,gc+phases:file=gc-results\gc-%%C.log" -cp target\classes com.distri.cache.GcPauseBenchmark --entries %ENTRIES% --seconds 60 --threads %THREADS% > gc-results\run-%%C.txt
  type gc-results\run-%%C.txt
  java -cp target\classes com.distri.cache.GcPauseBenchmark --summarize-gc-log gc-results\gc-%%C.log > gc-results\pauses-%%C.txt
  type gc-results\pauses-%%C.txt
)
echo.
echo Done. Send me the three run-*.txt and three pauses-*.txt files in gc-results\ (or paste them).
endlocal

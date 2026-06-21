Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = "D:\code\spark-movie-exp"
WshShell.Run ".\.venv\Scripts\python.exe run_debug.py", 0, False

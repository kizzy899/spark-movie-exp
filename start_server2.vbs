Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = "D:\code\spark-movie-exp"
WshShell.Run ".\.venv\Scripts\pythonw.exe run.py", 0, False

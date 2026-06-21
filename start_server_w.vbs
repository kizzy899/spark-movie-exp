Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = "D:\code\spark-movie-exp"
WshShell.Run "D:\code\spark-movie-exp\.venv\Scripts\pythonw.exe D:\code\spark-movie-exp\run.py", 0, False

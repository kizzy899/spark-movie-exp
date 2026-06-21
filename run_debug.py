import sys, os, traceback, time

base_dir = os.path.dirname(os.path.abspath(__file__))
logs_dir = os.path.join(base_dir, 'logs')
os.makedirs(logs_dir, exist_ok=True)
log_file = os.path.join(logs_dir, 'debug_crash.log')
try:
    from app import app
    with open(log_file, 'a') as f:
        f.write(f'{time.strftime("%H:%M:%S")} Starting Flask...\n')
        f.flush()
    
    app.run(host='0.0.0.0', port=5001, debug=False)
    
    with open(log_file, 'a') as f:
        f.write(f'{time.strftime("%H:%M:%S")} Flask returned (exiting normally)\n')
except Exception as e:
    with open(log_file, 'a') as f:
        f.write(f'{time.strftime("%H:%M:%S")} CRASH: {e}\n')
        traceback.print_exc(file=f)
        f.flush()

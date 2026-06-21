import sys, os, subprocess

DETACHED = 0x00000008
CREATE_NO_WINDOW = 0x08000000

launcher = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'run.py')
logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(logs_dir, exist_ok=True)

env = os.environ.copy()
if 'PATH' in env and 'Path' in env:
    env['PATH'] = env['PATH'].rstrip(';') + ';' + env['Path']

stdout_log = open(os.path.join(logs_dir, 'server_out.log'), 'a', encoding='utf-8')
stderr_log = open(os.path.join(logs_dir, 'server_err.log'), 'a', encoding='utf-8')
try:
    p = subprocess.Popen(
        [sys.executable, launcher],
        cwd=os.path.dirname(os.path.abspath(__file__)),
        env=env,
        creationflags=DETACHED | CREATE_NO_WINDOW,
        close_fds=True,
        stdout=stdout_log,
        stderr=stderr_log,
    )
finally:
    stdout_log.close()
    stderr_log.close()
print(f'Launched PID: {p.pid}')

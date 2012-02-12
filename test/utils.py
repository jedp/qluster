import os
import subprocess
import tempfile

CONF_TEMPLATE = """\
port %d
bind 127.0.0.1
daemonize no
timeout 0
dbfilename %s
dir %s
"""

def start_redis_server(port):
    configpath = tempfile.mktemp(suffix='.conf')
    dbfilepath = tempfile.mktemp(suffix='.db')
    dbdir, dbfilename = os.path.split(dbfilepath)

    open(configpath, 'w').write(CONF_TEMPLATE % (
        port, dbfilename, dbdir))
    proc = subprocess.Popen(["redis-server", configpath])
    return proc, configpath, dbfilepath

def stop_redis_server(popen, configpath, dbfilepath):
    os.kill(popen.pid, subprocess.signal.SIGINT)
    for filepath in (configpath, dbfilepath):
        if os.path.exists(filepath):
            os.unlink(filepath)


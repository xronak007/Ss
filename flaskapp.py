from flask import Flask
import subprocess
import threading

app = Flask(__name__)

process = None

def run_app_py():
    global process
    process = subprocess.Popen(['python3', 'app.py'])

@app.route('/')
def status():
    global process
    if process is None:
        return "Скрипт app.py не запущен"
    retcode = process.poll()
    if retcode is None:
        return "Скрипт app.py работает"
    else:
        return f"Скрипт app.py не работает (код выхода {retcode})"

if __name__ == '__main__':
    threading.Thread(target=run_app_py, daemon=True).start()
    app.run(host='0.0.0.0', port=5000, debug=True)
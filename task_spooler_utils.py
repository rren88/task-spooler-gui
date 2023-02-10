import glob
import os
import subprocess
import pandas as pd
from io import StringIO

TASK_SPOOLER_CMD = "tsp"


def list_jobs(socket_name):
    """
    List task-spooler jobs.
    """
    env = get_env(socket_name)
    output = subprocess.check_output(
        f"{TASK_SPOOLER_CMD} -l", env=env, shell=True, encoding="utf-8"
    )

    return parse_list_string(output)

def parse_list_string(list_string):
    """
    """
    lines = list_string.split("\n")
    data = []
    for line in lines[1:]:
        tokens = line.split()
        if len(tokens) == 0:
            break
        print(tokens)
        job_id = tokens[0]
        state = tokens[1]
        output = tokens[2]
        try:
            e_level = str(int(tokens[3]))
            i = 4
        except ValueError:
            e_level = ""
            i = 3
        try:
            time_ms = str(float(tokens[i].split("/")[0]) * 1000)
            i = i+1
        except ValueError:
            time_ms = ""
        command = " ".join(tokens[i:])
        job_info = [job_id, state, output, e_level, time_ms, command]
        print(job_info)
        data.append(job_info)
    print(data)
    df = pd.DataFrame(data, columns=["ID", "State", "Output", "E-Level", "Time_ms", "Command"])
    df = df.set_index("ID", drop=False).sort_index()
    return df


def tsp_remove(job_id, socket_name):
    """
    Remove a task-spooler job.
    """
    assert isinstance(job_id, int) or (
        isinstance(job_id, str) and job_id.isdigit()
    ), job_id
    env = get_env(socket_name)
    proc = subprocess.run(
        f"{TASK_SPOOLER_CMD} -r {job_id}",
        env=env,
        shell=True,
        encoding="utf-8",
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
    )
    return proc


def tsp_kill(job_id, socket_name):
    """
    Kill a running task-spooler job.
    """
    assert isinstance(job_id, int) or (
        isinstance(job_id, str) and job_id.isdigit()
    ), job_id
    env = get_env(socket_name)
    proc = subprocess.run(
        f"{TASK_SPOOLER_CMD} -k {job_id}",
        env=env,
        shell=True,
        encoding="utf-8",
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
    )
    return proc


def get_socket_names():
    """
    Get the names of all sockets in the /tmp directory.
    """
    sockets = glob.glob("/tmp/socket.*")
    socket_names = [os.path.basename(s)[len("socket.") :] for s in sockets]
    return socket_names


def get_env(socket_name):
    """
    Get environment variables for running task-spooler.
    """
    env = {}
    if socket_name is not None:
        env["TS_SOCKET"] = f"/tmp/socket.{socket_name}"
    return env

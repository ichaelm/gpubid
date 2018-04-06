import gpubid.crypto_price as crypto_price
import gpubid.gpu_scheduler_runnable as gpu_scheduler_runnable
import gpubid.gpu_scheduler_state_connection as gpu_scheduler_state_connection
import docker
import flask
import flask_api
import threading

ETH_RATE = 0.05/40/24/60/60
LOCAL_TASK_USD_PER_SEC = 1

docker_api = docker.from_env()

app = flask_api.FlaskAPI(__name__)


@app.route("/run_task/", methods=['POST'])
def handle_run_task():
    eth_price = crypto_price.query_price_usd('ETH')
    usd_rate = ETH_RATE * eth_price

    task_dict = flask.request.data
    task_name = task_dict.get('name', '')
    task_mount_path = task_dict.get('mount_path')
    try:
        task_cmd = task_dict['cmd']
    except IndexError:
        raise flask_api.exceptions.ParseError(detail='Request is missing the required "cmd" parameter')
    task_preemptable = task_dict.get('preemptable', False)
    task_priority = int(task_dict.get('priority', 0))
    state = gpu_scheduler_state_connection.GpuSchedulerStateConnection()
    state.add_local_task(gpu_scheduler_state_connection.Task(
        name=task_name,
        cmd=task_cmd,
        mount_path=task_mount_path,
        is_preemptable=task_preemptable,
        priority=task_priority,
        gpu_type_to_usd_per_sec=lambda x: LOCAL_TASK_USD_PER_SEC
    ))

    return {'id': True}, flask_api.status.HTTP_201_CREATED


def main():
    global_gpu_scheduler = gpu_scheduler_runnable.GpuSchedulerRunnable()
    scheduler_thread = threading.Thread(target=global_gpu_scheduler.start_or_resume, daemon=True)
    scheduler_thread.start()
    app.run(host='0.0.0.0', port=8000)


if __name__ == '__main__':
    main()
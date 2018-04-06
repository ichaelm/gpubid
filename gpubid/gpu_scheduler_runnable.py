import docker
import requests.exceptions
import gpubid.gpu_scheduler_state_connection as gpu_scheduler_state_connection
import time

NVIDIA_RUNTIME_NAME = 'nvidia'
CUDA_IMAGE_NAME = 'tensorflow-gpu-1.6.0'
MINING_USD_PER_SEC = 15.0 / 30 / 24 / 60 / 60

MINING_TASK_NAME = 'mining'
MINING_CMD = './mine.sh'
MINING_MOUNT_PATH = '/home/ichaelm/workspace/gpubid/dockerfiles/mining/'

CONTAINER_MOUNT_PATH = '/mnt/gpubid/'

MAIN_LOOP_DELAY_SEC = 0.5

docker_api = docker.from_env()


def try_kill(container):
    try:
        container.kill()
    except docker.errors.APIError:
        pass  # Already dead


def container_is_dead(container):
    try:
        container.wait(timeout=0.01)
        return True
    except requests.exceptions.ConnectionError:
        pass  # This means we are not dead, for some reason
    except requests.exceptions.ReadTimeout:
        print('container_is_dead ReadTimeout')
    return False


class PriorityException(Exception):
    pass


class GpuSchedulerRunnable:
    def __init__(self):
        self.mining_task = gpu_scheduler_state_connection.Task(
            name=MINING_TASK_NAME,
            cmd=MINING_CMD,
            mount_path=MINING_MOUNT_PATH,
            is_preemptable=True,
            priority=0,
            gpu_type_to_usd_per_sec=lambda x: MINING_USD_PER_SEC
        )
        self.quitting = False
        # Don't initialize gpu_scheduler_state_connection because it must be used in the same thread it's initialized in.
        self.container_objs = {}
        self.container_logs = {}

    def start_or_resume(self):
        self.state = gpu_scheduler_state_connection.GpuSchedulerStateConnection()
        # Setup tables
        self.state.create_tables_if_necessary()
        for gpu_id in range(1):
            self.state.add_gpu_row(gpu_type='')
        found_containers = docker_api.containers.list(filters={'ancestor': CUDA_IMAGE_NAME})
        current_gpu_id = 0
        if found_containers:
            print('Resuming!')
            # Temporary hack: replace with save file
            for container in found_containers:
                container_id = container.id
                self.state.add_container_row_cascade(
                    container_id=container_id,
                    is_preemptable=True,
                    priority=0,
                    usd_per_sec=MINING_USD_PER_SEC,
                    gpu_ids=[current_gpu_id],
                )
                self.container_objs[container_id] = container
                self.container_logs[container_id] = container.logs(stdout=True, stderr=True, stream=True)
                current_gpu_id += 1

        # Main loop
        while not self.quitting:
            time.sleep(MAIN_LOOP_DELAY_SEC)
            self.decide_changes()

    def decide_changes(self):
        # Clean up empty containers
        start_time = time.time()
        containers_to_remove = set()
        for container_id, container in self.container_objs.items():
            if container_is_dead(container):
                # Clean up container
                last_line = None
                for line in self.container_logs[container_id]:
                    last_line = line
                if last_line:
                    print('Output: %s' % last_line.decode('utf-8'))
                else:
                    print('No output')
                # container_row.container.remove(v=True)
                containers_to_remove.add(container_id)
        for container_id in containers_to_remove:
            self.state.remove_container_row_cascade(container_id)
            del self.container_objs[container_id]
        end_time = time.time()
        print('Cleanup took %f seconds.' % (end_time - start_time))
        # Fill all empty GPUs
        for gpu_id in self.state.get_unused_gpus():
            print('filling empty GPU')
            start_time = time.time()
            self.start_something(gpu_id)
            end_time = time.time()
            print('start_something took %f seconds.' % (end_time - start_time))

        # Evict containers when necessary
        done_evicting = False
        evicted_gpu_ids = set()  # Just for sanity check
        while not done_evicting:
            task = self.most_valuable_task()
            if task is None:
                print('task is None')
            running_container_id = self.least_valuable_running_container()
            if running_container_id is None:
                print('running container is None')
            if self.should_task_preempt_container(task, running_container_id):
                print('Evict')
                # Evict
                gpu_ids = self.state.container_gpus(running_container_id)
                for gpu_id in gpu_ids:
                    if gpu_id in evicted_gpu_ids:
                        print('Eviction loop hit the same GPU twice in a row! Sanity check failed.')
                        break
                    evicted_gpu_ids.add(gpu_id)
                # Clean up container
                last_line = next(self.container_logs[running_container_id], None)
                if last_line:
                    print('Output: %s' % last_line.decode('utf-8'))
                else:
                    print('No output')
                start_time = time.time()
                try_kill(self.container_objs[running_container_id])
                end_time = time.time()
                print('try_kill took %f seconds.' % (end_time - start_time))
                self.state.remove_container_row_cascade(running_container_id)
                # Spawn
                for gpu_id in gpu_ids:
                    try:
                        start_time = time.time()
                        self.start_task_on_empty_gpu(task, gpu_id)
                        end_time = time.time()
                        print('start_task_on_empty_gpu took %f seconds.' % (end_time - start_time))
                    except Exception as e:
                        print('start_something caused exception! This is unrecoverable. GPU %s has died.' % str(gpu_id),)
                        raise e
            else:
                done_evicting = True

    def most_valuable_task(self):
        if self.state.has_local_task():
            print('Has local task!')
            return self.state.pop_local_task()
        else:
            return self.mining_task

    def least_valuable_running_container(self):
        return self.state.least_important_container()

    def should_task_preempt_container(self, task, container_id):
        if task is None:
            return False
        if container_id is None:
            return True
        container_row = self.state.container_row(container_id)
        if not container_row['is_preemptable']:
            return False
        if task.priority != container_row['priority']:
            return task.priority > container_row['priority']
        task_usd_per_sec = task.gpu_type_to_usd_per_sec('')
        return task_usd_per_sec > container_row['usd_per_sec']

    def start_something(self, gpu_id):
        # Precondition: gpu is unused
        container_id = self.start_task_on_empty_gpu(self.most_valuable_task(), gpu_id)
        return container_id

    def start_task_on_empty_gpu(self, task, gpu_id):
        vol_map = None
        if task.mount_path:
            vol_map = {
                task.mount_path: {
                    'bind': CONTAINER_MOUNT_PATH,
                    'mode': 'ro',
                },
            }
        try:
            start_time = time.time()
            container = docker_api.containers.run(
                CUDA_IMAGE_NAME,
                detach=True,
                runtime=NVIDIA_RUNTIME_NAME,
                command=task.cmd,
                volumes=vol_map,  # May be none
                working_dir=CONTAINER_MOUNT_PATH if task.mount_path else None,
            )
            end_time = time.time()
            print('Docker run took %f seconds.' % (end_time - start_time))
        except docker.errors.APIError as e:
            raise e

        container_id = container.id
        self.state.add_container_row_cascade(
            container_id=container_id,
            is_preemptable=task.is_preemptable,
            priority=task.priority,
            usd_per_sec=task.gpu_type_to_usd_per_sec(''),
            gpu_ids=[gpu_id]
        )
        self.container_objs[container_id] = container
        self.container_logs[container_id] = container.logs(stdout=True, stderr=True, stream=True)
        return container_id



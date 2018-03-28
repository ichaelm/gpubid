import threading


class GpuTableRow:
    def __init__(self, container_id):
        self.container_id = container_id


class ContainerTableRow:
    def __init__(self, container, is_preemptable, logs, priority, usd_per_sec, gpu_id):
        self.container = container
        self.is_preemptable = is_preemptable
        self.logs = logs
        self.priority = priority
        self.usd_per_sec = usd_per_sec
        self.gpu_id = gpu_id


class Task:
    def __init__(self, name, cmd, mount_path, is_preemptable, priority, gpu_type_to_usd_per_sec):
        self.name = name
        self.cmd = cmd
        self.mount_path = mount_path
        self.is_preemptable = is_preemptable
        self.priority = priority
        self.gpu_type_to_usd_per_sec = gpu_type_to_usd_per_sec


class GpuSchedulerState:
    def __init__(self):
        self.container_table = {}  # {id: ContainerTableRow}
        self.gpu_table = {}  # {id: GpuTableRow}
        self.next_container_id = 1
        self.next_gpu_id = 0
        self.local_task_queue = []
        self.changed = threading.Event()

    def gpu_row(self, gpu_id):
        return self.gpu_table[gpu_id]

    def container_row(self, container_id):
        return self.container_table[container_id]

    def add_container_row_cascade(self, new_row):
        container_id = self.next_container_id
        self.container_table[container_id] = new_row
        self.next_container_id += 1
        if new_row.gpu_id is not None:
            self.gpu_row(new_row.gpu_id).container_id = container_id
        return container_id

    def add_gpu_row(self, new_row):
        gpu_id = self.next_gpu_id
        self.gpu_table[gpu_id] = new_row
        self.next_gpu_id += 1
        return gpu_id

    def remove_container_row_cascade(self, container_id):
        gpu_id = self.container_row(container_id).gpu_id
        self.container_table.pop(container_id)
        self.gpu_row(gpu_id).container_id = None

    def add_local_task(self, new_local_task):
        self.local_task_queue.append(new_local_task)

    def peek_local_task(self):
        return self.local_task_queue[0]

    def pop_local_task(self):
        return self.local_task_queue.pop(0)

    def has_local_task(self):
        return len(self.local_task_queue) > 0
import threading
import sqlite3


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

    def to_tuple(self):
        return self.name, self.cmd, self.mount_path, self.is_preemptable, self.priority

    @classmethod
    def from_tuple(cls, obj_tuple):
        return cls(*obj_tuple)


class GpuSchedulerStateConnection:
    def __init__(self):
        self.next_gpu_id = 0
        self.next_task_id = 0
        self.db = sqlite3.connect('gbs_state')
        self.db.row_factory = sqlite3.Row

    def create_tables_if_necessary(self):
        with self.db:
            self.db.execute('''DROP TABLE IF EXISTS Gpu;''')
            self.db.execute('''DROP TABLE IF EXISTS Container;''')
            self.db.execute('''DROP TABLE IF EXISTS GpuContainer;''')
            self.db.execute('''DROP TABLE IF EXISTS Task;''')
            self.db.execute('''CREATE TABLE IF NOT EXISTS Gpu (gpu_id INTEGER, gpu_type STRING);''')
            self.db.execute('''CREATE TABLE IF NOT EXISTS Container (container_id STRING, is_preemptable INTEGER, priority INTEGER, usd_per_sec REAL);''')
            self.db.execute('''CREATE TABLE IF NOT EXISTS GpuContainer (gpu_id INTEGER, container_id STRING);''')
            self.db.execute('''CREATE TABLE IF NOT EXISTS Task (task_id INTEGER, name STRING, cmd STRING, mount_path STRING, is_preemptable INTEGER, priority INTEGER);''')

    def gpu_row(self, gpu_id):
        return self.db.execute('''SELECT * FROM Gpu WHERE gpu_id = ?;''', (gpu_id,)).fetchone()

    def container_row(self, container_id):
        return self.db.execute('''SELECT * FROM Container WHERE container_id = ?;''', (container_id,)).fetchone()

    def gpu_containers(self, gpu_id):
        return [container_id for (container_id,) in self.db.execute('''SELECT container_id FROM GpuContainer WHERE gpu_id = ?;''', (gpu_id,)).fetchall()]

    def container_gpus(self, container_id):
        return [gpu_id for (gpu_id,) in self.db.execute('''SELECT gpu_id FROM GpuContainer WHERE container_id = ?;''', (container_id,)).fetchall()]

    def add_container_row_cascade(self, container_id, is_preemptable, priority, usd_per_sec, gpu_ids=None):
        with self.db:
            self.db.execute('''INSERT INTO Container VALUES (?, ?, ?, ?)''', (container_id, is_preemptable, priority, usd_per_sec))
            if gpu_ids:
                self.db.executemany('''INSERT INTO GpuContainer VALUES (?, ?)''', [(gpu_id, container_id) for gpu_id in gpu_ids])
        return container_id

    def add_gpu_row(self, gpu_type, container_ids=None):
        gpu_id = self.next_gpu_id
        with self.db:
            self.db.execute('''INSERT INTO Gpu VALUES (?, ?)''', (gpu_id, gpu_type))
            if container_ids:
                self.db.executemany('''INSERT INTO GpuContainer VALUES (?, ?)''', [(gpu_id, container_id) for container_id in container_ids])
        self.next_gpu_id += 1
        return gpu_id

    def get_unused_gpus(self):
        return [gpu_id for (gpu_id,) in self.db.execute('''SELECT Gpu.gpu_id FROM Gpu LEFT JOIN GpuContainer WHERE GpuContainer.gpu_id IS NULL''').fetchall()]

    def least_important_container(self):
        return self.db.execute('''SELECT container_id FROM Container ORDER BY priority ASC, usd_per_sec ASC LIMIT 1''').fetchone()[0]

    def remove_container_row_cascade(self, container_id):
        with self.db:
            self.db.execute('''DELETE FROM Container WHERE container_id = ?''', (container_id,))
            self.db.execute('''DELETE FROM GpuContainer WHERE container_id = ?''', (container_id,))

    def add_local_task(self, task):
        task_id = self.next_task_id
        with self.db:
            self.db.execute('''INSERT INTO Task VALUES (?, ?, ?, ?, ?, ?)''', (task_id,) + task.to_tuple())

    def peek_local_task(self):
        return Task.from_tuple(self.db.execute('''SELECT * FROM Task ORDER BY task_id ASC LIMIT 1''').fetchone()[1:] + (lambda x: 1,))  # Hack

    def pop_local_task(self):
        task_tuple = self.db.execute('''SELECT * FROM Task ORDER BY task_id ASC LIMIT 1''').fetchone()
        task_id, task = task_tuple[0], Task.from_tuple(task_tuple[1:] + (lambda x: 1,))  # Hack
        self.db.execute('''DELETE FROM Task WHERE task_id = ?''', (task_id,))
        return task

    def has_local_task(self):
        print(self.db.execute('''SELECT * FROM Task''').fetchone() is not None)
        return self.db.execute('''SELECT 0 FROM Task''').fetchone() is not None

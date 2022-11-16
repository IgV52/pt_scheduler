from collections import namedtuple


Task = namedtuple('Task', 'name priority target_set write_set read_set')


class Scheduler:
    STAGE = 2

    def __init__(self):
        self._task_queue = None
        self._task_process = None

    @property
    def content(self):
        return frozenset()

    def extend(self, task_queue):
        if not self._task_queue:
            self._task_queue = task_queue
        else:
            self._task_queue.extend(task_queue)

    def reach(self, target_set):
        pass

    def get(self):
        if self._task_queue:
            self._sorted_priority
            task_pop = self._task_queue.pop(0)
            if not self._task_process:
                self._task_process = [task_pop]
            else:
                self._task_process.append(task_pop)
            return task_pop
        return None

    def done(self, task):
        pass

    def _sorted_priority(self):
        self._task_queue = sorted(self._task_queue, 
                            key=lambda x: x.priority, reverse=True)

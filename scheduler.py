from collections import namedtuple


Task = namedtuple('Task', 'name priority target_set write_set read_set')


class Scheduler:
    STAGE = 4

    def __init__(self):
        self._task_process = None
        self._task_queue = None
        self._write_res_block = None

    @property
    def content(self):
        return frozenset()

    def extend(self, task_queue):
        if not self._task_queue:
            self._task_queue = task_queue
        else:
            self._task_queue.extend(task_queue)

    def reach(self, target_set):
        [self._task_queue.remove(task) for task in self._task_queue 
        for target in target_set 
        if target in task.target_set]

    def get(self):
        if self._task_queue:
            task_pop = self._iter_variable()
            self._add_write_res_block(task_pop.write_set)
            self._task_queue.remove(task_pop)
            if not self._task_process:
                self._task_process = [task_pop]
            else:
                self._task_process.append(task_pop)
            return task_pop
        return None

    def done(self, task):
        self._free_res(task)

    def _add_write_res_block(self, write_set):
        if not self._write_res_block:
            self._write_res_block = {write for write in write_set}
        else:
            self._write_res_block.add(write_set)

    def _free_res(self, task):
        self._task_process.remove(task)
        if self._write_res_block:
            self._write_res_block.difference_update(task.write_set)

    def _iter_variable(self):
        self._sorted_priority()
        for task in self._task_queue:
            if self._search_free_res(task):
                return task

    def _search_free_res(self, task):
        if self._write_res_block:
            for block_res in self._write_res_block:
                if  block_res in task.write_set:
                    return False
        return True

    def _sorted_priority(self):
        self._task_queue = sorted(self._task_queue, 
                            key=lambda x: x.priority, reverse=True)

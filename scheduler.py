from collections import namedtuple, defaultdict
from itertools import chain


Task = namedtuple('Task', 'name priority target_set write_set read_set')


class Scheduler:
    STAGE = 6

    def __init__(self):
        self._res_block = None
        self._task_process = None
        self._task_queue = None

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
            if task_pop:
                self._add_res_block(task_pop.write_set, task_pop.read_set)
                self._task_queue.remove(task_pop)
                if not self._task_process:
                    self._task_process = [task_pop]
                else:
                    self._task_process.append(task_pop)
            return task_pop
        return None

    def done(self, task):
        self._free_res(task)

    def _add_res_block(self, res_write, res_read):
        res_set = chain(res_read, res_write)
        if not self._res_block:
            self._res_block = {res for res in res_set}
        else:
            self._res_block.add(res_set)
    
    def _create_graph(self):
        data = defaultdict(list)
        for tupl in self._task_queue:
            data[str(tupl.priority)].append(tupl)
        return data

    def _depend_task(self, data, task):
        if data:
            node = {task: i for i in data
                    for j in i.read_set
                    if j in task.write_set}
            if node:
                return True
            return False

    def _free_res(self, task):
        self._task_process.remove(task)
        if self._res_block:
            self._res_block.difference_update(task.write_set, task.read_set)

    def _iter_variable(self):
        self._sorted_priority()
        data = self._create_graph()
        depend = [task for task in self._task_queue 
        if self._depend_task(data[str(task.priority)],task) 
        and self._search_free_res(task)]
        for task in self._task_queue:
            if depend:
                if task.priority == depend[0].priority:
                    return depend[0]
            if self._search_free_res(task):
                return task

    def _search_free_res(self, task):
        if self._res_block:
            for block_res in self._res_block:
                if  block_res in task.write_set or block_res in task.read_set:
                    return False
        return True

    def _sorted_priority(self):
        self._task_queue = sorted(self._task_queue, 
                            key=lambda x: x.priority, reverse=True)

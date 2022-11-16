import pytest
import yaml

from pathlib import Path

from scheduler import Task, Scheduler


@pytest.mark.parametrize(
    'script_path',
    sorted(map(str, Path(__file__).parent.glob('*.yaml')))
)
def test(script_path):
    with open(script_path, 'r', encoding='utf-8-sig') as script_file:
        script = yaml.safe_load(script_file)

    if script['stage'] > Scheduler.STAGE:
        return pytest.skip('NotImplemented')

    print('Start')

    task_map = {
        task_name: Task(
            name=task_name,
            priority=task_data.get('priority', 0),
            target_set=frozenset(task_data.get('target', '').split()),
            write_set=frozenset(task_data.get('write', '').split()),
            read_set=frozenset(task_data.get('read', '').split())
        )
        for task_name, task_data in script['tasks'].items()
    }

    scheduler = Scheduler()

    for num, step in enumerate(script['steps'], start=1):
        print(num, ':', step)

        if 'extend' in step:
            task_name_queue = (step['extend'] or '').split()

            scheduler.extend([
                task_map[task_name]
                for task_name in task_name_queue
            ])

        if 'reach' in step:
            target_set = frozenset((step['reach'] or '').split())

            scheduler.reach(target_set)

        if 'get' in step:
            expected_task_name = step['get']

            actual_task = scheduler.get()
            actual_task_name = actual_task and actual_task.name

            assert actual_task_name == expected_task_name

        if 'done' in step:
            task_name = step['done']

            scheduler.done(task_map[task_name])

        if 'content' in step:
            expected_task_name_set = frozenset((step['content'] or '').split())

            actual_task_set = scheduler.content
            actual_task_name_set = frozenset(
                task.name for task in actual_task_set
            )

            assert actual_task_name_set == expected_task_name_set

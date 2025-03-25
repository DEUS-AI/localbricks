import importlib.util
import os

class TaskLoader:
    def __init__(self, task_path: str):
        self.task_path = task_path

    def load_task(self):
        module_name = os.path.splitext(os.path.basename(self.task_path))[0]
        spec = importlib.util.spec_from_file_location(module_name, self.task_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
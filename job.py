import enum

class Action(enum.Enum):
   Start = 0
   End = 1
   Fork = 2
   ForkEnd = 10
   Calculation = 4
   Communication = 5

class Task:
    def __init__(self, process_id, task_id, action, needed_memory, runtime):
        self.process_id = process_id
        self.task_id = task_id
        self.action = action
        self.child_process = None
        self.needed_memory = needed_memory
        self.runtime = runtime
        self.next = None

    def info(self):
        return f"pid: {self.process_id}, tskid {self.task_id}, action {self.action}"
    
class Job:
    def __init__(self, start, deadline, total_runtime, total_needed_memory) -> None:
        self.start = start 
        self.deadline = deadline
        self.total_runtime = total_runtime
        self.total_needed_memory = total_needed_memory
        self.head = None


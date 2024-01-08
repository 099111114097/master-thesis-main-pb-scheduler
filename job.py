import enum

class Action(enum.Enum):
   Start = 0
   End = 1
   Fork = 2
   ForkEnd = 10
   Join = 3
   Calculation = 4
   Communication = 5

class Task:
    def __init__(self, task_id, process_id, action, needed_memory, instructions):
        self.process_id = process_id
        self.task_id = task_id
        self.action = action
        self.child_process = None
        self.child_process_id = -1
        self.comm_to = None #communicates and waits for
        self.comm_to_id = (-1,-1) #task_id, pid
        self.needed_memory = needed_memory
        self.instructions = instructions
        self.next = None
        self.next_id = -1

    def info(self):
        if self.next == None:
            next_task_id = -1
        else:
            next_task_id = self.next.task_id
        if self.child_process == None:
            child_process = -1
        else:
            child_process = self.child_process.process_id
        print(f"pid: {self.process_id}, tskid {self.task_id}, action {self.action}, next {next_task_id}, next id {self.next_id}, child {child_process}, comm to {self.comm_to_id}")

class Process:
    def __init__(self, process_id, approx_runtime, approx_needed_memory):
        self.process_id = process_id
        self.approx_runtime = approx_runtime
        self.approx_needed_memory = approx_needed_memory
        self.forked = []
        self.task_head = None
    
    def info(self) -> str:
        curr = self.task_head
        while curr != None:
            curr.info()
            if curr.action == 2:
                curr.child_process.info()
            curr = curr.next

class Job:
    def __init__(self, start, deadline, total_runtime, total_needed_memory):
        self.start = start 
        self.deadline = deadline
        self.total_runtime = total_runtime
        self.total_needed_memory = total_needed_memory
        self.head = None

    def info(self) -> str:
        curr = self.head
        while curr != None:
            curr.info()
            if curr.child_process != None:
                curr.child_process.info()
            curr = curr.next
import job
import csv

def read_job(job_id):
    csvfile = open(f"data/job_details_{str(job_id)}.csv", newline='')
    model_reader = csv.DictReader(csvfile, delimiter=";")
    data = next(model_reader)
    return job.Job(job_id, int(data['start']), int(data['deadline']), int(data['total_runtime']), int(data['total_needed_memory']))

def read_process(j: job.Job):
    csvfile = open(f"data/job_processes_{str(j.job_id)}.csv", newline='')
    model_reader = csv.DictReader(csvfile, delimiter=";")
    processes = []
    for row in model_reader:
        processes.append(job.Process(int(row['process_id']), int(row['approx_runtime']), int(row['approx_needed_memory'])))
    j.processes = processes

def read_tasks(j: job.Job) -> job.Task:
    tasks = []
    with open(f"data/job_tasks_{str(j.job_id)}.txt") as f:
        for line in f:
            tmp = " ".join(line.split())
            result = tmp.split("->")
            task_details = result[0].split(" ")
            task = extract_task(task_details)
            if len(result) > 1:
                next_task_id = [int(x) for x in result[1].split(" ") if x]
                task.next_id = int(next_task_id[0]) # tid
            if task.action == 2:
                child_id = [x for x in result[2].split(" ") if x]
                task.child_process_id = int(child_id[1]) # here pid
            if task.action == 5:
                comm_detail = result[0].split("--")[1]
                comm = [x for x in comm_detail.split(" ") if x]
                if len(comm) == 1 and comm[0] == "0": #communication init
                    task.comm_to_id = (int(comm[0]), 0)
                else:
                    task.comm_to_id = (int(comm[0]), int(comm[1])) # tid, pid
            tasks.append(task)
        for i in range(len(tasks)):
            tasks[i].next = find_task(tasks[i].next_id, tasks[i:]) # specific interval for tasks to save iterations as next task will be found later in the task net
            if tasks[i].action == 2:
                tasks[i].child_process = find_process(tasks[i].child_process_id, j.processes)
                tasks[i].child_process.task_head = find_first_task(tasks[i].child_process_id, tasks)
                p = find_process(tasks[i].process_id, j.processes)
                p.forked.append(tasks[i].child_process)
    j.head = tasks[0]
    j.processes[0].task_head = tasks[0]
        
def find_task(task_id, tasks: [job.Task])-> job.Task:
    for t in tasks:
        if t.task_id == task_id:
            return t
    return None

def find_first_task(process_id, tasks: [job.Task])-> job.Task:
    for t in tasks:
        if t.process_id == process_id:
            return t
    return None

def find_process(process_id, processes: [job.Process])-> job.Process:
    for p in processes:
        if p.process_id == process_id:
            return p
    return None

def extract_task(details):
    action = int(details[2])
    if action == 1:
        return job.Task(int(details[0]), int(details[1]), int(details[2]), 0, 0)
    return job.Task(int(details[0]), int(details[1]), int(details[2]), int(details[3]), int(details[4]))
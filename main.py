import csv
import reservation_static as re
import job
#def read_plan():



def machine_details_reader():
    csvfile = open("data/machine_details.csv", newline='')
    return csv.DictReader(csvfile, delimiter=";")
    
def init_reservation_for_cores(res_store):
    reader = machine_details_reader()
    for row in reader:
        core_ids = row['cores'].split(",")
        node_res = re.NodeReservation(int(row['node_id']), int(row['total_shared_memory']), core_ids)
        res_store.add(node_res)
        for core_id in core_ids:
            new_res = re.CoreReservation(core_id, node_res.shared_memory, len(node_res.core_ids), node_res.node_id)
            node_res.add(new_res)

def read_model():
    csvfile = open("data/memory_consumption_model_example.csv", newline='')
    model_reader = csv.DictReader(csvfile, delimiter=";")
    for row in model_reader:
        print(int(row['job_id']), int(row['process_id']), int(row['memory_consumption']))

def find_in_model(job_id, process_id, model_reader):
    for row in model_reader:
        if row['job_id'] == job_id and row['process_id'] == process_id:
            return int(row['memory_consumption'])
    
def read_job(job_id):
    tasks = []
    with open(f"data/job_{str(job_id)}.txt") as f:
        for line in f:
            tmp = " ".join(line.split())
            result = tmp.split("->")
            task_details = result[0].split(" ")
            task_id, process_id, action, memory, runtime = extract_task_details(task_details)
            #memory = find_in_model(job_id, process_id, task_id, model_reader)
            tasks.append(job.Task(process_id, task_id, action, memory, runtime))
    return tasks
        
def extract_task_details(details):
    return details[0], details[1], details[2], details[3], details[4]

def validate_request(): # check that resources that job can actually be finished by deadline and that we have the necessary resources + their amount availanble to do so while still meeting the deadline
    return

def add_reservation(rs: re.ReservationStore, prefered_core: re.CoreReservation, start: int, runtime: int, needed_memory: int, job_id: int, process_id: int, task_id: int) -> re.Reservation:
    node_id, core_id = -1, -1
    if prefered_core == None or needed_memory > prefered_core.memory:
        nodes = rs.nodes_with_sufficient_memory(needed_memory)
        tmp = rs.node_with_earliest_start(start, runtime, nodes)
        new_start, core_id = tmp[0]
        node_id = tmp[1]
    else:
        new_start, core_id = prefered_core.earliest_start(start, runtime)
        node_id = prefered_core.node_id
    if new_start > start: #in case earliest start is later than possible
        start = new_start
    n = rs.find(node_id)
    core = n.find(core_id)
    new_res = re.Reservation(start, start+runtime, job_id, process_id, task_id, needed_memory) # reservation not active yet
    core.add(new_res)
    return core

def add_job(job_id):
    tasks = read_job(job_id)
    for t in tasks:
        print(t.info())

def main():
    add_job(1)
    return
    #read_model()
    rs = re.ReservationStore()
    init_reservation_for_cores(rs)
    print(repr(rs))
    #get request
    core = add_reservation(rs, None, 1, 6, 30, 1, 1, 1)           
    add_reservation(rs, core, 1, 6, 304, 1, 1, 2) 
    print(repr(rs)) 

if __name__ == "__main__":
    main()



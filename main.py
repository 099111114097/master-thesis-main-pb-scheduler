import reservation_static as re
import job
import request_processig as rp

MIN_MEM_PERCENTAGE = 0.8 # min acceptaed percentage of needed available memory
CAL_WANT_TYPE = 1 # 0 - based on job, 1 - based on all processes and their durations

def add_job(job_id):
    j = rp.read_job(job_id)
    processes = rp.read_process(job_id)
    t = rp.read_tasks(job_id, processes)
    j.head = t
    return j, processes

def add_reservation_for_process(rs: re.ReservationStore, core: re.CoreReservation, job_id, process: job.Process, start):
    curr = process.task_head
    actual_start = -1
    latest_res = None
    while curr != None and (curr.process_id == process.process_id and curr.action != 3): # to make sure we do not count join to another process as task twice (cause counted for both processes)
        latest_res = rs.add_reservation(core, start, curr.instructions, curr.needed_memory, job_id, process.process_id, curr.task_id)
        if start == -1:
            start = latest_res.start
        curr = curr.next
    if latest_res == None:
        raise Exception("no process with actual task to map provided")
    p = re.ProcessReservation(actual_start, latest_res.end, job_id, process.process_id, process.approx_needed_memory)
    core.add_process_res(p)


def validate_request(j: job.Job, rs: re.ReservationStore, processes)-> bool:
    if j.deadline - j.start < j.total_runtime:
        raise Exception(f"Job {j.job_id}: deadline is set earlier than approx runtime can finish. start {j.start}, deadline {j.deadline}, approx runtime {j.approx_runtime}")
    total_memory = rs.total_memory()
    if total_memory < j.total_needed_memory:
        raise Exception(f"Job {j.job_id}: needed total memory {j.total_needed_memory} exceeds machine resources {total_memory}")
    # above checked that generally enough resource would be there and that the set time is possible
    # now it would have to check if during the time start to deadline enough resource is available for at least the runtime duration in sum
    want = -1
    if CAL_WANT_TYPE == 0:
        want = j.total_runtime*j.total_needed_memory
    if CAL_WANT_TYPE == 1:
        want = mem_needed_over_time_process(processes)
    if want == -1:
        raise Exception("Calculation type unknowen or calculations failed")
    left = available_mem_over_time(rs, j)
    cal = left/want
    if left/want < MIN_MEM_PERCENTAGE: # at least 80 percent of wanted available
        raise Exception(f"Job {j.job_id}: free memory over time {left} is less than {MIN_MEM_PERCENTAGE} (={cal})of needed memory over time (needed_mem*runtime) {want}")
    print(f"free memory over time {left} is >= {MIN_MEM_PERCENTAGE} (={cal}) of needed memory over time (needed_mem*runtime) {want}")
    return

def mem_needed_over_time_process(processes):
    mem_needed_over_time = -1
    for p in processes:
        mem_needed_over_time += p.approx_runtime*p.approx_needed_memory
    return mem_needed_over_time


def available_mem_over_time(rs: re.ReservationStore, j: job.Job):
    max_duration = (j.deadline-j.start)
    max_have = max_duration*rs.total_memory()
    taken_mem_over_time = rs.memory_taken_over_time(j.start,j.deadline)
    left_have = max_have - taken_mem_over_time
    return left_have


def main():
    rs = re.ReservationStore()
    rs.init_reservation_for_cores()
    j, processes = add_job(2)
    validate_request(j, rs, processes) #TODO catch error or return bool and log decision
    n = rs.find(3423)
    c = n.find("te")
    c.add_process_res(re.ProcessReservation(0, 300, 4, 3, 60)) #TODO fix that process_res cannot be bigger than mem available for core
    validate_request(j, rs, processes)
    return

if __name__ == "__main__":
    main()



import reservation_static as re
import job
import request_processig as rp

MIN_MEM_PERCENTAGE = 0.8 # min acceptaed percentage of needed available memory
CAL_WANT_TYPE = 1 # 0 - based on job, 1 - based on all processes and their durations
DEBUG = False

# we assume that a process even in estimation will not start right when its parent does and that the first fork will start after a min INIT_TIME
INIT_TIME = 2
FORK_TIME = 1

def add_job(rs, job_id):
    j = rp.read_job(job_id)
    rp.read_process(j)
    rp.read_tasks(j)
    #try:
    validate_request(j, rs)
    #except Exception as e:
    #    print(f"Requested not valid: " + str(e))
    return

def validate_request(j: job.Job, rs: re.ReservationStore)-> bool:
    # -- general check --
    if j.deadline - j.start < j.total_runtime: # have not checked if job details and job process file content makes sense together
        raise Exception(f"Job {j.job_id}: deadline is set earlier than approx runtime can finish. start {j.start}, deadline {j.deadline}, approx runtime {j.total_runtime}")
    total_memory = rs.total_memory()
    if total_memory < j.total_needed_memory:
        raise Exception(f"Job {j.job_id}: needed total memory {j.total_needed_memory} exceeds machine resources {total_memory}")
    # -- job level - check enough resources during desired time --
    want = -1
    if CAL_WANT_TYPE == 0:
        want = j.total_runtime*j.total_needed_memory
    if CAL_WANT_TYPE == 1:
        want = mem_needed_over_time_process(j.processes)
    if want == -1:
        raise Exception("Calculation type unknowen or calculations failed")
    left = available_mem_over_time(rs, j)
    cal = left/want
    if left/want < MIN_MEM_PERCENTAGE: # at least 80 percent of wanted available
        raise Exception(f"Job {j.job_id}: free memory over time {left} is less than {MIN_MEM_PERCENTAGE} (={cal})of needed memory over time (needed_mem*runtime) {want}")
    if DEBUG:
        print(f"free memory over time {left} is >= {MIN_MEM_PERCENTAGE} (={cal}) of needed memory over time (needed_mem*runtime) {want}")
    # -- process level - check if processes could fit intbetween process reservation while finishing before deadline
    process_test_mapping(rs, rp.find_process(0, j.processes), j)
    rs.clean_test_mapping()
    return

def process_test_mapping(rs: re.ReservationStore, init_process: job.Process, j: job.Job):
    res = rs.add_test_p_reservation(j.start, j.deadline, init_process.approx_runtime, init_process.approx_needed_memory, j.job_id, init_process.process_id)
    def test_mapping_forked_p(parent, res):
        if len(parent.forked) == 0:
            print("no further processes present")
            return
        for i in range(len(parent.forked)):
            p = parent.forked[i]
            res = rs.add_test_p_reservation(res.start+((i*FORK_TIME)+INIT_TIME), j.deadline, p.approx_runtime, p.approx_needed_memory, j.job_id, p.process_id) # start get calculated with the assumption that the process that forks the other ps needs some init and/or fork time inbetween
            test_mapping_forked_p(p, res)
    test_mapping_forked_p(init_process, res)


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

def main():
    rs = re.ReservationStore()
    rs.init_reservation_for_cores()
    #n = rs.find(3423)
    #c = n.find("te")
    #c.add_process_res(re.ProcessReservation(0, 300, 4, 3, 230))
    #c = n.find("ta")
    #c.add_process_res(re.ProcessReservation(0, 300, 4, 3, 230))
    #c = n.find("to")
    #c.add_process_res(re.ProcessReservation(0, 300, 4, 3, 230))
    #n = rs.find(1232424)
    #c = n.find("alan")
    #c.add_process_res(re.ProcessReservation(0, 300, 4, 3, 50))
    #c = n.find("ben")
    #c.add_process_res(re.ProcessReservation(0, 300, 4, 3, 50))
    #c = n.find("cara")
    #c.add_process_res(re.ProcessReservation(0, 300, 4, 3, 50))
    add_job(rs, 2)
    print("JOB ADDED")
    add_job(rs, 3)
    # 1. check job possible with memory resources
    #validate_request(j, rs) #TODO catch error or return bool and log decision
    # 2. map processes and check if possible
    # 3. map task and check if possible (if no do 2. differently and try again a few times, e.g. max 10 times)
    return

if __name__ == "__main__":
    main()



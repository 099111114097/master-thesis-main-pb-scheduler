import job
import reservation_static as re
import request_processig as rp
from exceptions import ValidationException

MIN_MEM_PERCENTAGE = 0.8 # min accepted percentage of needed available memory
CAL_WANT_TYPE = 0 # 0 - based on job, 1 - based on all processes and their durations
DEBUG = False

# Assumption: a process even in estimation will not start right when its parent does and the first fork will start after a min INIT_TIME
INIT_TIME = 2
FORK_TIME = 1

def validate_request(j: job.Job, rs: re.ReservationStore)-> bool:
    # -- general check --
    if j.deadline - j.start < j.total_runtime: # have not checked if job details and job process file content makes sense together
        raise ValidationException(f"Job {j.job_id}: deadline is set earlier than approx runtime can finish. start {j.start}, deadline {j.deadline}, approx runtime {j.total_runtime}")
    total_memory = rs.total_memory()
    if total_memory < j.total_needed_memory:
        raise ValidationException(f"Job {j.job_id}: needed total memory {j.total_needed_memory} exceeds machine resources {total_memory}")
    # -- job level - check enough resources during desired time --
    want = -1
    if CAL_WANT_TYPE == 0:
        want = j.total_runtime*j.total_needed_memory
    if CAL_WANT_TYPE == 1:
        want = mem_needed_over_time_process(j.processes)
    if want == -1:
        raise ValidationException("Calculation type unknowen or calculations failed")
    left = available_mem_over_time(rs, j)
    cal = left/want
    if left/want < MIN_MEM_PERCENTAGE: # at least MIN_MEM_PERCENTAGE*100 percent of wanted memory available
        raise ValidationException(f"Job {j.job_id}: free memory over time {left} is less than {MIN_MEM_PERCENTAGE} (={cal})of needed memory over time (needed_mem*runtime) {want}")
    if DEBUG:
        print(f"free memory over time {left} is >= {MIN_MEM_PERCENTAGE} (={cal}) of needed memory over time (needed_mem*runtime) {want}")
    # -- process level - check if processes could fit inbetween process reservation while finishing before deadline
    process_test_mapping(rs, rp.find_process(0, j.processes), j)
    rs.clean_test_mapping()
    return

def process_test_mapping(rs: re.ReservationStore, init_process: job.Process, j: job.Job):
    res = rs.add_test_p_reservation(j.start, j.deadline, init_process.approx_runtime, init_process.approx_needed_memory, j.job_id, init_process.process_id)
    def test_mapping_forked_p(parent, res):
        if len(parent.forked) == 0:
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
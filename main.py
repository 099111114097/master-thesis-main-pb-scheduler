import reservation_static as re
import job
import request_processig as rp
import build_plan as p

MIN_MEM_PERCENTAGE = 0.8 # min acceptaed percentage of needed available memory
CAL_WANT_TYPE = 1 # 0 - based on job, 1 - based on all processes and their durations
DEBUG = False

# we assume that a process even in estimation will not start right when its parent does and that the first fork will start after a min INIT_TIME
INIT_TIME = 2
FORK_TIME = 1

def schedule_job(rs, job_id):
    try:
        j = rp.read_job(job_id)
        rp.read_process(j)
        tasks = rp.read_tasks(j)
        validate_request(j, rs)
        print("JOB VALIDATION DONE")
        add_reservation_for_process(rs, j.job_id, j.deadline, j.processes[0], j.start, [])
        print("JOB ADDED")
        pb = p.PlanBuilder(rs, job_id)
        pb.build_plan()
    except Exception as e:
        print(f"Error while processing or mapping job: " + str(e))
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
    # -- task level - do I even check? probably part of the actual task scheduling right?
    return

def process_test_mapping(rs: re.ReservationStore, init_process: job.Process, j: job.Job):
    res = rs.add_test_p_reservation(j.start, j.deadline, init_process.approx_runtime, init_process.approx_needed_memory, j.job_id, init_process.process_id)
    def test_mapping_forked_p(parent, res):
        if len(parent.forked) == 0:
            print("no further processes present")
            return
        for i in range(len(parent.forked)):
            p = parent.forked[i]
            print(res.start)
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

def add_reservation_for_process(rs: re.ReservationStore, job_id, deadline: int, process: job.Process, start, additional_p_res: [re.ProcessReservation]):
    curr = process.task_head
    actual_process_start = -1
    actual_task_start = start
    latest_res = None
    pref_core = None
    p = None
    add_p_res_for_child = additional_p_res
    child_join_to_task = {}
    join_in_task = -1
    while curr != None: # to make sure we do not count join to another process as task twice (cause counted for both processes)
        #curr.info()
        if curr.process_id != process.process_id and curr.action == 3:
            join_in_task = curr.task_id
            break
        if curr.action == 3 and curr.process_id == process.process_id:
            if curr.task_id not in child_join_to_task:
                print("join task joins process itself")
            else:
                last_child_res = child_join_to_task[curr.task_id]
                actual_task_start = last_child_res.end
        latest_res, pref_core = rs.add_reservation(pref_core, actual_task_start, deadline, curr.instructions, curr.needed_memory, job_id, process.process_id, curr.task_id, additional_p_res)
        if actual_process_start == -1:
            actual_process_start = latest_res.start
        actual_task_start = latest_res.end
        if p == None:
            p = re.ProcessReservation(pref_core.node_id, pref_core.core_id, actual_process_start, -1, job_id, process.process_id, process.approx_needed_memory)
            add_p_res_for_child.append(p)
        if curr.action == 2:
            tmp_res, tmp_join_task = add_reservation_for_process(rs, job_id, deadline, curr.child_process, latest_res.end, add_p_res_for_child)
            child_join_to_task[tmp_join_task] = tmp_res
        

        #TODO what about join does the parent process has to wait till the other process finished the task before the join task
        curr = curr.next
    if latest_res == None:
        raise Exception("no process with actual task to map provided")
    p.end = latest_res.end
    p.info()
    pref_core.add_process_res(p)
    return latest_res, join_in_task

def main():
    rs = re.ReservationStore()
    rs.init_reservation_for_cores()
    schedule_job(rs, 2)
    #schedule_job(rs, 3)
    return

if __name__ == "__main__":
    main()



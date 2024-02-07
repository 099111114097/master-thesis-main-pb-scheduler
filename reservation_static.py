from __future__ import annotations
import math
import csv
import job
import random

from exceptions import ValidationException, ReservationException, StructureException

'''
This reservation version splits the shared memory of a node fair between the cores instead of doing dynamic balancing
'''

PROCESS_IDLE_PERCENTAGE = 0.2 # assumes how much percentage of the process runtime is actual waiting time (so other tasks/processes can be executed inbetween)
INVALID_FRAME = (-1, -1)
INVALID_START = -1

PROCESS = "p"
TASK = "t"

DEBUG = False

class TaskReservation:
    def __init__(self, start: int, end, job_id, process_id, task_id, memory_to_res):
        self.start = start
        self.end = end
        self.job_id = job_id
        self.process_id = process_id
        self.task_id = task_id
        self.memory_reserved = memory_to_res
        self.next = None
    
    def info(self):
        print(f"start {self.start}, end {self.end}, job {self.job_id}, process {self.process_id}, task {self.task_id}, memory {self.memory_reserved}")

class ProcessReservation: # to track how much memory is already reserved
    def __init__(self, node_id, core_id, start, end, job_id, process_id, memory_to_res, active=True):
        self.node_id = node_id
        self.core_id = core_id
        self.start = start
        self.end = end
        self.job_id = job_id
        self.process_id = process_id
        self.memory_to_res = memory_to_res
        self.next = None
        self.test_idel_used = False # this is set to True in case in our testing the waiting time would be used for another process
        self.active = active

    def info(self):
        print(f"start {self.start}, end {self.end}, job {self.job_id}, process {self.process_id}, memory {self.memory_to_res}, active {self.active}")

    def process_res_in_interval(self, start, end)-> bool:
        def point_in_interval(time, start, end):
            return time >= start and time <= end
        return (self.start <= start and self.end >= end) or (point_in_interval(self.start, start, end) or point_in_interval(self.end, start, end))

    def test_idel_used_set(self):
        self.test_idel_used = True

    def test_idel_time(self)-> int:
        if self.test_idel_used:
            return 0
        return math.floor((self.end-self.start)*PROCESS_IDLE_PERCENTAGE)
    
    def overlap(self, potential_start, approx_runtime)-> int: # overlap of new to schedule process end with this process_res, can be greater than process_res itself, = remaining to schedule of process
        return (potential_start+approx_runtime) - max(self.start, potential_start)

class CoreReservation:
    def __init__(self, core_id, node_memory, core_amount, node_id, instructions_pre_sec):
        self.memory = node_memory/core_amount
        self.core_id = core_id
        self.node_id = node_id
        self.instructions_pre_sec = instructions_pre_sec
        self.reservation_head = None
        self.process_res_head = None
        self.next = None

    def info_res(self):
        node = self.reservation_head
        nodes = []
        while node != None:
            nodes.append("res"+str(node.start)+"-"+str(node.end)+":"+str(node.job_id)+":"+str(node.process_id)+":"+str(node.task_id))
            node = node.next
        nodes.append("None")
        print(" -> ".join(nodes))
    
    def info_process_res(self):
        node = self.process_res_head
        nodes = []
        while node != None:
            nodes.append("p_res"+str(node.start)+"-"+str(node.end)+":"+str(node.job_id)+":"+str(node.process_id))
            node = node.next
        nodes.append("None")
        print(" -> ".join(nodes))

    def info(self):
        print(f"node {self.node_id}, core {self.core_id}")

    
    def add(self, reservation, res_type=TASK):
        if res_type == PROCESS:
            if self.process_res_head == None:
                self.process_res_head = reservation
                return
            res = self.process_res_head
            if res.start >= reservation.start:
                self.process_res_head = reservation
                reservation.next = res
                return
        if res_type == TASK:
            if self.reservation_head == None:
                self.reservation_head = reservation
                return
            res = self.reservation_head
            if res.start >= reservation.start:
                self.reservation_head = reservation
                reservation.next = res
                return
        if res.next == None:
            res.next = reservation
            return
        while res.next != None:
            if reservation.start <= res.next.start:
                reservation.next = res.next
                res.next = reservation
                return
            res = res.next
        res.next = reservation
    
    def add_process_res(self, reservation):
        if reservation.memory_to_res > self.memory:
            raise Exception(f"Process_res tries to reserve more memory ({reservation.memory_to_res}) than available for core ({self.memory})")
        self.add(reservation, res_type=PROCESS)

    def timeframe_possible(self, head_start, start, approx_runtime, approx_needed_memory)-> (int, int):
        if head_start == None:
            return start, start+approx_runtime
        if start+approx_runtime <= head_start.start:
            return start, start+approx_runtime
        else:
            if self.memory - head_start.memory_to_res >= approx_needed_memory: # overlap would be acceptable
                overlap = head_start.overlap(start, approx_runtime)
                approx_waiting = head_start.test_idel_time()
                if DEBUG:
                    print(f"overlap {overlap} approx_waiting {approx_waiting}")
                if overlap <= approx_waiting:
                    head_start.test_idel_used_set()
                    return start, start+approx_runtime
                rest_overlap = overlap - approx_waiting # approx_waiting can be 0 if idle time was already used by other process_res
                if DEBUG:
                    print(f"rest_overlap {rest_overlap}")
                if rest_overlap == approx_runtime: # not start point and did not use idle time of processes yet
                    start = head_start.end
                if head_start.next == None:
                    head_start.test_idel_used_set()
                    return start, head_start.end+rest_overlap
                else:
                    timeframe = self.timeframe_possible(head_start.next, start, rest_overlap, approx_needed_memory)
                    if head_start.process_res_in_interval(timeframe[0], timeframe[1]):
                        head_start.test_idel_used_set()
                    return timeframe
            else:
                return self.timeframe_possible(head_start.next, head_start.end, approx_runtime, approx_needed_memory)

    def find_possible_timeframe_p_res(self, start, approx_runtime, approx_needed_memory)-> ((int, int), (int, str)): # find the earliest start, end for a new p_res
        if self.memory < approx_needed_memory: # no valid timeframe possible
            return (INVALID_FRAME, (self.node_id, self.core_id))
        if self.process_res_head == None:
            return ((start, start+approx_runtime), (self.node_id, self.core_id))
        timeframe = self.timeframe_possible(self.process_res_head, start, approx_runtime, approx_needed_memory)
        return (timeframe, (self.node_id, self.core_id))
    
    def remove_inactive_test_frames(self):
        if self.process_res_head == None:
            return
        if not self.process_res_head.active:
            if self.process_res_head.next == None:
                self.process_res_head = None
                return
            else:
                self.process_res_head = self.process_res_head.next
        curr = self.process_res_head   
        while curr != None: # clean all inactive p_res until head is active or None
            if curr.active:
                break
            self.process_res_head = curr.next
            curr = curr.next
        if curr == None or curr.next == None:
            return
        while curr.next != None: # clean up p_res afterupdated head
            if not curr.next.active:
                curr.next = curr.next.next
                continue
            curr = curr.next

    def cleanup_res(self, job_id, res_type=TASK):
        curr = None
        if res_type == PROCESS:
            if DEBUG:
                print("p_res cleaning")
            if self.process_res_head == None:
                return
            if self.process_res_head.job_id == job_id:
                if self.process_res_head.next == None:
                    self.process_res_head = None
                    return
                else:
                    self.process_res_head = self.process_res_head.next
            curr = self.process_res_head   
            while curr != None: # clean all inactive p_res until head is active or None
                if curr.job_id != job_id:
                    break
                self.process_res_head = curr.next
                curr = curr.next
        if res_type == TASK:
            if DEBUG:
                print("res cleaning")
            if self.reservation_head == None:
                return
            if self.reservation_head.job_id == job_id:
                if self.reservation_head.next == None:
                    self.reservation_head = None
                    return
                else:
                    self.reservation_head = self.reservation_head.next
            curr = self.reservation_head
            while curr != None: # clean all inactive p_res until head is active or None
                if curr.job_id != job_id:
                    break
                self.reservation_head = curr.next
                curr = curr.next
        if curr == None or curr.next == None:
            return
        while curr.next != None:
            if curr.next.job_id == job_id:
                if DEBUG:
                    print(f"removing {curr.next.info()}")
                curr.next = curr.next.next
                continue
            curr = curr.next
    
    def process_info(self):
        node = self.process_res_head
        nodes = []
        while node != None:
            nodes.append(str(node.start)+"-"+str(node.end)+":"+str(node.task_id))
            node = node.next
        nodes.append("None")
        print(" -> ".join(nodes))

    def runtime(self, instructions)-> int: # in seconds
        return math.ceil(instructions/self.instructions_pre_sec)
    
    def taken_mem_over_time(self, start, end)-> int:
        taken_mem_over_time = 0
        curr = self.process_res_head
        while curr != None: 
            if curr.process_res_in_interval(start, end):
                taken_mem_over_time += (min(curr.end, end)-max(curr.start, start))*curr.memory_to_res
            curr = curr.next
        return taken_mem_over_time
    
    def mem_res_in_timeframe(self, start, end)-> int:
        mem_res = 0
        curr = self.process_res_head
        while curr != None: 
            if curr.process_res_in_interval(start, end):
                mem_res += curr.memory_to_res
            curr = curr.next
        return mem_res

    def earliest_start(self, possible_start, instructions, add_p_res, needed_memory)-> (int, int): # return earliest free timeslot from given start point that is long enough (>=runtime), -1 means not enough resources on core for requested task reservation
        def additional_p_res_mem(additional_p_res):
            if len(additional_p_res) == 0:
                return 0
            res_mem = 0
            for p in additional_p_res:
                res_mem += p.memory_to_res
            return res_mem
        runtime = self.runtime(instructions)
        curr = self.reservation_head
        add_p_res_mem = additional_p_res_mem(add_p_res)
        if (self.memory - add_p_res_mem) < needed_memory:
            return (INVALID_START, self.core_id) # no valid timeframe can be found as init process blocks core due to memory reservation too high
        if curr == None:
            return (possible_start, self.core_id)
        if curr.next == None:
            return (max(curr.end, possible_start), self.core_id)
        while curr.next != None:
            if curr.next.start - max(curr.end, possible_start) >= runtime:
                potential_start = max(curr.end, possible_start)
                if self.too_much_memory_res_by_p(potential_start, potential_start+runtime, add_p_res_mem, needed_memory):
                    curr = curr.next
                    continue
                return  (potential_start, self.core_id)
            curr = curr.next
        return (max(curr.end,possible_start), self.core_id)

    def too_much_memory_res_by_p(self, start, end, add_p_res_mem, needed_memory)-> bool:
        available_mem = self.memory - add_p_res_mem
        mem_res_in_t = self.mem_res_in_timeframe(start, end) #TODO: try to improve as bit inaccurate as if one p_res ends and another p_res starts in timeframe both of their memory res are added up as used memory
        if available_mem - mem_res_in_t < needed_memory:
            return False
        return True
    
    def get_res_description_by_job(self, job_id)-> list[str]:
        task_res = []
        curr = self.reservation_head
        while curr != None:
            if curr.job_id == job_id:
                task_res.append({"start": curr.start, "end": curr.end, "process_id": curr.process_id, "task_id": curr.task_id, "mem_consumption": curr.memory_reserved})
            curr = curr.next
        return task_res

class NodeReservation:
    def __init__(self, node_id, shared_memory, core_ids, instructions_pre_sec):
        self.node_id = node_id
        self.shared_memory = shared_memory
        self.core_ids = core_ids
        self.instructions_pre_sec = instructions_pre_sec
        self.core_head = None
        self.next = None

    def __repr__(self):
        node = self.core_head
        nodes = []
        while node != None:
            nodes.append("- "+str(node.core_id)+":"+repr(node))
            node = node.next
        return "\n".join(nodes)

    def find(self, core_id) -> CoreReservation:
        curr = self.core_head
        while curr != None:
            if curr.core_id == core_id:
                return curr
            curr = curr.next
        return None

    def add(self, core_res):
        curr = self.core_head
        if curr == None:
            self.core_head = core_res
            return
        if curr.next == None:
            curr.next = core_res
            return
        while curr.next != None:
            curr = curr.next
        curr.next = core_res

    def core_with_earliest_done(self, start, instructions, additional_node_p_res, needed_memory, schedule_try=0) -> (int, int):
        earliest_starts = []
        curr = self.core_head
        while curr != None:
            additional_core_p_res = [x for x in additional_node_p_res if x.core_id == curr.core_id]
            earliest_starts.append((curr.earliest_start(start, instructions, additional_core_p_res, needed_memory), curr.runtime(instructions)))
            curr = curr.next
        if schedule_try > 0:
            random_pick = random.randint(0,len(earliest_starts)-1)
            if earliest_starts[random_pick][0] != INVALID_START:
                return earliest_starts[random_pick][0]
        earliest = earliest_starts[0]
        for t in earliest_starts:
            if earliest[0][0] == INVALID_START:
                earliest = t
                continue
            if t[0][0] == INVALID_START:
                continue
            if t[0][0]+t[1] < earliest[0][0]+earliest[1]:
                earliest = t
        return earliest[0]
    
    def find_earliest_done_timeframe_p_res(self, start, approx_runtime, approx_needed_memory)-> ((int, int), (int, str)):
        curr = self.core_head
        timeframes = []
        while curr != None:
            timeframes.append(curr.find_possible_timeframe_p_res(start, approx_runtime, approx_needed_memory))
            curr = curr.next
        earliest = timeframes[0]
        for t in timeframes:
            if earliest == INVALID_FRAME:
                earliest = t
                continue
            if t[0] == INVALID_FRAME:
                continue
            # NICE-TO-HAVE check the memory as in: chose the timeframe with the core that barly matches the memory requirements
            if t[0][1] < earliest[0][1]:
                earliest = t
        return t
    
    def remove_inactive_test_frames(self):
        curr = self.core_head
        while curr != None:
            curr.remove_inactive_test_frames()
            curr = curr.next

    def cleanup_res(self, job_id):
        curr = self.core_head
        while curr != None:
            
            curr.cleanup_res(job_id)
            curr.cleanup_res(job_id, res_type=PROCESS) # cleanup does not work yet
            if DEBUG:
                print(f"All cleaned of job {job_id}?")
                curr.info_process_res()
                curr.info_res()
            curr = curr.next
    
    def sufficient_memory(self, needed_memory)-> bool:
        return self.shared_memory/ len(self.core_ids) >= needed_memory

    def runtime(self, instructions)-> int: # in seconds
        return math.ceil(instructions/self.instructions_pre_sec)
    
    def taken_mem_over_time(self, start, end)-> int:
        taken_mem_over_time = 0
        curr = self.core_head
        while curr != None:
            taken_mem_over_time += curr.taken_mem_over_time(start, end)
            curr = curr.next
        return taken_mem_over_time
    
    def get_res_description_by_job(self, job_id) -> dict[str,list[str]]:
        curr = self.core_head
        by_core = {}
        while curr != None:
            tasks = curr.get_res_description_by_job(job_id)
            if len(tasks) > 0:
                by_core[curr.core_id] = tasks
            curr = curr.next
        return by_core

class ReservationStore:
    def __init__(self):
        self.node_head = None

    def find(self, node_id) -> NodeReservation:
        curr = self.node_head
        while curr != None:
            if curr.node_id == node_id:
                return curr
            curr = curr.next
        return None

    def add(self, node_res):
        curr = self.node_head
        if curr == None:
            self.node_head = node_res
            return
        if curr.next == None:
            curr.next = node_res
            return
        while curr.next != None:
            curr = curr.next
        curr.next = node_res

    def nodes_with_sufficient_memory(self, needed_memory)-> list[NodeReservation]:
        curr = self.node_head
        nodes = []
        if curr == None:
            raise StructureException("No nodes found in the reservation store")
        while curr != None:
            if curr.sufficient_memory(needed_memory):
                nodes.append(curr)
            curr = curr.next
        return nodes

    def node_with_earliest_done(self, start, instructions, nodes, additional_p_res, needed_memory, schedule_try=0)-> ((int, int), int, int):
        earliest_starts = []
        for n in nodes:
            additional_node_p_res = [x for x in additional_p_res if x.node_id == n.node_id]
            earliest_starts.append((n.core_with_earliest_done(start, instructions, additional_node_p_res, needed_memory, schedule_try), n.node_id, n.runtime(instructions)))
        if schedule_try > 0:
            random_pick = random.randint(0,len(earliest_starts)-1)
            if earliest_starts[random_pick][0] != INVALID_START:
                return earliest_starts[random_pick]
        earliest = earliest_starts[0]
        for t in earliest_starts:
            if earliest[0][0] == INVALID_START:
                earliest = t
                continue
            if t[0][0] == INVALID_START:
                continue
            if t[0][0]+t[2] < earliest[0][0]+earliest[2]: # core is faster and finishes earlier than the other
                earliest = t
        return earliest
    
    def earliest_done_timeframe_p_res(self, start, approx_runtime, approx_needed_memory, nodes)-> ((int, int), (int, str)):
        timeframes = []
        for n in nodes:
            timeframes.append(n.find_earliest_done_timeframe_p_res(start, approx_runtime, approx_needed_memory))
            earliest = timeframes[0]
        for t in timeframes:
            if earliest == INVALID_FRAME:
                earliest = t
                continue
            if t[0] == INVALID_FRAME:
                continue
            if t[0][1] < earliest[0][1]: # ends earlier NICE-TO-HAVE: could additionally check for instructions_pre_sec from node; higher amount -> faster done
                earliest = t
        return earliest

    def __repr__(self):
        nodes = []
        curr = self.node_head
        while curr != None:
            nodes.append("node "+str(curr.node_id)+"\n"+repr(curr))
            curr = curr.next
        return "\n".join(nodes)

    def init_reservation_for_cores(self):
        csvfile = open("data/machine_details.csv", newline='')
        reader = csv.DictReader(csvfile, delimiter=";")
        for row in reader:
            if "#" in row['node_id']: #ignore lines that are commented out
                continue
            core_ids = row['cores'].split(",")
            node_res = NodeReservation(int(row['node_id']), int(row['total_shared_memory']), core_ids, int(row['instructions_pre_sec']))
            self.add(node_res)
            for core_id in core_ids:
                new_res = CoreReservation(core_id, node_res.shared_memory, len(node_res.core_ids), node_res.node_id, node_res.instructions_pre_sec)
                node_res.add(new_res)

    def add_reservation(self, preferred_core: CoreReservation, start: int, deadline: int, instructions: int, needed_memory: int, job_id: int, process_id: int, task_id: int, additional_p_res: ProcessReservation, schedule_try=0) -> (TaskReservation, CoreReservation):
        filtered_add_p_res = [x for x in additional_p_res if x.process_id != process_id]
        node_id, core_id = -1, -1
        if preferred_core == None: # or needed_memory > preferred_core.memory: # mem check causes process to switch between cores if max appro mem not checked against preferred core before
            nodes = self.nodes_with_sufficient_memory(needed_memory)
            if len(nodes) == 0:
                raise ReservationException("Not enough resources to map task")
            tmp = self.node_with_earliest_done(start, instructions, nodes, filtered_add_p_res, needed_memory, schedule_try)
            if DEBUG:
                print(f"timeslot in which the task finishes the fastes {tmp}")
            new_start, core_id = tmp[0]
            node_id = tmp[1]
        else:
            core_p_res = [x for x in filtered_add_p_res if x.node_id == preferred_core.node_id and x.core_id == preferred_core.core_id]
            new_start, core_id = preferred_core.earliest_start(start, instructions, core_p_res, needed_memory)
            node_id = preferred_core.node_id
        if new_start == INVALID_START:
            raise ReservationException(f"No possible core found to map task {task_id} to")
        n = self.find(node_id)
        core = n.find(core_id)
        runtime = core.runtime(instructions)
        end = new_start+runtime
        new_res = TaskReservation(new_start, end, job_id, process_id, task_id, needed_memory)
        if new_res.end > deadline:
            raise ReservationException(f"Deadline ({deadline}) exeeded! job {job_id} process {process_id} task {task_id} tried to reserve for the time {new_start}-{end}")
        core.add(new_res)
        return new_res, core
    
    def cleanup_res(self, job_id):
        curr = self.node_head
        while curr != None:
            curr.cleanup_res(job_id)
            curr = curr.next

    def add_reservation_for_process(self, job_id, deadline: int, process: job.Process, start, additional_p_res: [ProcessReservation], schedule_try=0)-> (TaskReservation, int):
        curr = process.task_head
        actual_process_start = -1
        actual_task_start = start
        latest_res = None
        pref_core = None
        p = None
        add_p_res_for_child = additional_p_res
        child_join_to_task = {}
        join_in_task = -1
        while curr != None:
            if curr.process_id != process.process_id and curr.action == 3: # JOIN TO OTHER PROCESS
                join_in_task = curr.task_id
                break # process gets joined to other process, last task of forked process is reached
            if curr.action == 3 and curr.process_id == process.process_id: # JOIN
                if curr.task_id in child_join_to_task: # if False, join task concerns own process
                    last_child_res = child_join_to_task[curr.task_id]
                    actual_task_start = last_child_res.end
            latest_res, pref_core = self.add_reservation(pref_core, actual_task_start, deadline, curr.instructions, curr.needed_memory, job_id, process.process_id, curr.task_id, additional_p_res, schedule_try)
            if actual_process_start == -1:
                actual_process_start = latest_res.start
            actual_task_start = latest_res.end
            if p == None:
                p = ProcessReservation(pref_core.node_id, pref_core.core_id, actual_process_start, -1, job_id, process.process_id, process.approx_needed_memory)
                add_p_res_for_child.append(p)
            if curr.action == 2: # FORK
                tmp_res, tmp_join_task = self.add_reservation_for_process(job_id, deadline, curr.child_process, latest_res.end, add_p_res_for_child, schedule_try)
                child_join_to_task[tmp_join_task] = tmp_res
            curr = curr.next
        if latest_res == None:
            raise ReservationException("No process with actual task to map provided")
        p.end = latest_res.end
        try:
            pref_core.add_process_res(p)
        except Exception as e:
            raise ReservationException(str(e))
        return latest_res, join_in_task

    def add_test_p_reservation(self, start: int, deadline: int, approx_runtime: int, approx_needed_memory: int, job_id: int, process_id: int) -> ProcessReservation:
        node_id, core_id = -1, -1
        nodes = self.nodes_with_sufficient_memory(approx_needed_memory)
        if len(nodes) == 0:
            raise ValidationException("Not enough resources to map task")
        tmp = self.earliest_done_timeframe_p_res(start, approx_runtime, approx_needed_memory, nodes)
        new_start, end = tmp[0]
        node_id, core_id = tmp[1]
        if new_start == INVALID_START:
            raise ValidationException("No possible timeframe was found")
        n = self.find(node_id)
        core = n.find(core_id)
        new_res = ProcessReservation(node_id, core_id, new_start, end, job_id, process_id, approx_needed_memory, active=False)
        if new_res.end > deadline:
            raise ValidationException(f"Deadline ({deadline}) exeeded! job {job_id} process {process_id} tried to reserve for the time {new_start}-{new_res.end}")
        try:
            if DEBUG:
                new_res.info()
            core.add_process_res(new_res)
        except Exception as e:
            raise ValidationException(str(e))
        return new_res
    
    def clean_test_mapping(self):
        curr = self.node_head
        while curr != None:
            curr.remove_inactive_test_frames()
            curr = curr.next

    def total_memory(self)-> int:
        memory = []
        curr = self.node_head
        while curr != None:
            memory.append(curr.shared_memory)
            curr = curr.next
        return sum(memory)
    
    def memory_taken_over_time(self, start, deadline)-> int:
        taken_mem_over_time = 0
        curr = self.node_head
        while curr != None:
            taken_mem_over_time += curr.taken_mem_over_time(start, deadline)
            curr = curr.next
        return taken_mem_over_time

    def get_res_description_by_job(self, job_id)-> dict[int, dict[str,list[str]]]:
        curr = self.node_head
        by_node = {}
        while curr != None:
            cores = curr.get_res_description_by_job(job_id)
            if cores: # dict not empty
                by_node[curr.node_id] = cores
            curr = curr.next
        return by_node

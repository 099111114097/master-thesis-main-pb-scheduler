import math
import csv

PROCESS_IDLE_PERCENTAGE = 0.2 # assumes how much percentage of the process runtime is actual waiting time (so other tasks/processes can be executed inbetween)
NULL_FRAME = (-1, -1)
NULL_START = -1

# this version splits the shared memory fixed between the cores instead of dynamic balancing
class Reservation:
    def __init__(self, start: int, end, job_id, process_id, task_id, memory_to_res, active=False):
        self.start = start
        self.end = end
        self.job_id = job_id # in combination wiht active indicates wether to ignore in planning or not
        self.process_id = process_id
        self.task_id = task_id
        self.memory_reserved = memory_to_res
        self.next = None
        self.active = active #false for planning if actually added set to True
    
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
        self.idel_used = False # make sure that if idel_used is True - test_idel_user is also true
        self.test_idel_used = False # this is set to True in case in our testing the waiting time would be used for another process
        self.active = active

    def info(self):
        print(f"start {self.start}, end {self.end}, job {self.job_id}, process {self.process_id}, memory {self.memory_to_res}")

    def process_res_in_interval(self, start, end):
        def point_in_interval(time, start, end):
            return time >= start and time <= end
        return (self.start <= start and self.end >= end) or (point_in_interval(self.start, start, end) or point_in_interval(self.end, start, end))
    
    def idel_used(self):
        self.idel_used = True
        self.test_idel_used = True

    def test_idel_used_set(self):
        self.test_idel_used = True

    def test_idel_time(self):
        if self.test_idel_used:
            return 0
        return (self.end-self.start)*PROCESS_IDLE_PERCENTAGE
    
    def overlap(self, approx_end): # overlap of new to schedule process end with this process_res, can be greater than process_res it self, = remaining to schedule of process 
        return approx_end - self.start

class CoreReservation:
    def __init__(self, core_id, node_memory, core_amount, node_id, instructions_pre_sec):
        self.head = None
        self.memory = node_memory/core_amount
        self.core_id = core_id
        self.node_id = node_id
        self.instructions_pre_sec = instructions_pre_sec
        self.reservation_head = None
        self.process_res_head = None
        self.next = None

    def info(self):
        print(f"node {self.node_id}, core {self.core_id}")

    def add(self, reservation, res_type="task"):
        if res_type == "process":
            if self.process_res_head == None:
                self.process_res_head = reservation
                return
            res = self.process_res_head
        if res_type == "task":
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
        if self.process_res_head == None:
            self.process_res_head = reservation
            return
        res = self.process_res_head
        if res.start > reservation.start:
            self.process_res_head = reservation
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

    def timeframe_possible(self, head_start, start, approx_runtime, approx_needed_memory):
        if head_start == None:
            #print("head empty")
            return start, start+approx_runtime
        if start+approx_runtime <= head_start.start:
            #print("next p_res starts before runtime ends")
            return start, start+approx_runtime
        else:
            if self.memory - head_start.memory_to_res >= approx_needed_memory: # overlap would be acceptable
                overlap = head_start.overlap(start+approx_runtime)
                approx_waiting = head_start.test_idel_time()
                #print(f"approx waiting for core {self.core_id}: {approx_waiting}")
                if overlap <= approx_waiting:
                    head_start.test_idel_used_set()
                    #print("process can run on idle time")
                    return start, start+approx_runtime
                rest_overlap = overlap - approx_waiting # approc_waiting can be 0 if idle time was already used by other process_res
                if head_start.next == None:
                    #print("process can run on idle time and enough time free after")
                    head_start.test_idel_used_set()
                    return start, head_start.end+rest_overlap
                else:
                    #print("gotta check if next timeframe okay")
                    timeframe = self.timeframe_possible(head_start.next, head_start.end, rest_overlap, approx_needed_memory) 
                    if timeframe != (-1,-1):
                        #print("timeframe fine")
                        head_start.test_idel_used_set()
                        return timeframe
                    else: 
                        #print("gotta try later timeframes")
                        return self.timeframe_possible(head_start.next, head_start.end, approx_runtime, approx_needed_memory)
            else: 
                #print("gotta try later timeframes")
                return self.timeframe_possible(head_start.next, head_start.end, approx_runtime, approx_needed_memory)
                #if self.memory - head_start.next.memory_to_res > approx_needed_memory:
                #    if head_start.next.start >= head_start.end+rest_overlap:
                #        head_start.test_idel_used_set() #TODO working here
                #        return start, head_start.end+rest_overlap
                #    rest_overlap = head_start.next
                #else:


    def find_possible_timeframe_p_res(self, start, approx_runtime, approx_needed_memory): # find the earliest start, end for a new p_res
        if self.memory < approx_needed_memory: # no valid timeframe possible
            #print("not enough memory")
            return ((-1, -1), (self.node_id, self.core_id))
        if self.process_res_head == None:
            #print("no p_res present")
            return ((start, start+approx_runtime), (self.node_id, self.core_id))
        timeframe = self.timeframe_possible(self.process_res_head, start, approx_runtime, approx_needed_memory)
        #print(timeframe)
        return (timeframe, (self.node_id, self.core_id))
    
    def remove_inactive_test_frames(self):
        curr = self.process_res_head
        if curr == None:
            return
        if not curr.active:
            if curr.next == None:
                self.process_res_head = None
                return
            else:
                self.process_res_head = curr.next
        if curr.next == None:
            return
        while curr.next != None:
            if not curr.next.active:
                curr.next = curr.next.next
            else:
                curr = curr.next
        return

    def __repr__(self):
        node = self.reservation_head
        nodes = []
        while node != None:
            nodes.append(str(node.start)+"-"+str(node.end)+":"+str(node.process_id))
            node = node.next
        nodes.append("None")
        return " -> ".join(nodes)
    
    def process_info(self):
        node = self.process_res_head
        nodes = []
        while node != None:
            nodes.append(str(node.start)+"-"+str(node.end)+":"+str(node.task_id))
            node = node.next
        nodes.append("None")
        print(" -> ".join(nodes))

    def runtime(self, instructions): # in seconds
        return math.ceil(instructions/self.instructions_pre_sec)
    
    def taken_mem_over_time(self, start, end):
        taken_mem_over_time = 0
        curr = self.process_res_head
        while curr != None: 
            if curr.process_res_in_interval(start, end):
                taken_mem_over_time += (min(curr.end, end)-max(curr.start, start))*curr.memory_to_res
            #if curr.start > end: #process res are sorted so later process_res are not in timeframe
            #    break TODO is this true?
            curr = curr.next
        return taken_mem_over_time
    
    def mem_res_in_timeframe(self, start, end):
        mem_res = 0
        curr = self.process_res_head
        while curr != None: 
            if curr.process_res_in_interval(start, end):
                mem_res += curr.memory_to_res
            #if curr.start > end: #process res are sorted so later process_res are not in timeframe
            #    break TODO is this true?
            curr = curr.next
        return mem_res

        
    # in the static memory split version we already checked that the memory of one core is enough so the found lot should work
    def earliest_start(self, possible_start, instructions, additional_p_res, needed_memory, prefers_core=False)-> (int, int): # return earliest free timeslot from given start point that is long enough (>=runtime) -1 means no endling and the last existing reservation is returned
        def additional_p_res_mem(additional_p_res):
            if len(additional_p_res) == 0:
                return 0
            res_mem = 0
            for p in additional_p_res:
                res_mem += p.memory_to_res
            return res_mem
        runtime = self.runtime(instructions)
        curr = self.reservation_head
        if curr == None:
            return (possible_start, self.core_id)
        if curr.next == None:
            return (max(curr.end, possible_start), self.core_id)
        while curr.next != None:
            if curr.next.start - max(curr.end, possible_start) >= runtime:
                potential_start = max(curr.end, possible_start)
                add_p_res_mem = additional_p_res_mem(additional_p_res)
                if (self.memory - add_p_res_mem) < needed_memory and prefers_core:
                    return (-1, self.core_id) # no valid timeframe can be found as init process blocks core due to memory reservation too high
                if self.too_much_memory_res_by_p(potential_start, potential_start+runtime, add_p_res_mem, needed_memory):
                    curr = curr.next
                    continue
                return  (potential_start, self.core_id)
            curr = curr.next
        return (max(curr.end,possible_start), self.core_id)
    #TODO check also processRes if too much memory is reserved during that time

    def too_much_memory_res_by_p(self, start, end, add_p_res_mem, needed_memory):
        available_mem = self.memory - add_p_res_mem
        mem_res_in_t = self.mem_res_in_timeframe(start, end) #TODO: try to improve as bit inaccurate as if one p_res ends and another p_res starts in timeframe both of their memory res are added up as used. T
        if available_mem - mem_res_in_t < needed_memory:
            return False
        return True

class NodeReservation:
    def __init__(self, node_id, shared_memory, core_ids, instructions_pre_sec):
        self.node_id = node_id
        self.shared_memory = shared_memory
        self.core_ids = core_ids
        self.instructions_pre_sec = instructions_pre_sec
        self.core_head = None
        self.next = None

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

    def core_with_earliest_start(self, start, instructions, additional_p_res, needed_memory, prefers_core=False) -> (int, int):
        earliest_starts = []
        curr = self.core_head
        while curr != None:
            earliest_starts.append((curr.earliest_start(start, instructions, additional_p_res, needed_memory, prefers_core), curr.runtime(instructions)))
            curr = curr.next
        earliest = earliest_starts[0]
        for t in earliest_starts:
            if earliest == NULL_START:
                earliest = t
                continue
            if t[0] == NULL_START:
                continue
            #print(t[0][0], earliest[0][0], t[1], earliest[1])
            if t[0][0] < earliest[0][0] or (t[0] == earliest[0] and t[1] < earliest[1]):
                earliest = t
        return earliest[0]
    
    def find_earliest_done_timeframe_p_res(self, start, approx_runtime, approx_needed_memory):
        curr = self.core_head
        timeframes = []
        while curr != None:
            timeframes.append(curr.find_possible_timeframe_p_res(start, approx_runtime, approx_needed_memory))
            curr = curr.next
        earliest = timeframes[0]
        for t in timeframes:
            #print(t, earliest)
            if earliest == NULL_FRAME:
                earliest = t
                continue
            if t[0] == NULL_FRAME:
                continue
            #TODO does it supposed to also check the memory as in: chose the timeframe with the core that barly matches the memory requirements?
            if t[0][1] < earliest[0][1]:
                earliest = t
        return t
    
    def remove_inactive_test_frames(self):
        curr = self.core_head
        while curr != None:
            curr.remove_inactive_test_frames()
            curr = curr.next
    
    def sufficient_memory(self, needed_memory):
        return self.shared_memory/ len(self.core_ids) >= needed_memory

    def runtime(self, instructions): # in seconds
        return math.ceil(instructions/self.instructions_pre_sec)
    
    def __repr__(self):
        node = self.core_head
        nodes = []
        while node != None:
            nodes.append("- "+str(node.core_id)+":"+repr(node))
            node = node.next
        return "\n".join(nodes)
    
    def taken_mem_over_time(self, start, end):
        taken_mem_over_time = 0
        curr = self.core_head
        while curr != None:
            taken_mem_over_time += curr.taken_mem_over_time(start, end)
            curr = curr.next
        return taken_mem_over_time

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

    def add_node(self, memory, node_id, core_ids):
        node_res = self.find(node_id)
        if node_res != None:
            raise Exception("Error node already exists")
        self.add(NodeReservation(node_id, memory, core_ids))

    def nodes_with_sufficient_memory(self, needed_memory)-> [NodeReservation]:
        curr = self.node_head
        nodes = []
        if curr == None:
            raise Exception("No nodes found in the reservation store")
        while curr != None:
            if curr.sufficient_memory(needed_memory):
                nodes.append(curr)
            curr = curr.next
        return nodes

    #TODO: maybe check if instead of earliest, first to finish is more efficient
    # if core is faster an finish earlier than the other would it is also efficient to use it
    def node_with_earliest_start(self, start, instructions, nodes, additional_p_res, needed_memory, prefers_core=False):
        earliest_starts = []
        for n in nodes:
            earliest_starts.append((n.core_with_earliest_start(start, instructions, additional_p_res, needed_memory, prefers_core), n.node_id, n.runtime(instructions)))
        earliest = earliest_starts[0]
        for t in earliest_starts:
            if earliest == NULL_START:
                earliest = t
                continue
            if t[0] == NULL_START:
                continue
            if t[0][0] < earliest[0][0] or (t[0][0] == earliest[0][0] and t[2] < earliest[2]):
                earliest = t
        return earliest
    
    def earliest_done_timeframe_p_res(self, start, approx_runtime, approx_needed_memory, nodes):
        timeframes = []
        for n in nodes:
            timeframes.append(n.find_earliest_done_timeframe_p_res(start, approx_runtime, approx_needed_memory))
            earliest = timeframes[0]
        print(timeframes)
        for t in timeframes:
            if earliest == NULL_FRAME:
                earliest = t
                continue
            if t[0] == NULL_FRAME:
                continue
            if t[0][1] < earliest[0][1]: # ends earlier TODO could additionally check for instructions_pre_sec from node
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
            core_ids = row['cores'].split(",")
            node_res = NodeReservation(int(row['node_id']), int(row['total_shared_memory']), core_ids, int(row['instructions_pre_sec']))
            self.add(node_res)
            for core_id in core_ids:
                new_res = CoreReservation(core_id, node_res.shared_memory, len(node_res.core_ids), node_res.node_id, node_res.instructions_pre_sec)
                node_res.add(new_res)

    # TODO add deadline like in add_test_p_res
    def add_reservation(self, prefered_core: CoreReservation, start: int, instructions: int, needed_memory: int, job_id: int, process_id: int, task_id: int, additional_p_res: ProcessReservation) -> Reservation:
        node_id, core_id = -1, -1
        if prefered_core == None or needed_memory > prefered_core.memory: # mem check causes process to switch between cores if max appro mem not checked against prefered core before
            nodes = self.nodes_with_sufficient_memory(needed_memory)
            if len(nodes) == 0:
                raise Exception("Not enough resources to map task")
            tmp = self.node_with_earliest_start(start, instructions, nodes, additional_p_res, needed_memory, prefers_core=True)
            new_start, core_id = tmp[0]
            node_id = tmp[1]
        else:
            core_p_res = [x for x in additional_p_res if x.node_id == prefered_core.node_id and x.core_id == prefered_core.core_id]
            new_start, core_id = prefered_core.earliest_start(start, instructions, core_p_res, needed_memory)
            node_id = prefered_core.node_id
        if new_start == -1:
            raise Exception("No no possible core found to map task to")
        if new_start > start: #in case earliest start is later than possible
            start = new_start
        n = self.find(node_id)
        core = n.find(core_id)
        runtime = core.runtime(instructions)
        new_res = Reservation(start, start+runtime, job_id, process_id, task_id, needed_memory) # reservation not active 
        #new_res.info()
        core.add(new_res)
        #core.info()
        return new_res, core
    
    def add_test_p_reservation(self, start: int, deadline: int, approx_runtime: int, approx_needed_memory: int, job_id: int, process_id: int) -> Reservation:
        node_id, core_id = -1, -1
        nodes = self.nodes_with_sufficient_memory(approx_needed_memory)
        if len(nodes) == 0:
            raise Exception("Not enough resources to map task")
        tmp = self.earliest_done_timeframe_p_res(start, approx_runtime, approx_needed_memory, nodes)
        new_start, end = tmp[0]
        node_id, core_id = tmp[1]
        if new_start == -1:
            raise Exception("no possible timeframe was found")
        if new_start > start: #in case earliest start is later than possible
            start = new_start
        n = self.find(node_id)
        core = n.find(core_id)
        new_res = ProcessReservation(node_id, core_id, start, end, job_id, process_id, approx_needed_memory, active=False) # reservation not active yet TODO maybe add field to state for which job this inactive p_res is
        #new_res.info()
        if new_res.end > deadline:
            raise Exception("deadline exeeded!!")
        core.add(new_res, res_type="process")
        #core.info()
        return new_res
    
    def clean_test_mapping(self):
        curr = self.node_head
        while curr != None:
            curr.remove_inactive_test_frames()
            curr = curr.next

    def total_memory(self):
        memory = []
        curr = self.node_head
        while curr != None:
            memory.append(curr.shared_memory)
            curr = curr.next
        return sum(memory)
    
    def memory_taken_over_time(self, start, deadline):
        taken_mem_over_time = 0
        curr = self.node_head
        while curr != None:
            taken_mem_over_time += curr.taken_mem_over_time(start, deadline)
            curr = curr.next
        return taken_mem_over_time

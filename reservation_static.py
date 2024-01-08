import math
import csv

# this version splits the shared memory fixed between the cores instead of dynamic balancing
class Reservation:
    def __init__(self, start, end, job_id, process_id, task_id, memory_to_res, active=False):
        self.start = start
        self.end = end
        self.job_id = job_id # in combination wiht active indicates wether to ignore in planning or not
        self.process_id = process_id
        self.task_id = task_id
        self.memory_reserved = memory_to_res
        self.next = None
        self.active = active #false for planning if actually added set to True

class CoreReservation:
    def __init__(self, core_id, node_memory, core_amount, node_id, instructions_pre_sec):
        self.head = None
        self.memory = node_memory/core_amount
        self.core_id = core_id
        self.node_id = node_id
        self.instructions_pre_sec = instructions_pre_sec
        self.reservation_head = None
        self.next = None

    def add(self, reservation):
        if self.reservation_head == None:
            self.reservation_head = reservation
            return
        res = self.reservation_head
        if res.start > reservation.start:
            self.reservation_head = reservation
            reservation.next = res
            return
        if res.next == None:
            res.next = reservation
            return
        while res.next != None:
            if reservation.start < res.next.start:
                reservation.next = res.next
                res.next = reservation
                return
            res = res.next
        res.next = reservation
    
    def __repr__(self):
        node = self.reservation_head
        nodes = []
        while node != None:
            nodes.append(str(node.start)+"-"+str(node.end)+":"+str(node.task_id))
            node = node.next
        nodes.append("None")
        return " -> ".join(nodes)

    def runtime(self, instructions): # in seconds
        return math.ceil(instructions/self.instructions_pre_sec)

    # in the static memory split version we already checked that the memory of one core is enough so the found lot should work
    def earliest_start(self, possible_start, instructions)-> (int, int): # return earliest free timeslot from given start point that is long enough (>=runtime) -1 means no endling and the last existing reservation is returned
        runtime = self.runtime(instructions)
        curr = self.reservation_head
        if curr == None:
            return (possible_start, self.core_id)
        if curr.next == None:
            return (max(curr.end, possible_start), self.core_id)
        while curr.next != None:
            if curr.next.start - max(curr.end, possible_start) >= runtime:
                return  (curr.end, self.core_id)
            curr = curr.next
        return (max(curr.end,possible_start), self.core_id)

    # this func checks that the possible time is valid for the found timeslot
    #def enough_time(self, time, end, runtime):
    #    return time < end and end - time >= runtime

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

    def core_with_earliest_start(self, start, instructions) -> (int, int):
        earliest_starts = []
        curr = self.core_head
        while curr != None:
            earliest_starts.append((curr.earliest_start(start, instructions), curr.runtime(instructions)))
            curr = curr.next
        earliest = earliest_starts[0]
        for t in earliest_starts:
            #print(t[0][0], earliest[0][0], t[1], earliest[1])
            if t[0][0] < earliest[0][0] or (t[0] == earliest[0] and t[1] < earliest[1]):
                earliest = t
        return earliest[0]
    
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
        nodes = [] #TODO ids or rather the whole object?
        if curr == None:
            raise Exception("No nodes found in the reservation store")
        while curr != None:
            if curr.sufficient_memory(needed_memory):
                nodes.append(curr)
            curr = curr.next
        return nodes

    #TODO: maybe check if instead of earliest, first to finish is more efficient
    # if core is faster an finish earlier than the other would it is also efficient to use it
    def node_with_earliest_start(self, start, instructions, nodes):
        earliest_starts = []
        for n in nodes:
            earliest_starts.append((n.core_with_earliest_start(start, instructions), n.node_id, n.runtime(instructions)))
        earliest = earliest_starts[0]
        print(earliest_starts)
        for t in earliest_starts:
            if t[0][0] < earliest[0][0] or (t[0][0] == earliest[0][0] and t[2] < earliest[2]):
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

## HELPER FUNCS

def add_reservation(rs: ReservationStore, prefered_core: CoreReservation, start: int, instructions: int, needed_memory: int, job_id: int, process_id: int, task_id: int) -> Reservation:
    node_id, core_id = -1, -1
    if prefered_core == None or needed_memory > prefered_core.memory:
        nodes = rs.nodes_with_sufficient_memory(needed_memory)
        tmp = rs.node_with_earliest_start(start, instructions, nodes)
        new_start, core_id = tmp[0]
        node_id = tmp[1]
    else:
        new_start, core_id = prefered_core.earliest_start(start, instructions)
        node_id = prefered_core.node_id
    if new_start > start: #in case earliest start is later than possible
        start = new_start
    n = rs.find(node_id)
    core = n.find(core_id)
    runtime = core.runtime(instructions)
    new_res = Reservation(start, start+runtime, job_id, process_id, task_id, needed_memory) # reservation not active yet
    core.add(new_res)
    return core

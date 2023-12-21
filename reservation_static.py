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
    def __init__(self, core_id, node_memory, core_amount, node_id):
        self.head = None
        self.memory = node_memory/core_amount
        self.core_id = core_id
        self.node_id = node_id
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

    # in the static memory split version we already checked that the memory of one core is enough so the found lot should work
    def earliest_start(self, possible_start, runtime)-> (int, int): # return earliest free timeslot from given start point that is long enough (>=runtime) -1 means no endling and the last existing reservation is returned
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
    def __init__(self, node_id, shared_memory, core_ids):
        self.node_id = node_id
        self.shared_memory = shared_memory
        self.core_ids = core_ids
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

    def core_with_earliest_start(self, start, runtime) -> (int, int):
        earliest_starts = []
        curr = self.core_head
        while curr != None:
            earliest_starts.append(curr.earliest_start(start, runtime))
            curr = curr.next
        earliest = earliest_starts[0]
        for t in earliest_starts:
            if t[0] < earliest[0]:
                earliest = t
        return earliest
    
    def sufficient_memory(self, needed_memory):
        return self.shared_memory/ len(self.core_ids) >= needed_memory
    
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

    def node_with_earliest_start(self, start, runtime, nodes):
        earliest_starts = []
        for n in nodes:
            earliest_starts.append((n.core_with_earliest_start(start, runtime), n.node_id))
        earliest = earliest_starts[0]
        for t in earliest_starts:
            if t[0][0] < earliest[0][0]:
                earliest = t
        return earliest

    def __repr__(self):
        nodes = []
        curr = self.node_head
        while curr != None:
            nodes.append("node "+str(curr.node_id)+"\n"+repr(curr))
            curr = curr.next
        return "\n".join(nodes)
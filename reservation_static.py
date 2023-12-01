# this version splits the shared memory fixed between the cores instead of dynamic balancing
class Reservation:
    def __init__(self, start, end, process_id, task_id, memory_to_res):
        self.start = start
        self.end = end
        self.process_id = process_id
        self.task_id = task_id
        self.memory_reserved = memory_to_res
        self.next = None

class CoreReservation:
    def __init__(self, core_name, node_memory, core_amount):
        self.head = None
        self.memory = node_memory/core_amount
        self.core_name = core_name
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
        node = self.head
        nodes = []
        while node != None:
            nodes.append(str(node.time_slot)+":"+str(node.process_id)+":"+str(node.task_id))
            node = node.next
        nodes.append("None")
        return " -> ".join(nodes)

    # in the static memory split version we already checked that the memory of one core is enough so the found lot should work
    def earliest_start(self, start_timeslot, runtime)-> (int,int, Reservation): # return earliest free timeslot from given start point that is long enough (>=runtime) -1 means no endling and the last existing reservation is returned
        curr = start_timeslot
        if curr == None:
            return (0,-1, None)
        if curr.next == None:
            return (curr.end, -1, curr)
        while curr.next != None:
            if curr.next.start - curr.end >= runtime:
                return  (curr.end,curr.next.start, curr)
            curr = curr.next
        return (curr.end, -1, curr)
    
    def get_max_reserved_memory(self, start, end)-> int: # returns max reserved memory during this time slot
        curr = self.reservation_head
        reserved_memory = []
        if curr == None:
            return 0
        while curr != None:
            if (start >= curr.start and start <= curr.end) or (end >= curr.start and end <= curr.end) or (start <= curr.start and end >= curr.end): # covers start/end is in one time slot or spans over the whole thing
                reserved_memory.append(curr.memory_reserved)
            curr = curr.next
        return max(reserved_memory)
    
    def get_reserved_memory(self, start, end)-> int: # returns max reserved memory during this time slot
        curr = self.reservation_head
        reserved_memory = []
        if curr == None:
            return 0
        while curr != None:
            if (start >= curr.start and start <= curr.end) or (end >= curr.start and end <= curr.end) or (start <= curr.start and end >= curr.end): # covers start/end is in one time slot or spans over the whole thing
                reserved_memory.append((curr.memory_reserved, curr)) # so I can check the start and end and to know if I can shift a hole time slot if too much memory reserved
            curr = curr.next
        return max(reserved_memory)

class NodeReservation:
    def __init__(self, node_id, shared_memory, core_ids):
        self.node_id = node_id
        self.shared_memory = shared_memory
        self.core_ids = core_ids
        self.core_head = None
        self.next = None

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
            if curr.shared_memory/ len(curr.core_ids) >= needed_memory:
                nodes.append(curr)
            curr = curr.next
        return nodes

    def __repr__(self):
        nodes = []
        curr = self.node_head
        while curr != None:
            nodes.append(str(curr.node_id)+":"+str(curr.shared_memory))
            curr = curr.next
        nodes.append("None")
        return " -> ".join(nodes)
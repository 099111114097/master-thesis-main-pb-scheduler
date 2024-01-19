class CoreReservation:
    def __init__(self, core_id, process_id, task_id, memory_consumption):
        self.core_id = core_id
        self.process_id = process_id
        self.task_id = task_id
        self.memory_consumption = memory_consumption

class TimePointReservation:
    def __init__(self, time_point, available_memory, amount_cores):
        #self.start = start
        #self.end = end
        self.time_point = time_point
        self.available_memory = available_memory
        self.amount_cores = amount_cores
        self.cores = []
        self.prev = None
        self.next = None

    def reserve_memory(self, memory)-> bool:
        if self.available_memory < memory:
            return False
        self.available_memory -= memory
        return True
        
    def free_memory(self, memory):
        self.available_memory += memory
    
    def sufficient_resources(self, needed_memory):
        return self.amount_cores > len(self.cores) and self.available_memory >= needed_memory:

class NodeReservation:
    def __init__(self, node_id, shared_memory, core_ids):
        self.node_id = node_id
        self.shared_memory = shared_memory
        self.core_ids = core_ids
        self.time_point_head = None
        self.next = None

    def earliest_available_time_slot(self, start, end, memory_needed)-> TimePointReservation: # int currently used as time indication
        curr = self.time_point_head
        if curr == None:
                ts = TimePointReservation(start, end, self.shared_memory, len(self.core_ids))
                self.time_point_head = ts
                return ts
        else:
            if len(curr.cores) < self.core_ids and curr.available_memory >= memory_needed and start >= curr.start:
                if curr.end >= end:
                    return curr
                
        while curr != None:
            curr = curr.next

class Reservierung:
    def __init__(self):
        self.node_res_head = None

    def add(self, node):
        curr = self.node_res_head
        if curr == None:
            self.node_res_head = node
            return
        while curr.next != None:
            curr = curr.next
        curr.next = node

    def earliest_available_reservation(self, start, runtime, memory_needed):
        def filter_nodes(node):
            return node.shared_memory < memory_needed
        nodes = []
        curr = self.node_res_head
        while curr != None:
            nodes.append(curr)
            curr = curr.next
        possible_nodes = list(filter(filter_nodes, nodes))
        for node in possible_nodes:



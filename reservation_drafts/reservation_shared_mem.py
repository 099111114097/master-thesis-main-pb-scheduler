import csv

class CoreReservation:
    def __init__(self, core_id):
        self.core_id = core_id
        self.process_id = None
        self.task_id = None
        self.needed_memory = 0
        self.next = None

class TimeSlotReservation:
    def __init__(self, machine_id, time, memory):
        self.machine_id = machine_id
        self.time = time
        self.nodes_head = None
        self.free_memory = memory
        self.next = None

    def find_node(self, node_id):
        if self.nodes_head == None:
            return None
        node = self.nodes_head
        while node != None:
            if node.node_id == node_id:
                return node
            node = node.next
        return None

    def add_task(self, task_id, node_id, needed_memory):
        if self.free_memory < needed_memory:
            raise Exception(f"Error not enough memory in timeslot {self.time} for task {task_id}")
            
        node_res = self.find_node(node_id)
        if node_res == None:
            raise Exception("Node not existend")
        node_res.task_id = task_id
        node_res.needed_memory = needed_memory
        if self.nodes_head == None:
            self.nodes_head = node_res
        elif self.nodes_head.next == None:
            self.nodes_head.next = node_res
        curr = self.nodes_head
        while curr.next != None:
            curr = curr.next
        self.free_memory -= needed_memory

    def get_free_memory(self):
        return self.free_memory

    def get_free_cpu(self):
        free_cpus = []
        node = self.nodes_head
        while node != None:
            if node.task_id == None:
                free_cpus.append(node.node_id)
            node = node.next

class ReservationStore:
    def __init__(self):
        self.head = None

    def add_time_slot(self, machine_id, time):
        r = machine_details_reader()
        memory = 0
        for row in r:
            if row['machine_id'] == machine_id:
                memory = row['total_shared_memory']
        if memory == 0:
            raise Exception(f"no data for machine {machine_id}")
        time_slot = TimeSlotReservation(machine_id, time, memory)
        if self.head == None:
            self.head = time_slot
            return
        res = self.head
        if res.time > time:
            self.head = time_slot
            time_slot.next = res
            return
        if res.next == None:
            res.next = time_slot
            return
        while res.next != None:
            if res.time < time and res.next.time > time:
                time_slot.next = res.next
                res.next = time_slot
                return
            res = res.next
        res.next = time_slot

    def free_nodes_on_machine(self, machine_id, time):


def machine_details_reader():
    csvfile = open("data/machine_details.csv", newline='')
    return csv.DictReader(csvfile, delimiter=";")
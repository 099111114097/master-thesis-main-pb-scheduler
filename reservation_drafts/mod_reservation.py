class Reservation:
    def __init__(self, time_slot, process_id, task_id, memory):
        self.time_slot = time_slot
        self.process_id = process_id
        self.task_id = task_id
        self.memory = memory
        self.next = None
    
class NodeRes:
    def __init__(self, id):
        self.id = id
        self.head = None
        self.next = None

    def add(self, reservation):
        if self.head == None:
            self.head = reservation
            return
        res = self.head
        if res.time_slot > reservation.time_slot:
            self.head = reservation
            reservation.next = res
            return
        if res.next == None:
            res.next = reservation
            return
        while res.next != None:
            res = res.next
        res.next = reservation
        

class MachineRes:
    def __init__(self, id, memory):
        self.id = id
        self.memory = memory
        self.node_head = None
        self.next = None

    def add_node(self, node):
        curr = self.node_head
        if curr == None:
            self.node_head = node
            return
        if curr.next == None:
            self.node_head.next = node
        while curr.next != None:
            curr = curr.next
        curr.next = node

    def find_node(self, node_id) -> NodeRes:
        node = self.node_head
        while node != None:
            if node.id == node_id:
                return node
            node = node.next
        return None
    
    def get_memory_total(self):
        return self.memory
    
class ReservationStore:
    def __init__(self):
        self.machine_head = None

    def add_machine(self, machine):
        curr = self.machine_head
        if curr == None:
            self.machine_head = machine
            return
        if curr.next == None:
            self.machine_head.next = machine
        while curr.next != None:
            curr = curr.next
        curr.next = machine

    def find_machine(self, machine_id) -> MachineRes:
        machine = self.machine_head
        while machine != None:
            if machine.id == machine_id:
                return machine
            machine = machine.next
        return None

    def find_node(self, machine_id, node_id) -> NodeRes:
        machine = self.find_machine(machine_id)
        if machine == None:
            return machine
        return machine.find_node(node_id)

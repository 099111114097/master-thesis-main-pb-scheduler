import csv
import reservation_phd as r

#def read_plan():



def machine_details_reader():
    csvfile = open("data/machine_details.csv", newline='')
    return csv.DictReader(csvfile, delimiter=";")

def init_reservation_for_cores(reservations):
    reader = machine_details_reader()
    for row in reader:
            new_res = r.NodeReservation(row['node_id'], row['total_shared_memory'], row['cores'].split(","))
            reservations.add(new_res)

def read_model():
    csvfile = open("data/memory_consumption_model_example.csv", newline='')
    model_reader = csv.DictReader(csvfile, delimiter=";")
    for row in model_reader:
        print(row['job_id'], row['process_id'], row['memory_consumption'])

def validate_request(): # check that resources that job can actually be finished by deadline and that we have the necessary resources + their amount availanble to do so while still meeting the deadline
    return

def main():
    #read_model()
    re = r.Reservierung()
    init_reservation_for_cores(re)
    print(repr(re))

    curr = re.node_res_head
    while curr != None:
        print(str(curr.node_id)+":"+str(curr.shared_memory)+":"+",".join(curr.core_ids))
        curr = curr.next 

if __name__ == "__main__":
    main()



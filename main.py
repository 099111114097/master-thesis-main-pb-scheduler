import csv
import reservation_static as re

#def read_plan():



def machine_details_reader():
    csvfile = open("data/machine_details.csv", newline='')
    return csv.DictReader(csvfile, delimiter=";")
    
def init_reservation_for_cores(res_store):
    reader = machine_details_reader()
    for row in reader:
        core_ids = row['cores'].split(",")
        node_res = re.NodeReservation(int(row['node_id']), int(row['total_shared_memory']), core_ids)
        res_store.add(node_res)
        for core_id in core_ids:
            new_res = re.CoreReservation(core_id, int(row['total_shared_memory']), len(core_ids))
            node_res.add(new_res)

def read_model():
    csvfile = open("data/memory_consumption_model_example.csv", newline='')
    model_reader = csv.DictReader(csvfile, delimiter=";")
    for row in model_reader:
        print(int(row['job_id']), int(row['process_id']), int(row['memory_consumption']))

def validate_request(): # check that resources that job can actually be finished by deadline and that we have the necessary resources + their amount availanble to do so while still meeting the deadline
    return

def main():
    #read_model()
    rs = re.ReservationStore()
    init_reservation_for_cores(rs)
    print(repr(rs))
           

if __name__ == "__main__":
    main()



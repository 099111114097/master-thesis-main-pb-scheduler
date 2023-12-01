import csv
import reservation

#def read_plan():



def machine_details_reader():
    csvfile = open("data/machine_details.csv", newline='')
    return csv.DictReader(csvfile, delimiter=";")
    

def init_reservation_for_cores(reservations):
    reader = machine_details_reader()
    for row in reader:
        
        for core_name in row['cores'].split(","):
            new_res = reservation.CoreReservation(row['node_id'], row['total_shared_memory'], core_name)
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
    re = reservation.ReservationStore()
    init_reservation_for_cores(re)
           

if __name__ == "__main__":
    main()



import csv
import reservation_static as re
import job
import request_processig as rp

def validate_request(): # check that resources that job can actually be finished by deadline and that we have the necessary resources + their amount availanble to do so while still meeting the deadline
    return

def add_job(job_id):
    j = rp.read_job(job_id)
    processes = rp.read_process(job_id)
    t = rp.read_tasks(job_id, processes)
    j.head = t
    j.info()
    return j

def main():
    j = add_job(2)
    return
    #read_model()
    rs = re.ReservationStore()
    rs.init_reservation_for_cores()
    print(repr(rs))
    #get request
    core = add_reservation(rs, None, 1, 6, 30, 1, 1, 1)           
    add_reservation(rs, None, 1, 6, 304, 1, 1, 2)
    add_reservation(rs, None, 1, 6, 30, 1, 1, 2)
    print(repr(rs)) 

if __name__ == "__main__":
    main()



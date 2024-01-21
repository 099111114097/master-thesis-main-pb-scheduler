import reservation_static as re
import request_processig as rp
import build_plan as p
import request_valdiation as validator
from exceptions import ValidationException, ReservationException, StructureException

def print_banner():
    print('8888888b.  888888b.    .d8888b.                                                                                         ')  
    print('888   Y88b 888  "88b  d88P  Y88b                                                                                        ')  
    print('888    888 888  .88P  Y88b.                                                                                             ')  
    print('888   d88P 8888888K.   "Y888b.                                                                                          ')  
    print('8888888P"  888  "Y88b     "Y88b.                                                                                        ')  
    print('888        888    888       "888                                                                                        ')  
    print('888        888   d88P Y88b  d88P                                                                                        ')  
    print('888        8888888P"   "Y8888P"                                                                                         ')  
    print('                                                                                                                        ')  
    print('.d888                                                            .d888                                                   ')  
    print('d88P"                                                            d88P"                                                  ')  
    print('888                                                              888                                                    ')  
    print('888888 .d88b.  888d888 88888b.d88b.       888  888 .d8888b       888888 .d88b.  888d888      888  888  .d88b.  888  888 ')  
    print('888   d88""88b 888P"   888 "888 "88b      888  888 88K           888   d88""88b 888P"        888  888 d88""88b 888  888 ')  
    print('888   888  888 888     888  888  888      888  888 "Y8888b.      888   888  888 888          888  888 888  888 888  888 ')  
    print('888   Y88..88P 888     888  888  888      Y88b 888      X88      888   Y88..88P 888          Y88b 888 Y88..88P Y88b 888 ')  
    print('888    "Y88P"  888     888  888  888       "Y88888  88888P"      888    "Y88P"  888           "Y88888  "Y88P"   "Y88888 ')  
    print('                                                                                                 888                   ')  
    print('                                                                                            Y8b d88P                   ')  
    print('                                                                                            "Y88P"                    ')  

def schedule_job(rs, job_id):
    try:
        j = rp.read_job(job_id)
        rp.read_process(j)
        rp.read_tasks(j)
        print("JOB VALIDATION STARTS...", end="")
        validator.validate_request(j, rs)
        print("DONE")
        print("JOB RESERVATION/SCHEDULING STARTS...", end="")
        rs.add_reservation_for_process(j.job_id, j.deadline, j.processes[0], j.start, [])
        print("DONE")
        pb = p.PlanBuilder(rs, job_id)
        print("JOB PLAN BUILDING...")
        pb.build_plan()
    except ValidationException as e:
        print(f"Validation unsuccessful: " + str(e))
    except ReservationException as e:
        print(f"Reservation/Scheduling failed: " + str(e))
        rs.cleanup_res(job_id)
    except StructureException as e:
        print(f"Structure is flawed (check provided files in data/): " + str(e))
    except Exception as e:
        print(f"Error while processing/scheduling job: " + str(e))

def main():
    rs = re.ReservationStore()
    rs.init_reservation_for_cores()
    scheduled_jobs = []
    
    print_banner()
    while True:
        i = input("Please enter the job id of the job you want to run (2 or 3)")
        if i == "q":
            print("You quite the schedule terminal. Goodbye!")
            return
        job_id = int(i)
        if job_id in scheduled_jobs:
            print(f"Job {job_id} is already scheduled!")
            continue
        schedule_job(rs, job_id)
        scheduled_jobs.append(job_id)

if __name__ == "__main__":
    main()


import reservation_static as re
import request_processig as rp
import build_plan as p
import request_valdiation as validator
from exceptions import ValidationException, ReservationException, StructureException

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
    schedule_job(rs, 2)
    schedule_job(rs, 3)

if __name__ == "__main__":
    main()



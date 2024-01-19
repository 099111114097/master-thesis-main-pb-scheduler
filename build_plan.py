import reservation_static as rs
import json
import csv

class PlanBuilder:
    def __init__(self, rs: rs.ReservationStore, job_id) -> None:
        self.rs = rs
        self.job_id = job_id

    def build_plan(self):
        res_per_node = self.rs.get_res_by_job(self.job_id)
        if not res_per_node:
            print("No reservations found for job {self.job_id}. Plan cannot be build.")
        #print(json.dumps(res_per_node))
        with open(f"plans/plan_{self.job_id}.csv", 'w', newline='') as csvfile:
            fieldnames = ['node_id', 'core_id', 'task_id', 'start', 'end', 'mem_consumption']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=";")

            writer.writeheader()
            for node_id in res_per_node:
                for core_id in res_per_node[node_id]:
                    res_in_core = res_per_node[node_id][core_id]
                    for res in res_in_core:
                        res['node_id'] = node_id
                        res['core_id'] = core_id
                        writer.writerow(res)


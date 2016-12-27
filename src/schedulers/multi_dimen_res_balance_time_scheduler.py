__author__ = 'bluesky'

from multi_dimen_double_easy_scheduler import MultiDimenDoubleEasyScheduler
from common import ResourceSnapshot, list_copy

#latest job first
latest_sort_key = (
    lambda job : -job.submit_time
)

class MultiDimensionResourceBalanceTimeScheduler(MultiDimenDoubleEasyScheduler):
    def __init__(self, num_processors, num_memory):
        super(MultiDimensionResourceBalanceTimeScheduler, self).__init__(num_processors, num_memory)

    def backfill_jobs(self, current_time):
        "overriding parent method"
        if len(self.unscheduled_jobs) <= 1:
            return []

        result = []
        first_job = self.unscheduled_jobs[0]
        tail_of_waiting_list = list_copy(self.unscheduled_jobs[1:])

        tail_by_reverse_order = sorted(tail_of_waiting_list, key=latest_sort_key)

        candidate_jobs = []

        first_job_earlist_time = self.resource_snapshot.job_earliest_assignment(first_job, current_time)
        self.resource_snapshot.assign_job(first_job, first_job_earlist_time)

        for job in tail_by_reverse_order:
            if self.resource_snapshot.can_job_start_now(job, current_time):
                if current_time + job.predicted_run_time <= first_job_earlist_time:
                    self.unscheduled_jobs.remove(job)
                    self.resource_snapshot.assign_job(job, current_time)
                    result.append(job)
                else:
                    candidate_jobs.append(job)

        #next we have to calculate the resource utilization with different job to be backfilled
        best_utilization = None
        temp_result = None


        for job in candidate_jobs:
            if self.resource_snapshot.can_job_start_now(job, current_time):
                utilization_with_job_backfilled = self.utilization_with_backfill(job, first_job_earlist_time)
                if best_utilization is None:
                    best_utilization = utilization_with_job_backfilled
                    temp_result = job
                else:
                    if utilization_with_job_backfilled < best_utilization:
                        best_utilization = utilization_with_job_backfilled
                        temp_result = job

        if temp_result != None:
            self.unscheduled_jobs.remove(temp_result)
            self.resource_snapshot.assign_job(temp_result, current_time)
            result.append(temp_result)

        self.resource_snapshot.del_job_from_res_slices(first_job)
        return result

    def utilization_with_backfill(self, job, current_time):
        resouces = self.resource_snapshot.free_resource_available_at(current_time)
        cpu_utilization = float((self.resource_snapshot.total_processors - resouces[0])) / self.resource_snapshot.total_processors + \
            job.num_required_processors / self.resource_snapshot.total_processors

        mem_utilization = float((self.resource_snapshot.total_memory - resouces[1])) / self.resource_snapshot.total_memory + \
            job.num_required_memory / self.resource_snapshot.total_memory

        average_utilization = (cpu_utilization + mem_utilization) / 2
        max_resource_util = max(cpu_utilization, mem_utilization)

        assert average_utilization != 0.0
        return (max_resource_util / average_utilization) * (1 - average_utilization)



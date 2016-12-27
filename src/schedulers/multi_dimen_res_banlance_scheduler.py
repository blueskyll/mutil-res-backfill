__author__ = 'bluesky'

from multi_dimen_double_easy_scheduler import MultiDimenDoubleEasyScheduler
from common import ResourceSnapshot, list_copy

class MultiDimensionResourceBalanceScheduler(MultiDimenDoubleEasyScheduler):
    def __init__(self, num_processors, num_memory):
        super(MultiDimensionResourceBalanceScheduler, self).__init__(num_processors, num_memory)

    def backfill_jobs(self, current_time):
        "overriding parent method"
        if len(self.unscheduled_jobs) <= 1:
            return []

        result = []
        first_job = self.unscheduled_jobs[0]
        tail_of_waiting_list = list_copy(self.unscheduled_jobs[1:])

        candidate_jobs = []
        for job in tail_of_waiting_list:
            if self.can_be_backfilled(job, current_time):
                candidate_jobs.append(job)

        #next we have to calculate the resource utilization with different job to be backfilled
        best_utilization = None
        result = []

        for job in candidate_jobs:
            utilization_with_job_backfilled = self.utilization_with_backfill(job, current_time)
            if best_utilization is None:
                best_utilization = utilization_with_job_backfilled
                result.append(job)
            else:
                if utilization_with_job_backfilled < best_utilization:
                    best_utilization = utilization_with_job_backfilled
                    result[0] = job

        if len(result) != 0:
            self.unscheduled_jobs.remove(result[0])
            self.resource_snapshot.assign_job(result[0], current_time)

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



    def can_be_backfilled(self, second_job, current_time):
        assert len(self.unscheduled_jobs) >= 2
        assert second_job in self.unscheduled_jobs[1:]

        resource = self.resource_snapshot.free_resource_available_at(current_time)
        if resource[0] < second_job.num_required_processors or resource[1] < second_job.num_required_memory:
            return False

        first_job = self.unscheduled_jobs[0]

        temp_resource_snapshot = self.resource_snapshot.copy()
        temp_resource_snapshot.assign_job_earliest(first_job, current_time)

        return temp_resource_snapshot.can_job_start_now(second_job, current_time)

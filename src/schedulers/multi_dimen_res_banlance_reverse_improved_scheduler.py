__author__ = 'bluesky'

from common import ResourceSnapshot, list_copy
from multi_dimen_res_banlance_scheduler import MultiDimensionResourceBalanceScheduler

#latest job first
latest_sort_key = (
    lambda job : -job.submit_time
)
class MultiDimensionResourceBalanceReverseImprovedScheduler(MultiDimensionResourceBalanceScheduler):

    def __init__(self, num_processors, num_memory):
        super(MultiDimensionResourceBalanceReverseImprovedScheduler, self).__init__(num_processors, num_memory)

    def backfill_jobs(self, current_time):
        "overriding parent method"
        if len(self.unscheduled_jobs) <= 1:
            return []

        result = []
        first_job = self.unscheduled_jobs[0]
        tail = list_copy(self.unscheduled_jobs[1:])
        tail_by_reverse_order = sorted(tail, key=latest_sort_key)

        self.resource_snapshot.assign_job_earliest(first_job, current_time)

        #next we have to calculate the resource utilization with different job to be backfilled
        current_util = self.current_utilization(current_time)
        result = []

        for job in tail_by_reverse_order:
            if self.resource_snapshot.can_job_start_now(job, current_time):
                utilization_with_job_backfilled = self.utilization_with_backfill(job, current_time)
                if utilization_with_job_backfilled <= current_util:
                    self.unscheduled_jobs.remove(job)
                    self.resource_snapshot.assign_job(job, current_time)
                    result.append(job)

        self.resource_snapshot.del_job_from_res_slices(first_job)
        return result

    def current_utilization(self, current_time):
        resouces = self.resource_snapshot.free_resource_available_at(current_time)
        cpu_utilization = float((self.resource_snapshot.total_processors - resouces[0])) / self.resource_snapshot.total_processors

        mem_utilization = float((self.resource_snapshot.total_memory - resouces[1])) / self.resource_snapshot.total_memory

        average_utilization = (cpu_utilization + mem_utilization) / 2
        max_resource_util = max(cpu_utilization, mem_utilization)

        assert average_utilization != 0.0

        return max_resource_util / average_utilization





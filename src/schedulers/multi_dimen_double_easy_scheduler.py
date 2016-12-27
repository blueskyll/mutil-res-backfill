__author__ = 'bluesky'

from common import list_copy, ResourceSnapshot
from base.prototype import JobStartEvent

class MultiDimenDoubleEasyScheduler(object):
    def __init__(self, num_processors, num_memory):
        self.num_processors = num_processors
        self.num_memory = num_memory
        self.resource_snapshot = ResourceSnapshot(num_processors, num_memory)
        self.unscheduled_jobs = []

    def new_events_on_job_submission(self, just_submitted_job, current_time):
        just_submitted_job.predicted_run_time = 2 * just_submitted_job.user_estimated_run_time
        self.resource_snapshot.archive_old_slices(current_time)
        self.unscheduled_jobs.append(just_submitted_job)
        return [
            JobStartEvent(current_time, job)
            for job in self.schedule_jobs(current_time)
        ]

    def new_events_on_job_termination(self, job, current_time):
        """ Here we first delete the tail of the just terminated job (in case it's
        done before user estimation time). We then try to schedule the jobs in the waiting list,
        returning a collection of new termination events (test)"""
        self.resource_snapshot.archive_old_slices(current_time)
        self.resource_snapshot.del_tail_of_job_from_res_slices(job)
        return [
            JobStartEvent(current_time, job)
            for job in self.schedule_jobs(current_time)
        ]

    def schedule_jobs(self, current_time):
        "schedule jobs that can run right now, and returns them"
        jobs = self.schedule_head_of_list(current_time)
        jobs += self.backfill_jobs(current_time)
        return jobs

    def schedule_head_of_list(self, current_time):
        result = []
        while True:
            if len(self.unscheduled_jobs) == 0:
                break
            #Try to schedule the first job
            free_resources = self.resource_snapshot.free_resource_available_at(current_time)
            if free_resources[0] >= self.unscheduled_jobs[0].num_required_processors and \
                            free_resources[1] >= self.unscheduled_jobs[0].num_required_memory:
                job = self.unscheduled_jobs.pop(0)
                self.resource_snapshot.assign_job(job, current_time)
                result.append(job)
            else:
                break
        return result

    def backfill_jobs(self, current_time):
        """
        find jobs that can be backfilled and update the resource snapshot
        """
        if len(self.unscheduled_jobs) <= 1:
            return []

        result = []

        tail_of_waiting_list = list_copy(self.unscheduled_jobs[1:])

        for job in tail_of_waiting_list:
            if self.can_be_backfilled(job, current_time):
                self.unscheduled_jobs.remove(job)
                self.resource_snapshot.assign_job(job, current_time)
                result.append(job)

        return result

    def can_be_backfilled(self, second_job, current_time):
        assert len(self.unscheduled_jobs) >= 2
        assert second_job in self.unscheduled_jobs[1:]

        free_resources = self.resource_snapshot.free_resource_available_at(current_time)
        if free_resources[0] < second_job.num_required_processors or free_resources[1] < second_job.num_required_memory:
            return False

        first_job = self.unscheduled_jobs[0]

        temp_resource_snapshot = self.resource_snapshot.copy()
        temp_resource_snapshot.assign_job_earliest(first_job, current_time)

        # if true, this means that the 2nd job is "independent" of the 1st, and thus can be backfilled
        return temp_resource_snapshot.can_job_start_now(second_job, current_time)




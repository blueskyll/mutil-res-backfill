
def list_copy(my_list):
        result = []
        for i in my_list:
            result.append(i)
        return result


def list_print(my_list):
    for i in my_list:
        print i
        print


class ResourceSnapshot(object):
    def __init__(self, total_processors, total_memory):
        self.total_processors = total_processors
        self.total_memory = total_memory
        self.slices = []
        self.slices.append \
            (ResourceTimeSlice(self.total_processors, self.total_memory, start_time = 0, duration = 1000, \
                               total_processors = total_processors, total_memory = total_memory))
        self.archive_of_old_slices = []

    @property
    def snapshot_end_time(self):
        assert len(self.slices) > 0
        return self.slices[-1].end_time


    def ensure_a_slice_starts_at(self, start_time):
        if self.slice_starts_at(start_time):
            return

        if start_time < self.snapshot_end_time:
            index = self.slice_index_to_split(start_time)

            slice = self.slices.pop(index)
            self.slices[index:index] = slice.split(start_time)
            return

        if start_time > self.snapshot_end_time:
            self.append_time_slice(self.total_processors, self.total_memory, start_time - self.snapshot_end_time)
            assert self.snapshot_end_time == start_time

        self.append_time_slice(self.total_processors, self.total_memory, 1000)


    def append_time_slice(self, free_processors, free_memory, duration):
        self.slices.append(ResourceTimeSlice(free_processors, free_memory, self.snapshot_end_time, \
                                             duration, self.total_processors, self.total_memory))


    def slice_index_to_split(self, split_time):
        assert not self.slice_starts_at(split_time)

        for index, slice in enumerate(self.slices):
            if slice.start_time < split_time < slice.end_time:
                return index

        assert False

    def slice_starts_at(self, time):
        for slice in self.slices:
            if slice.start_time == time:
                return True
        return False

    def archive_old_slices(self, current_time):
        assert self.slices
        self.unify_slices()
        self.ensure_a_slice_starts_at(current_time)

        size = len(self.slices)
        while size > 0:
            s = self.slices[0]
            if s.end_time <= current_time:
                self.archive_of_old_slices.append(s)
                self.slices.pop(0)
                size -= 1
            else:
                break

    def unify_slices(self):
        assert self.slices

        if len(self.slices) < 10:
            return

        prev = self.slices[0]
        #use a copy so we don't change the container while iterating over it
        for s in list_copy(self.slices[1: ]):
            assert s.start_time == prev.start_time + prev.duration
            if s.free_processors == prev.free_processors and s.free_memory == prev.free_memory \
                    and s.job_ids == prev.job_ids:
                prev.duration += s.duration
                self.slices.remove(s)
            else:
                prev = s

    def free_resource_available_at(self, time):
        for s in self.slices:
            if s.start_time <= time <= s.end_time:
                return [
                    s.free_processors,
                    s.free_memory
                ]
        return [
            self.total_processors,
            self.total_memory
        ]

    def assign_job(self, job, job_start):
        """
        assigns the job to start at the given job_start time
        """
        job.start_to_run_at_time = job_start
        self.ensure_a_slice_starts_at(job_start)
        self.ensure_a_slice_starts_at(job.predicted_finish_time)
        for s in self.slice_time_range(job_start, job.predicted_finish_time):
            s.add_job(job)

    def slice_time_range(self, start, end):
        assert self.slice_starts_at(start), "start time is: " + str(start)
        assert self.slice_starts_at(end), "end time is: " + str(end)

        return (s for s in self.slices if start <= s.start_time < end)

    def copy(self):
        result = ResourceSnapshot(self.total_processors, self.total_memory)
        result.slices = [slice.copy() for slice in self.slices]
        return result

    def assign_job_earliest(self, job, time):
        self.assign_job(job, self.job_earliest_assignment(job, time))

    def job_earliest_assignment(self, job, time):
        assert job.num_required_processors <= self.total_processors, str(self.total_processors)
        assert job.num_required_memory <= self.total_memory, str(self.total_memory)

        self.append_time_slice(self.total_processors, self.total_memory, time + job.predicted_run_time + 1)

        partially_assigned = False
        tentative_start_time = accumulated_duration = 0

        assert time >= 0

        for s in self.slices:# continuity assumption: if t' is the successor of t, then: t' = t + duration_of_slice_t
            if s.end_time <= time or s.free_processors < job.num_required_processors or \
                s.free_memory < job.num_required_memory:
                # the job can't be assigned to this slice, need to reset
                # partially_assigned and accumulated_duration
                partially_assigned = False
                accumulated_duration = 0

            elif not partially_assigned:
                # we'll check if the job can be assigned to this slice and perhaps to its successive
                partially_assigned = True
                tentative_start_time = max(time, s.start_time)
                accumulated_duration = s.end_time - tentative_start_time

            else:
                # job is partially_assigned
                accumulated_duration += s.duration

            if accumulated_duration >= job.predicted_run_time:
                # making sure that the last "empty" slice we've just added will not be huge
                self.slices[-1].duration = 1000
                return tentative_start_time

        assert False

    def can_job_start_now(self, job, current_time):
        return self.job_earliest_assignment(job, current_time) == current_time

    def del_tail_of_job_from_res_slices(self, job):
        """
        This function is used when the actual duration is smaller than the
        estimated duration, so the tail of the job must be deleted from the
        slices. We iterate trough the sorted slices until the critical point is found:
        the point from which the tail of the job starts.
        Assumptions: job is assigned to successive slices.
        """
        for s in self.slice_time_range(job.finish_time, job.predicted_finish_time):
            s.del_job(job)

    def del_job_from_res_slices(self, job):
        for s in self.slice_time_range(job.start_to_run_at_time, job.predicted_finish_time):
            s.del_job(job)

    def restore_old_slices(self):
        size = len(self.archive_of_old_slices)
        while size > 0:
            size -= 1
            s = self.archive_of_old_slices.pop()
            self.slices.insert(0, s)

class ResourceTimeSlice(object):
    def __init__(self, free_processors, free_memory, start_time, duration, total_processors, total_memory):
        assert duration > 0
        assert start_time >= 0
        assert total_processors > 0
        assert 0 <= free_processors <= total_processors
        assert 0 <= free_memory <= total_memory

        self.total_processors = total_processors
        self.free_processors = free_processors
        self.total_memory = total_memory
        self.free_memory = free_memory
        self.start_time = start_time
        self.duration = duration

        self.job_ids = set()

    @property
    def end_time(self):
        return self.start_time + self.duration

    @property
    def busy_processors(self):
        return self.total_processors - self.free_processors

    @property
    def busy_memory(self):
        return self.total_memory - self.free_memory

    def add_job(self, job):
        assert job.num_required_processors <= self.free_processors, job
        assert job.num_required_memory <= self.free_memory, job
        assert job.id not in self.job_ids, "job.id = " + str(job.id) + ", job_ids " + str(self.job_ids)
        self.free_processors -= job.num_required_processors
        self.free_memory -= job.num_required_memory
        self.job_ids.add(job.id)

    def del_job(self, job):
        assert job.num_required_processors <= self.busy_processors, job
        assert job.num_required_memory <= self.busy_memory, job
        self.free_memory += job.num_required_memory
        self.free_processors += job.num_required_processors
        self.job_ids.remove(job.id)

    def __str__(self):
        return '%d %d %d %d %s' % (self.start_time, self.duration, self.free_processors, self.free_memory, self.job_ids)

    def quick_copy(self):
        result = ResourceTimeSlice(
            free_processors = self.free_processors,
            free_memory = self.free_memory,
            start_time = self.start_time,
            duration = self.duration,
            total_processors = self.total_processors,
            total_memory = self.total_memory
        )

        return result

    def copy(self):
        result = ResourceTimeSlice(
            free_processors = self.free_processors,
            free_memory = self.free_memory,
            start_time = self.start_time,
            duration = self.duration,
            total_processors = self.total_processors,
            total_memory = self.total_memory
        )

        result.job_ids = self.job_ids.copy()

        return result

    def split(self, split_time):
        first = self.copy()
        first.duration = split_time - self.start_time

        second = self.copy()
        second.start_time = split_time
        second.duration = self.end_time - split_time

        return first, second


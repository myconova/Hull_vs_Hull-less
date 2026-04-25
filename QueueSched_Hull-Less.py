from jsonschema import validate, ValidationError
from collections import deque

# Job Fields
class JobFields:
    ID = "id"
    SUBMISSION_TIME = "submission_time"
    DURATION = "duration"

# Job Schema
job_schema = {
    "type": "object",
    "properties": {
        JobFields.ID: {"type": "string"},
        JobFields.SUBMISSION_TIME: {"type": "integer", "minimum": 0},
        JobFields.DURATION: {"type": "integer"}
    },
    "required": [JobFields.ID, JobFields.SUBMISSION_TIME, JobFields.DURATION],
    "additionalProperties": False
}

job_input = [
    {"id": "job_a", "submission_time": 0, "duration": -1},
    {"id": "job_b", "submission_time": 1, "duration": 2},
    {"id": "job_c", "submission_time": 2, "duration": 1}
]

def validate_jobs(job_input, job_schema):
    if not isinstance(job_input, list):
        return False, "Job input must be a list of job dictionaries."

    for i, job in enumerate(job_input):
        if not isinstance(job, dict):
            return False, f"Job at index {i} is not a dictionary."

        try:
            validate(instance=job, schema=job_schema)
        except ValidationError as e:
            return False, f"Job at index {i} is invalid: {e.message}"

    # duplicate ID check
    seen_ids = set()
    for i, job in enumerate(job_input):
        job_id = job[JobFields.ID]
        if job_id in seen_ids:
            return False, f"Duplicate job ID found: {job_id} at index {i}"
        seen_ids.add(job_id)

    return True, f"All jobs are valid."

# FIFO scheduling  

def run_fifo(job_input):
    is_valid, validation_message = validate_jobs(job_input, job_schema)
    if not is_valid:
        return False, validation_message, []
    
    sorted_jobs = sorted(job_input, key=lambda x: x['submission_time'])

    clock = 0
    max_ticks = 100 
    next_job_index = 0
    job_queue = deque()
    current_job = None
    remaining_time = 0
    completed_jobs = []

    while len(completed_jobs) < len(sorted_jobs) and clock < max_ticks:

        while next_job_index < len(sorted_jobs) and sorted_jobs[next_job_index]['submission_time'] <= clock:
            job_queue.append(sorted_jobs[next_job_index])
            next_job_index += 1

        if current_job is None and len(job_queue) > 0:
            current_job = job_queue.popleft()
            remaining_time = current_job['duration']
            current_job['start_time'] = clock
            current_job['wait_time'] = clock - current_job['submission_time']

        if current_job is not None:
            remaining_time -= 1
            if remaining_time == 0:
                completion_time = clock + 1
                current_job['completion_time'] = completion_time
                current_job['turnaround_time'] = completion_time - current_job['submission_time']   
                completed_jobs.append(current_job)
                current_job = None

            clock += 1

        else:
            if next_job_index < len(sorted_jobs):
                clock = sorted_jobs[next_job_index]['submission_time']
            else:
                break
    
    if len(completed_jobs) == len(sorted_jobs):
        return True, f"FIFO run complete", completed_jobs
    else:
        if current_job is not None:
            return False, f"FIFO run incomplete: {current_job[JobFields.ID]} reached max ticks ({max_ticks})", completed_jobs
        else:
            return False, f"FIFO run incomplete: reached max ticks ({max_ticks})", completed_jobs

is_valid, validation_message, completed_jobs = run_fifo(job_input)
print(is_valid, validation_message)
for job in completed_jobs:
    print(job)
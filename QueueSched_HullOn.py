from jsonschema import validate, ValidationError

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
        JobFields.DURATION: {"type": "integer", "minimum": 1}
    },
    "required": [JobFields.ID, JobFields.SUBMISSION_TIME, JobFields.DURATION],
    "additionalProperties": False
}

job_input = []

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





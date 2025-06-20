import importlib
import argparse

def job_launcher():
    parser = argparse.ArgumentParser(description="Running a specific job.")
    parser.add_argument(
        'job',
        help='The job script to run within jobs'
    )
    args = parser.parse_args()

    # lets call the job class from here
    try:
        module = importlib.import_module(f'jobs.{args.job}')
        job_class = getattr(module, "Job")
        job = job_class()
        job.run()
    except ModuleNotFoundError as e:
        print("No job found", e)
    except AttributeError as e:
        print("Job is not defined.")
    except Exception as e:
        print(f'Unexpected Error: {e}')

if __name__ == "__main__":
    job_launcher()
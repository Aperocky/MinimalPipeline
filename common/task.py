# Task class that all task must inherit from

class Task:

    # inherit this function for running job
    def run(input_paths, output_path, config):
        raise NotImplementedError("Implement this task in sub job")



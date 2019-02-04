# Task class that all task must inherit from

class Task:

    # inherit this function for running job
    # in order to run a job in an isolated environment, both input_paths and output_path
    # are temporary directories created by the launcher which launches the task.
    # launcher are then responsible of putting them back into corresponding folders.
    def run(self, input_paths, output_path, config):
        """
        input_paths dict[key: temp_paths]: key: string of repository name, temp_path: a directory containing relevant blocks.
        output_path string: An empty temp directory where output should go.
        config: configuration for execution of this task (to be read by task code)
        """
        raise NotImplementedError("Implement this task in sub job")



from jor import JobsBase


class Jobs(JobsBase):

    _jobs = [{'p': 1}, {'p': 2}]
    output_suffix = 'pickle'

    def execute(self, i):
        print(i)

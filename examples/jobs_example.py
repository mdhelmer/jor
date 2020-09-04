import pathlib
import jor


class Jobs(jor.JobsBase):

    # slurm options
    name = 'job'
    time = '0-23:59:59'
    mem = '5G'
    cpus_per_task = 1

    def __init__(self, n=3, path_prefix='.'):

        # init base class
        super().__init__(path_prefix=path_prefix)

        # store job-specific keyword arguments
        self.n = int(n)
        
        # assemble a list of jobs
        self._mk_jobs()

    def _mk_jobs(self):
        """Generates the attribute ``self._jobs``

        ``self._jobs`` is a list of dictionaries, containing one dictionary
        for each jobs to be run. Each of these dictionaries specifies the 
        parameters for each individual job.
        """
        self._jobs = [
            dict(index=i)
            for i in range(self.n)
        ]

    def _get_output_folder(self):
        """Return output folder for jobs

        Output folder is the ``path_prefix``, followed by the name of a subfolder
        encoding the arguments given in the constructor.
        """
        output_folder = pathlib.Path(self.path_prefix) / f'example{self.n}'
        return str(output_folder)

    def _get_output_fname(self, index):
        """Return output file name for a given job

        The particular job is specified by the arguments to this function,
        which match the keys of the dictionaries in ``self._jobs`` (cf.
        :func:`self._mk_jobs`).
        """
        outfname = f'ind{index}.txt'
        return outfname

    def execute(self, i):
        """This function performs the actual work

        Parameters
        ----------
        i : int between 0 and number of jobs (``len(self._jobs)``)
            indicating the index in ``self._jobs`` from which to take
            the dictionary with this job's parameter values
        """
        myargs = self._jobs[i]
        output_path = self._get_output_path(**myargs)

        # do the work and write outcomes to file ``output_path``
        with open(output_path, 'wt') as f:
            f.write(str(myargs) + '\n')

    def collect(self):
        """Concatenates all outputs into a common output

        This function is optional and can be implemented if desired for 
        the particular job. It is called by running ``jor collect`` on the 
        command line.
        """ 
        pass

Usage:

This program only can handle the map task failure now. 
If the worker fails during the reduce phase, it will cause problem.

For local execution:

In terminal 1: start the master

$ python mr_master.py <port>
$ python mr_master.py master 4242

In terminal 2: register the worker

$ python mr_worker.py <port>
$ python mr_worker.py worker 4243

In terminal 3: register the worker

$ python mr_worker.py <port>
$ python mr_worker.py worker 4244

In terminal 4: register the worker

$ python mr_worker.py <port>
$ python mr_worker.py worker 4245

In terminal 5: set the job with mr_job.py, collect all reduced results with mr_collect.py 
				& get the result file of the sequential execution with mr_seq.py.

Note: 	Based on the size of input file, the split size should be a reasonable multiple of the size of the input file(1/10).
		Otherwise, the performance of whole MapReduce job will be slow if the split size is too small or too big.
		e.g.: the input file test.txt is 718000 bytes, the split size should be 100000 bytes

$ python mr_job.py <job_name>  <split_size> <num_reducers> <input_filename> <output_filename_base>
$ python mr_job.py wordcount 100000 3 test.txt count

$ python mr_collect.py <filename_base> <output_filename>
$ python mr_collect.py count count_all

$ python mr_seq.py <job_name> <input_filename> <output_filename_base>
$ python mr_seq.py wordcount test.txt sequential_file


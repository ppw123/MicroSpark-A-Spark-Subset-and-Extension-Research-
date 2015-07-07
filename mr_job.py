__author__ = 'gordonye'
import zerorpc
import sys

master_addr = 'tcp://0.0.0.0:4242'

if __name__ == '__main__':


    job_name = sys.argv[1]
    split_size = int(sys.argv[2])
    num_reducers = int(sys.argv[3])
    input_filename = sys.argv[4]
    output_filename_base = sys.argv[5]

    c = zerorpc.Client(timeout=3000)
    c.connect(master_addr)

    c.setjob(job_name, split_size, num_reducers, input_filename, output_filename_base)



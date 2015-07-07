import sys
import string
import collections
import re
class Handler(object):

    def __init__(self):
        self.dic = {}

    def set_job(self, job_name, input_filename, output_filename):
        """
            Setting the job configuration and start do sequential job.
        """
        if job_name == 'wordcount':
            self.wordcount(input_filename, output_filename)

        elif job_name == 'hamming':
            pass

    def wordcount(self, input_filename, output_filename):
        """
            Word count and write the result into output file
        """
        with open(input_filename, 'r') as f_in:
            temp = f_in.read()

        f_in.close()

        temp = re.sub('[^\w]',' ',temp)
        words = temp.strip().split()

        for w in words:

            if w in self.dic:
                self.dic[w] += 1
            else:
                self.dic[w] = 1

        od = collections.OrderedDict(sorted(self.dic.items()))

        out_str = output_filename + '.txt'
        with open(out_str, 'w') as f_out:
            for k, v in od.items():
                output_line = '{} : {}\n'.format(k, v)
                f_out.write(output_line)

        f_out.close()

if __name__ == '__main__':

    job_name = sys.argv[1]
    input_filename = sys.argv[2]
    output_filename = sys.argv[3]
    handler = Handler()
    handler.set_job(job_name, input_filename, output_filename)

import zerorpc
import sys
import collections
master_addr = 'tcp://0.0.0.0:4242'

if __name__ == '__main__':
    filename_base = sys.argv[1]
    output_filename = sys.argv[2]

    c = zerorpc.Client()
    c.connect(master_addr)
    result = c.collect_result(filename_base)

    od = collections.OrderedDict(sorted(result.items()))

    output_file = output_filename + '.txt'



    with open(output_file, 'w') as f_out:
        for k, v in od.items():
            f_out.write('{} : {}\n'.format(k, v))

    f_out.close()




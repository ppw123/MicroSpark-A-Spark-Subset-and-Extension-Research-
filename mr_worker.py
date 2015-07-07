import zerorpc
import ast
import os
import re
import string
import sys
import gevent
from itertools import islice
import math
import json
import collections


master_addr = 'tcp://0.0.0.0:4242'

class Worker(object):


    def __init__ (self):
        self.dic = {}
        self.reduce_task = 0
        gevent.spawn(self.controller)

    def controller(self):
        """
            Check the worker is alive or not.
        """
        while True:
            print "[Worker]"
            gevent.sleep(1)

    def ping(self):
        """
           Get ping from master
        """
        print('[Worker] Ping from Master')

    def readFile(self, fileName, startByte, endByte):
        """
           Read the input file.
        """
        input = open(fileName, 'r').read()[startByte:endByte]

        return input


    def word_count_map(self, file_name, start, end):
        """
           Word count
            :return word frequency dictionary {word: frequency}
        """
        temp = self.readFile(file_name, start, end)
        temp = re.sub('[^\w]',' ',temp)
        words = temp.strip().split()

        for w in words:

            if w in self.dic:
                self.dic[w] += 1
            else:
                self.dic[w] = 1

        self.dic = collections.OrderedDict(sorted(self.dic.items()))
        print self.dic
        return self.dic


    def chunks(self, data, size):
        """
           Get the chunk from intermediate file. For word count, it gets partial dictionary
        """
        it = iter(data)
        for i in xrange(0, len(data), size):
            yield {k:data[k] for k in islice(it, size)}


    def getMapDic(self, r, taskNum):
        """
            Retrieve the chunk of the dictionary from local memory
        """
        size = int(math.ceil(float(len(self.dic)/r)))
        ret = []
        for i in self.chunks(self.dic, size):
            ret.append(i)

        return ret[taskNum]


    # str will be a string which consisted of c who have the mapper result
    def reduce(self, R, taskNum, c_List, output_file_base):
        """
            Go to each intermediate files to collect certain part of the intermediate files and put them into one file
        """
        super_dict = {}
        self.reduce_task = taskNum
        dic_List = []

        for c in c_List:

            ip = c[0]
            port = c[1]
            c1 = zerorpc.Client()
            c1.connect("tcp://" + ip + ':' + port)

            temp = c1.getMapDic(R, taskNum)
            dic_List.append(temp)


        for d in dic_List:
            d = collections.OrderedDict(sorted(d.items()))

            for k, v in d.iteritems():
                super_dict[k] = super_dict.setdefault(k, 0) + v


        output_file_name = output_file_base + '_' + str(taskNum) + '.txt'
        with open(output_file_name, 'w') as f_out:
            json.dump(super_dict, f_out)

        f_out.close()
        return super_dict

    def collect(self, filename_base):
        """
            Read intermediate file back and return the dictionary format data.
        """
        collect_dic = {}

        whip = {}

        filename = filename_base + '_' + str(self.reduce_task) + '.txt'

        with open(filename, 'r') as f:

            whip = json.load(f)

        for k, v in whip.items():

            if k not in collect_dic.keys():
                collect_dic[k] = v
            else:
                collect_dic[k] += v

        return collect_dic


if __name__ == '__main__':

    s = zerorpc.Server(Worker())
    ip = '0.0.0.0'
    port = sys.argv[1]
    s.bind('tcp://' + ip + ':' + port)
    c = zerorpc.Client()
    c.connect(master_addr)
    c.register(ip, port)
    s.run()





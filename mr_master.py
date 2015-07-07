__author__ = 'gordonye'

import zerorpc
import gevent
import sys
import os
import collections
import string
import math
import logging
import random
import ast
from gevent import Timeout

class StatusEnum(object):
    STATUS_READY = 'READY'
    STATUS_WORKING = 'WORKING'
    STATUS_DOWN = 'DOWN'

class Master(object):

    def __init__(self):
        gevent.spawn(self.controller)
        self.state = StatusEnum.STATUS_READY
        self.workers = {} # [(ip, port)] = (status, c)
        self.working_on_map = False
        self.working_on_reduce = False

        # track work load status
        self.map_work_loads = {} # func_name: (args, is_done)
        # track gevent proc for each worker
        self.map_worker_procs = {} # gevent proc: key in map_worker
        # track assigned task of each worker
        self.map_work_assignment = {} # key in worker: key in map_work_loads
        # track done map tasks of each worker
        self.map_worker_done_assignments = collections.defaultdict(list)

        self.reduce_work_loads = {} # func_name : (task_num, is_done)
        # track gevent proc for each worker
        self.reduce_worker_procs = {} # gevent proc: key in reduce_worker
        # track assigned task of each worker
        self.reduce_work_assignment = {} # key in worker: key in reduce_work_loads
        # track done reduce tasks of each worker
        self.reduce_work_done_assignments = collections.defaultdict(list)

    def _update_worker_status(self, w_key, status):
        """
           Update worker's status
        """

        old_status, c = self.workers[w_key]
        #=======================================================================
        # if it is DOWN, its gone forever
        #=======================================================================
        if old_status == StatusEnum.STATUS_DOWN:
            return
        self.workers[w_key] = (status, c)
        print '[Worker: %s,%s] status change from <%s> to <%s>' % (w_key[0],
                                                                   w_key[1],
                                                                   old_status,
                                                                   status
                                                                   )

    def _find_next_available_worker(self):
        """
           Find next available worker whose status is Ready
            :return worker key (ip, port)
        """
        for k, v in self.workers.items():
            if v[0] == StatusEnum.STATUS_READY:
                return k
        return ''

    def controller(self):
        """
           Master controller keep pinging all workers and deal with worker failures
        """
        while True:
            print '-----Master:%s-------\n' % self.state,

            for w in self.workers.keys():

                try:
                        self.workers[w][1].ping()

                except Exception, e:
                    #====================
                    #   node down
                    #====================
                    print "<========== worker down, reset all map work done by it to original state=======>\n"
                    self._update_worker_status(w, StatusEnum.STATUS_DOWN)

                    # if map worker down, reset all map works done by this worker to original state
                    for v in self.map_worker_done_assignments[w]:

                        for key, partition in self.map_work_loads.items():

                            if partition[0] == v:

                                work_load_key = key

                        self.map_work_loads[work_load_key] = (self.map_work_loads[work_load_key][0], False)

                finally:
                    print '(%s,%s,%s)' % (w[0], w[1], self.workers[w][0])
                    # time.cancel()
                    # remove down worker from worker queue
            gevent.sleep(1)

    def register_async(self, ip, port):
        """
           Register new worker asynchronously
        """
        print '-----Master:%s------- ' % self.state,
        print 'Registered worker (%s,%s)' % (ip, port)
        c = zerorpc.Client()
        c.connect("tcp://" + ip + ':' + port)
        self.workers[(ip, port)] = (StatusEnum.STATUS_READY, c)
        c.ping()

    def register(self, ip, port):
        """
           Register new worker
        """
        gevent.spawn(self.register_async, ip, port)

    def word_count_file_split(self, input_filename, split_size):
        """
            word count input file split by user defined size
             :return partitions {partition id: (startByteOffset, endByteOffset)}
        """

        filesize = os.stat(input_filename).st_size
        in_file = open(input_filename)

        positions = []
        partitions = {}

        while in_file.tell() != filesize:

            if in_file.tell() + split_size >= filesize:
                positions.append(filesize)
                break

            text = in_file.read(split_size)

            if text[-1] == " ":
                positions.append(in_file.tell())

            else:
                while True:
                    char = in_file.read(1)

                    if char == " " or char == "\n":

                        positions.append(in_file.tell())
                        break

        partitions[0] = (0, positions[0])
        for i in range(1, len(positions)-1):
            partitions[i] = (positions[i-1], positions[i])
        partitions[len(positions)-1] = (positions[-2], positions[-1])
        print partitions
        return partitions

    def bi_hamming_file_split(self, input_filename, split_size):
        """
            binary hamming input file split by user defined size
        """
        partitions = {}
        return partitions

    def collect_result(self, filename_base):
        """
           Collect distributed results
           :return collected result from reducers
        """
        collected_result = {}

        for worker_key in self.reduce_work_done_assignments.keys():

            result = self.workers[worker_key][1].collect(filename_base)
            print '-------------collected result---------------------'
            print result
            for k, v in result.items():
                if k not in collected_result.keys():
                    collected_result[k] = v
                else:
                    collected_result[k] += v

        return collected_result

    def setjob(self, job_name, split_size, num_reducers, input_filename, output_filename_base):
        """
           Setting all job configurations and start MapReduce job.
        """
        if job_name == 'wordcount':

            partitions = self.word_count_file_split(input_filename, split_size)

        for i in xrange(len(partitions)):

            self.map_work_loads['do_work_%s'%i] = (partitions[i], False)

        # elif job_name == 'bi_hamming':
        #     partitions = self.bi_hamming_file_split(input_filename, split_size)
        #
        # else:
        #     sys.exit("This program only supports wordcount or binary hamming code operations")
        gevent.spawn(self.do_map, input_filename, num_reducers, output_filename_base)
        # self.do_reduce(num_reducers,  output_filename_base)

    def do_map(self, input_filename, num_reducers, output_filename_base):
        """
           Map task
        """

        #=======================================================================
        # do map jobs until all map jobs done
        #=======================================================================
        self.working_on_map = True

        while self.working_on_map:
            # print '='*100

            if not self.map_work_loads:
                print 'No map work assignment, I am done!'
                return ''

            else:

                if all([v[1] for v in self.map_work_loads.values()]):
                    print 'I finished all map works'
                    self.working_on_map = False
                    self.do_reduce(num_reducers, output_filename_base)

                else:

                    for k, v in self.map_work_loads.items():

                        if not v[1]:

                            worker_key = self._find_next_available_worker()

                            if not worker_key:
                                print 'All workers are busy, waiting for available workers'
                                gevent.sleep(1)
                            else:
                                print '[Worker %s %s] get map assignment %s %s' % (worker_key[0], worker_key[1],
                                                                                   k, v[0])

                                self._update_worker_status(worker_key, StatusEnum.STATUS_WORKING)
                                proc = gevent.spawn(self.workers[worker_key][1].word_count_map, input_filename, v[0][0], v[0][1])
                                self.map_worker_procs[proc] = worker_key
                                self.map_work_assignment[worker_key] = k

                    #start working
                    procs = self.map_worker_procs.keys()

                    if procs:

                        gevent.joinall(procs)

                        for p in procs:
                            worker_key = self.map_worker_procs[p]
                            worker_load_key = self.map_work_assignment[worker_key]

                            if not p.exception:

                                self.map_work_loads[worker_load_key] = (self.map_work_loads[worker_load_key][0], True)
                                self._update_worker_status(worker_key, StatusEnum.STATUS_READY)
                                self.map_worker_procs.pop(p)
                                self.map_worker_done_assignments[worker_key].append(self.map_work_loads[worker_load_key][0])

                                print '[Worker %s %s] finish map job %s' % (worker_key[0], worker_key[1], worker_load_key)
                            else:
                                print '[Worker %s %s] map job %s encounter an Exception' % (worker_key[0], worker_key[1],
                                                                                        worker_load_key)

    def do_reduce(self, num_reducers, output_filename_base):
        """
           Reduce task
        """

        self.working_on_reduce = True

        task_num = random.sample(xrange(num_reducers), num_reducers)

        for task in task_num:
            self.reduce_work_loads['reduce_work_%s' %task] = (task, False)

        worker_clients = [k for k in self.workers.keys() if self.workers[k][0] == StatusEnum.STATUS_READY]

        #=======================================================================
        # do reduce jobs until all reduce jobs done
        #=======================================================================

        while self.working_on_reduce:

            if not self.reduce_work_loads:
                print 'No reduce work assignment, I am done!'
                return ''

            else:

                if all([v[1] for v in self.reduce_work_loads.values()]):
                    print 'I finished all reduce works'
                    self.working_on_reduce = False

                else:

                    for k, v in self.reduce_work_loads.items():

                        if not v[1]:

                            worker_key = self._find_next_available_worker()

                            if not worker_key:
                                print 'All workers are busy, waiting for available workers'
                                gevent.sleep(1)

                            else:
                                print '[Worker %s %s] get reduce assignment %s %s' % (worker_key[0], worker_key[1], k, v[0])
                                self._update_worker_status(worker_key, StatusEnum.STATUS_WORKING)
                                proc = gevent.spawn(self.workers[worker_key][1].reduce, num_reducers, v[0], worker_clients, output_filename_base)
                                self.reduce_worker_procs[proc] = worker_key
                                self.reduce_work_assignment[worker_key] = k

                    procs = self.reduce_worker_procs.keys()

                    if procs:

                        gevent.joinall(procs)

                        for p in procs:
                            # print '*'*50
                            # print '\n reduce result: '
                            # print p.value

                            worker_key = self.reduce_worker_procs[p]
                            worker_load_key = self.reduce_work_assignment[worker_key]


                            if not p.exception:

                                self.reduce_work_loads[worker_load_key] = (self.reduce_work_loads[worker_load_key][0], True)
                                self._update_worker_status(worker_key, StatusEnum.STATUS_READY)
                                self.reduce_worker_procs.pop(p)
                                self.reduce_work_done_assignments[worker_key].append(worker_load_key)

                                print '[Worker %s %s] finish reduce job %s' % (worker_key[0], worker_key[1], worker_load_key)

                            else:
                                print '[Worker %s %s] reduce job %s encounter an Exception' % (worker_key[0], worker_key[1],
                                                                                        worker_load_key)


if __name__ == '__main__':
    logging.basicConfig()
    port = sys.argv[1]
    s = zerorpc.Server(Master())
    s.bind("tcp://0.0.0.0:" + port)
    s.run()









#! /usr/bin/env python2.4

import sys
if __debug__:
    import warnings
    #warnings.warn("Running in debug mode, this will be slow... try 'python2.4 -O %s'" % sys.argv[0])

from base.workload_parser import parse_lines
from base.prototype import _job_inputs_to_jobs
from schedulers.simulator import run_simulator
import optparse
from schedulers.multi_dimen_double_easy_scheduler import MultiDimenDoubleEasyScheduler
from schedulers.multi_dimen_res_banlance_scheduler import MultiDimensionResourceBalanceScheduler
from schedulers.multi_dimen_res_banlance_reverse_improved_scheduler import MultiDimensionResourceBalanceReverseImprovedScheduler
from schedulers.multi_dimen_res_balance_time_scheduler import MultiDimensionResourceBalanceTimeScheduler
def parse_options():
    parser = optparse.OptionParser()
    parser.add_option("--num-processors", type="int", \
                      help="the number of available processors in the simulated parallel machine")
    parser.add_option("--num-memory", type="int", \
                      help="the number of available memory in the simulated parallel machine")
    parser.add_option("--input-file", \
                      help="a file in the standard workload format: http://www.cs.huji.ac.il/labs/parallel/workload/swf.html, if '-' read from stdin")
    parser.add_option("--scheduler", 
                      help="1) MultiDimenReverseDoubleScheduler")
    
    options, args = parser.parse_args()

    if options.num_processors is None:
        parser.error("missing num processors")

    if options.num_memory is None:
        parser.error("missing num memory")

    if options.input_file is None:
        parser.error("missing input file")

    if options.scheduler is None:
         parser.error("missing scheduler")

    if args:
        parser.error("unknown extra arguments: %s" % args)

    return options

def main():
    options = parse_options()

    if options.input_file == "-":
        input_file = sys.stdin
    else:
        input_file = open(options.input_file)

    if options.scheduler == "MultiDimenDoubleEasyScheduler" or options.scheduler == "1":
        scheduler = MultiDimenDoubleEasyScheduler(options.num_processors, options.num_memory)

    elif options.scheduler == "MultiDimensionResourceBalanceScheduler" or options.scheduler == "2":
        scheduler = MultiDimensionResourceBalanceScheduler(options.num_processors, options.num_memory)

    elif options.scheduler == "MultiDimensionResourceBalanceReverseImprovedScheduler" or options.scheduler == "3":
        scheduler = MultiDimensionResourceBalanceReverseImprovedScheduler(options.num_processors, options.num_memory)

    elif options.scheduler == "MultiDimensionResourceBalanceTimeScheduler" or options.scheduler == "4":
        scheduler = MultiDimensionResourceBalanceTimeScheduler(options.num_processors, options.num_memory)
    else:
        print "No such scheduler"
        return 

    try:
        print "...." 
        run_simulator(
                num_processors = options.num_processors,
                num_memory = options.num_memory,
                jobs = _job_inputs_to_jobs(parse_lines(input_file), options.num_processors, options.num_memory),
                scheduler = scheduler 
            )
        
        print "Num of Processors: ", options.num_processors
        print "Input file: ", options.input_file
        print "Scheduler:", type(scheduler)

    finally:
        if input_file is not sys.stdin:
            input_file.close()

"""
if __name__ == "__main__" and not "time" in sys.modules:
"""
if __name__ == "__main__":
    """
    try:
        import psyco
        psyco.full()
    except ImportError:
        print "Psyco not available, will run slower (http://psyco.sourceforge.net)"
    """
    main()

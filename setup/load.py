import argparse
import logging
import os
import os.path
import pickle
import random
import socket
import struct
import sys
import time
import traceback


pre_random = {
    "int_100" : [
        random.randint(0, 100)
        for i in xrange(1000)
    ],
}

def gen_random(random_set):
    """Yields infinite list of semi-random numbers from the named set above.
    
    Each generator shuffles the values.

    """
    data = list(pre_random[random_set])
    random.shuffle(data)
    i = 0
    ceil = len(data) - 1
    while True:
        yield data[i]
        i = 0 if i == ceil else i + 1 
        
def gen_cycle(min, max):
    """Yields an infinite list of integers that cycle between min and max 
    (inclusive).
    
    """
    c = min
    delta = 1
    while True:
        yield c
        if c == max:
            delta = -1
        elif c == min:
            delta = 1
        c += delta
         
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# 
def arg_parser():
    """Builds a :class:`argparse.ArgumentParser` 
    
    """
    
    # This is the main parser that the script entry point uses.
    main_parser = argparse.ArgumentParser(
        description="tlpterm: The Last Pickle Terminal", 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    
    # Global Configuration
    
    main_parser.add_argument("--carbon-host", dest="carbon_host", 
        default="127.0.0.1", type=str,
        help="Address of the carbon host to write to.")
    
    main_parser.add_argument("--hosts-per-thread", dest="hosts_per_thread", 
        default=100, type=int,
        help="Number of hosts each thread will generate")
        
    # main_parser.add_argument("--num-threads", dest="num_threads", 
    #     default=1, type=int,
    #     help="Number of insert threads")

    main_parser.add_argument("--num-ticks", dest="num_ticks", 
        default=1000, type=int,
        help="Number of times to send the full metric set for each host")

    main_parser.add_argument("--time-step", dest="time_step", 
        default=60, type=int,
        help="Number of seconds to advance each data point")
        
    main_parser.add_argument("--batch-size", dest="batch_size", 
        default=10, type=int,
        help="Size to send to carbon")

    main_parser.add_argument("--num-metrics", dest="num_metrics", 
        default=10, type=int,
        help="Number of metrics per host")

    main_parser.add_argument("--log-level", default="DEBUG", 
        dest="log_level", 
        choices=["FATAL", "CRITICAL", "ERROR", "WARN", "INFO", "DEBUG"],
        help="Logging level.")
    main_parser.add_argument("--log-file", 
        default=os.path.expanduser("./load.log"), 
        dest="log_file", 
        help="Logging file.")

    return main_parser

class CarbonClient(object):
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.conn = None
        
    def __enter__(self):
        self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.conn.connect((self.host, self.port))
        return self
        
    def __exit__(self, type, value, traceback):
        
        try:
            self.conn.close()
        except (Exception) as e:
            print "Error closing ", e
        return
        
    def send(self, points):
        payload = pickle.dumps(points)
        header = struct.pack("!L", len(payload))
        message = header + payload
        self.conn.send(message)
        return
    

def gen_metrics(num_hosts, num_metrics, num_ticks, timestep):
    """Yield data to send to carbon
    
    generates ("metric", (time, value))
    
    """
    
    gen_values = gen_cycle(0, 100) #gen_random("int_100")
    timestamp = int(time.time()) - (num_ticks * timestep)
    
    for tick in xrange(num_ticks):
        print "Starting tick", tick
        value = gen_values.next()
        for host in xrange(num_hosts):
            host_name = "com.acquia.host-%s" % (host,)
            
            for metric in xrange(num_metrics):
                metric_name = "%s.metric-%s" % (host_name, metric,)
                point = (metric_name, (timestamp, value))
                #print "Tick", tick, point
                yield point
            # time.sleep(.1)
            # next metric
        # next host
        timestamp += timestep
    # next tick
    
def main():
    """Script entry point for the command line tool
    """
    args = arg_parser().parse_args()

    logging.basicConfig(filename=os.path.abspath(args.log_file), 
        level=getattr(logging, args.log_level))
    log = logging.getLogger(__name__)
    log.debug("Got command args %(args)s" % vars())
    
    gen = gen_metrics(args.hosts_per_thread, args.num_metrics, args.num_ticks, 
        args.time_step)
    
    with CarbonClient(args.carbon_host, 2004) as client:
        data = []
        for row in gen:
            data.append(row)
            if len(data) == args.batch_size:
                #print "SEND", data
                client.send(data)
                del data[:]
        client.send(data)
    sys.exit(0)

if __name__ == "__main__":
    main()
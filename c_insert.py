from multiprocessing import Process, Queue
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import cassandra
import sys
import random
import string
import getopt
import time
import datetime
import collections

def auth_provider(host):
    return {"username" : "cassandra", "password" : "cassandra"}

def connect(seeds, keyspace, datacenter=None, port=9042):

    from cassandra.io.libevreactor import LibevConnection
    from cassandra.cluster import Cluster
    from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy, ExponentialReconnectionPolicy

    class CustomRetryPolicy(RetryPolicy):

        def on_write_timeout(self, query, consistency, write_type,
                     required_responses, received_responses, retry_num):

            # retry at most 5 times regardless of query type
            if retry_num >= 5:
                return (self.RETHROW, None)

            return (self.RETRY, consistency)


    load_balancing_policy = None
    if datacenter:
        # If you are using multiple datacenters it's important to use
        # the DCAwareRoundRobinPolicy. If not then the client will
        # make cross DC connections. This defaults to round robin
        # which means round robin across all nodes irrespective of
        # data center.
        load_balancing_policy = DCAwareRoundRobinPolicy(local_dc=datacenter)

    cluster = Cluster(contact_points=seeds,
              port=port,
              auth_provider=auth_provider,
              default_retry_policy=CustomRetryPolicy(),
              reconnection_policy=ExponentialReconnectionPolicy(1, 60),
              load_balancing_policy=load_balancing_policy)

    cluster.connection_class = LibevConnection
    cluster.set_core_connections_per_host(0, 1)
    cluster.set_core_connections_per_host(1, 0)
    cluster.control_connection_timeout = 10.0
    cluster.set_max_connections_per_host(2, 1)
    cluster.compression = False

    session = cluster.connect(keyspace)
    return session

def worker(threadnum, queue):
    # get connection
    #connection = connect(['sjm-ats-cas1', 'sjm-ats-cas2', 'sjm-ats-cas3'], keyspace='mfgprod', datacenter='DC1')
    connection = connect(['node0','node1','node2'], keyspace='test', datacenter='us-west')
    cqlstmt = connection.prepare("INSERT INTO tst (sernum, area, rectime) VALUES (?, ?, ?)")

    inserts = 0
    total_insert_time = 0.0
    while (inserts < ninserts):
        # make a unique and incremental serial number across threads
        sernum = 'SN%05X%07X' %(threadnum, inserts)
        # make 2 to 9 inserts for this sernum
        ni = random.randint(2,9)
        for i in xrange(ni):
             cqlstmt = " INSERT INTO tst (sernum, area, rectime) VALUES ('%s', '%s', '%s') ; " %(sernum, str(i), datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
             stmt = stmt + cqlstmt
        stmt = stmt + " APPLY BATCH"     
        start_ins_time = datetime.datetime.now()
        print stmt
        try:
           connection.execute(stmt)
        except:
           e = sys.exc_info()
           print e

        stop_ins_time = datetime.datetime.now()
        insert_time = (stop_ins_time - start_ins_time).total_seconds()
        total_insert_time += insert_time
        inserts += 1
        if (inserts >= ninserts): break
    print 'Thread %d, performed %d inserts in %f secs' %(threadnum, ninserts, total_insert_time)
    connection.shutdown()
    # save all the thread specific data
    queue.put([total_insert_time])

def main():
    global nclients, ninserts
    print "Starting....\n"
    start = datetime.datetime.now()
    threads = []
    thdata = Queue()

    for x in xrange(nclients):
        t = Process(target=worker, args=(x, thdata))
        threads.append(t)
        t.start()
        time.sleep(0.1)

    for thread in threads:
        thread.join() 
    end = datetime.datetime.now()
    # gather statistics
    total_insert_time = 0.0
    while not thdata.empty():
        [insert_time] = thdata.get()
        total_insert_time += insert_time
    # because all threads running concurrently
    total_insert_time = total_insert_time / nclients
    print
    print 'Average insert time per record (using %d threads %d inserts) is %f millisecs' %(nclients, ninserts, (total_insert_time * 1000)/(nclients * ninserts))
    print
    print 'No of inserts per second %d' %(nclients * ninserts / total_insert_time)
    print
    print 'Whole test ran for %f minutes' %((end - start).total_seconds()/60)
    print 
    print "Finished!!\n"
    
def print_help():
    print '''cassandra_test1.py clients inserts reads

    --clients=    no of clients
    --inserts=    no of inserts/client
    --help        print this help

    -c        same as --clients
    -i        same as --inserts
    -h        same as --help

    '''

if __name__ == "__main__":
    global nclients # num of threads/clients
    global ninserts # no of inserts
    try:
        options, args = getopt.getopt(
            sys.argv[1:], 'hc:i:', ['clients=',
                                   'inserts=',
                                   'help'])
    except getopt.GetoptError, err:
        print str(err)
        print_help()
        sys.exit(2)
        
    for opt, arg in options:
        if opt in ('--clients'):
            nclients = int(arg)
        elif opt in ('--inserts'):
            ninserts = int(arg)
        elif opt in ('--help'):
            print_help()
            sys.exit(2)

    # default values in case of missing command line arguments
    try:
        nclients
    except NameError:
        nclients = 1
        #print "No of clients need to be specified."
        #print_help()
        #sys.exit(2)

    try:
        ninserts
    except NameError:
        ninserts = 1000

    main()


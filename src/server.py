import threading
import time
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from hashlib import sha256
import argparse
import logging
import xmlrpc.client
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s) %(message)s',)

def getfileinfomap():
    """Gets the fileinfo map"""
    print("GetFileInfoMap()")

    return fileinfomap

# Update a file's fileinfo entry
def updatefile(filename, version, hashlist):
    """Updates a file's fileinfo entry"""
    print("UpdateFile("+filename+")")
    if flag_leader==1:
        entries=[currentTerm,len(log),filename,version,hashlist]
        log.append(entries)
        client_signal.set()
    else:
        raise Exception('An updatefile function occurred')
        return False

    return True

def state_machine_running():
    global lastApplied
    while True:
        while commitIndex > lastApplied:
            lastApplied = lastApplied + 1
            fileinfomap[log[lastApplied][2]] = [log[lastApplied][3], log[lastApplied][4]]
        time.sleep(0.01)

### append entries only change the log,
def append_Entries(prevlogindex, prev_log, entries, leader_term,leader_commit):
    global log, match_index,currentTerm,commitIndex,flag_follower,flag_candidate,flag_leader
    if leader_term >= currentTerm:
        with global_var_lock:
            flag_follower=1
            flag_candidate=0
            flag_leader=0
        if leader_term > currentTerm:
            with global_var_lock:
                currentTerm = leader_term
        if log[prevlogindex] != prev_log:
            match_index = 0
            return False, match_index, currentTerm
        if entries:  ## have the entries value
            if len(log) - 1 == prevlogindex:
                log.append(entries)
            else:
                log[prevlogindex + 1] = entries
                while len(log) > prevlogindex + 2:
                    log.pop()
            match_index = prevlogindex + 1
            ### update the commit index when it appends new log entries
            if leader_commit > commitIndex:
                with global_var_lock:
                    commitIndex = min(leader_commit,prevlogindex + 1 )
            return True, match_index, currentTerm
        else:  ## is a heartbeat
            reset_timer = 1  ##reset the timer
            return True, match_index, currentTerm
    else:
        return False,match_index, currentTerm

def crash():
    global flag_crash,flag_leader,flag_follower,flag_candidate
    flag_crash=1
    flag_leader=0
    flag_follower=0
    flag_candidate=0

def readconfig(config, servernum):
    """Reads cofig file"""
    global serverlist
    fd = open(config, 'r')
    l = fd.readline()

    maxnum = int(l.strip().split(' ')[1])

    if servernum >= maxnum or servernum < 0:
        raise Exception('Server number out of range.')

    d = fd.read()
    d = d.splitlines()

    for i in range(len(d)):
        hostport = d[i].strip().split(' ')[1]
        if i == servernum:
            host = hostport.split(':')[0]
            port = int(hostport.split(':')[1])

        else:
            hostport = "http://" + hostport

            follower=[]
            follower_rpc = xmlrpc.client.ServerProxy(hostport, allow_none=True)
            follower.append(i)
            follower.append(follower_rpc)
            serverlist.append(follower)


    return maxnum, host, port

def tester_getversion(filename):
    return fileinfomap[filename][0]

def requestVote(serverid, candidate_term,candidate_log,candidate_prevlogindex):
    """Requests vote to be the leader"""
    global currentTerm,reset_timer
    global votedFor ,flag_candidate,flag_follower,flag_leader
    ## that is because candidate term must larger than follower, or it cannot get the vote from the follower
    with global_var_lock:
        if candidate_term > currentTerm:
            currentTerm = candidate_term
            reset_timer=1
            flag_candidate=0; flag_follower=1;flag_leader=0
            if candidate_log[candidate_prevlogindex] == log[len(candidate_log)-1]:  ## candidate log[commit index]== fellower log[commit_index]
                votedFor = serverid
                print("I vote for "+str(votedFor))
                return currentTerm, True

    return currentTerm,False

def restore():
    """Restores this metadata store"""
    print("Restore()")
    global flag_candidate
    global flag_leader
    global flag_follower
    global flag_crash
    flag_crash=0
    flag_follower=1
    flag_leader=0
    flag_candidate=0
    return True

def isCrashed():
    """Returns whether this node is crashed or not"""
    print("IsCrashed()")
    if flag_crash==1:
        return True

def ping():
    """A simple ping method"""
    print("Ping()")
    return True

def isLeader():
    """Is this metadata store a leader?"""
    print("always show this")
    if flag_leader == 1:
        return True

class _thread_leader_fellower (threading.Thread):
    def __init__(self, ):
        threading.Thread.__init__(self)
        self.leader_action_finished=threading.Event()
        self.follower_action_finished=threading.Event()
    def run(self):
        # logging.debug('event set: %s', event_is_set)
        logging.debug("Thread: %s start" , (self.name))
        while True:
            print("node"+str(id)+ " currentTerm is "+str(currentTerm))
            if self.isleader():
                print("become leader")
                thread_leader_action=threading.Thread(name='thread_leader_action',
                                target=self.leader_action)
                thread_leader_action.start()
                self.leader_action_finished.wait()## don't finished until all the leader threads is finished
                self.leader_action_finished.clear()
            if self.iscrash():
                print("become crash")
                thread_crash_action=threading.Thread(name='thread_crash_action',
                                target=self.crash_action)
                thread_crash_action.start()
                self.crash_action_finished.wait()## don't finished until all the leader threads is finished

            if self.isfollower():
                print("become follower")
                thread_follower_action=threading.Thread(name='thread_follower_action',
                                target=self.follower_action)
                thread_follower_action.start()
                self.follower_action_finished.wait()## don't finished until all the leader threads is finished
                self.follower_action_finished.clear()
            if self.iscandidate():
                print("become candidate")
                self.candidate_action()
            print("state changed")






    ### leader part
    def leader_action(self): ###??? follower as global variable

        global nextIndex,matchIndex,currentTerm
        with global_var_lock:
            for i in range(maxnum) :
                nextIndex[i]=commitIndex+1
                matchIndex[i] =0

        all_threads=[]
        client_signals=[]
        thread_commit_increase=threading.Thread(name='commit_increase',
                         target=self.commit_increase)
        thread_commit_increase.start()
        for follower in followers:
            client_signal=threading.Event()
            client_signals.append(client_signal)
            thread_append = threading.Thread(name='heartbeat_periodically',
                                             target=self.heartbeat_periodically,
                                             args=( follower,currentTerm))
            thread_client_push = threading.Thread(name='client_push',
                                             target=self.client_push,
                                             args=( client_signal,follower,currentTerm))
            thread_append.start()
            thread_client_push.start()
            all_threads.append(thread_append)
            all_threads.append(thread_client_push)
            ## leader_threads is the list of [thread_append, thread_control]

        while flag_leader==1:
            time.sleep(0.01)
        for client_signal in client_signals:
            client_signal.set()
        for thread in all_threads: ## cannot release directly, must wait at least one heartbeat, but I think the best choic is to use the recursive
            thread.join()
        time.sleep(0.1)
        self.leader_action_finished.set()  ##how to make leader finished, 1. the follower term, 2. rpc call

    def commit_increase(self):
        global flag_leader,nextIndex,match_index,flag_follower,id,commitIndex
        get_agree=0
        if flag_leader==0:
            return
        else:
            for i in range(maxnum):
                if nextIndex[i]>commitIndex+1:
                    get_agree+=1
            if get_agree>=(maxnum-1)/2:
                with global_var_lock:
                    commitIndex=commitIndex+1
        threading,Timer(fresh_frequence,self.commit_increase).start()

    def client_push(self,client_signal,follower,leader_term):
        global flag_leader,nextIndex,match_index,flag_follower

        client_signal.wait()### when we close the leader, we need to set the client_signal
        if flag_leader==0:
            logging.debug("stop leader running")
            return
        prevlogindex = nextIndex[follower[0]] - 1
        prev_log = log[prevlogindex]
        if nextIndex[follower[0]] >= len(log):
            entries = []
        else:
            entries = log[nextIndex[follower[0]]]
        follower_rpc = follower[1]
        flag, match_index, follower_term = follower_rpc.surfstore.append_Entries(prevlogindex, prev_log, entries, leader_term)
        ###judge the case:
        self.lock.acquire()
        if leader_term < follower_term:
            flag_leader = 0
            flag_follower = 1
            global currentTerm
            currentTerm = follower_term
            return
        ### change the global parameter

        if flag == False:
            nextIndex[follower[0]] = nextIndex[follower[0]] - 1
        else:
            match_index[follower[0]] = match_index
        self.lock.release()
        ##
        client_signal.clear()
        threading.Thread(name='client_push',target=self.client_push,
                        args=(client_signal, follower, leader_term)).start()

    def heartbeat_periodically(self,follower,leader_term):##
        global flag_leader, currentTerm,nextIndex,match_index,flag_follower ,log
        if flag_leader == 0:
            ## if it doesn't run well, using global variable
            return
        prevlogindex = nextIndex[follower[0]] - 1
        prev_log = log[prevlogindex]
        if nextIndex[follower[0]] >= len(log):
            entries = []
        else:
            entries = log[nextIndex[follower[0]]]
        follower_rpc = follower[1]
        flag, match_index, follower_term = follower_rpc.surfstore.append_Entries(prevlogindex, prev_log, entries, leader_term)
        ###judge the case:

        if leader_term < follower_term:
            with global_var_lock:
                flag_leader = 0
                flag_follower = 1
                currentTerm = follower_term
            return
        ### change the global parameter
        if flag == False:
            with global_var_lock:
                nextIndex[follower[0]] = nextIndex[follower[0]] - 1
        else:
            with global_var_lock:
                match_index[follower[0]] = match_index

        ##
        ### do that again
        threading.Timer(period_heartbeat, self.heartbeat_periodically,
                        ( follower,leader_term)).start()
    ####candidate part
    def candidate_action(self):
        global followers, getvotes,voteGranted,candidate_exit,flag_leader,flag_candidate,candidate_time_out,period_election,candidate_threads,currentTerm
        candidate_threads=[]
        getvotes = 0
        candidate_time_out = 0
        candidate_exit = 0
        t1=threading.Timer(period_election,self.candidate_timeout)
        candidate_threads.append(t1)

        for follower in followers:
            thread_follower_vote=threading.Thread(name="request_vote", target= self.request_vote_from_follower,args=(follower,))
            candidate_threads.append(thread_follower_vote)

        for thread in candidate_threads:
            thread.start()

        ### regular refresh
        while not candidate_exit:
            if candidate_time_out:
                candidate_exit = 1
                currentTerm = currentTerm + 1
            else:
                if getvotes >=( len(voteGranted) - 1) / 2:
                    flag_leader = 1
                    flag_candidate = 0
                    candidate_exit = 1
                ### what happens if them conflict
            time.sleep(fresh_frequence)
        time.sleep(period_heartbeat)
        for thread in candidate_threads:
            thread.join()
        print("candidate"+str(id)+" get votes:" + str(getvotes))

    def candidate_timeout(self):
        global candidate_time_out,candidate_exit
        with global_var_lock:
            if candidate_exit:
                return
            candidate_time_out = 1


    def request_vote_from_follower(self, follower):

        global currentTerm, flag_candidate, flag_follower, getvotes, voteGranted, flag_leader, candidate_threads, candidate_exit
        if candidate_exit == 1:
            return
        follower_rpc = follower[1]
        ### if the follower is not connected, I do this
        try:
            follower_term, flag = follower_rpc.surfstore.requestVote(id, currentTerm, log, len(log) - 1)
        except:
            print("follower_rpc.surfstore.requestVote work wrong, the follower id is %d" % follower[0])
            t = threading.Timer(period_heartbeat, self.request_vote_from_follower, (follower,))
            candidate_threads.append(t)
            t.start()
            return
        global_var_lock.acquire()
        if follower_term > currentTerm:
            currentTerm = follower_term
            flag_candidate = 0
            flag_follower = 1
            candidate_exit = 1
        global_var_lock.release()
        if flag == True:  ## vote to you
            voteGranted[follower[0]] = True
            getvotes += 1
        if flag == -1:  ## crash
            t = threading.Timer(period_heartbeat, self.request_vote_from_follower, (follower,))
            t.start()
            candidate_threads.append(t)

    ##
    ### follower part
    def follower_action(self):
        global  flag_follower,fresh_frequence,reset_timer
        ### this funciton only creat
        thread_cycle=threading.Timer(period_follower, self.follower_timeout)
        thread_cycle.start()
        ##scan the reset_timer
        while flag_follower==1:
            if reset_timer==1:
                print("follower get reset")
                reset_timer=0
                thread_cycle.cancel()
                del thread_cycle
                thread_cycle = threading.Timer(period_follower, self.follower_timeout)
                thread_cycle.start()
            time.sleep(fresh_frequence )
        if thread_cycle.is_alive():
            thread_cycle.cancel()
        self.follower_action_finished.set()

    def follower_timeout(self):
        global  flag_follower
        global  flag_candidate
        with global_var_lock:
            flag_candidate = 1
            flag_follower = 0
    ##

    def isleader(self):
        if flag_crash==0 and flag_leader==1 and flag_follower==0 and flag_candidate==0:
            return True

    def isfollower(self):
        if flag_crash==0 and flag_leader==0 and flag_follower==1 and flag_candidate==0:
            return True
    def iscrash(self):
        if flag_crash==1 and flag_leader==0 and flag_follower==0 and flag_candidate==0:
            return True
    def iscandidate(self):
        if flag_crash==0 and flag_leader==0 and flag_follower==0 and flag_candidate==1:
            return True

class _thread_as_server (threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        logging.debug("Thread: %s start", (self.name))
        server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SurfStore server")
    parser.add_argument('config', help='path to config file')
    parser.add_argument('servernum', type=int, help='server number')

    args = parser.parse_args()

    config = args.config
    servernum = args.servernum

    # server list has list of other servers
    serverlist = []


    # maxnum is maximum number of servers
    maxnum, host, port = readconfig(config, servernum)

    hashmap = dict()

    fileinfomap = dict()  # filename return [version, hashlist]

    ####
    ### parameter
    currentTerm = 0
    votedFor = None
    id = servernum  ##for example
    log = []  ##[term, index,] ,filename, version, hashlist depend on myself
    log.append([0, 0, "0", 0, 0])
    commitIndex = 0  ## current log index
    lastApplied = 0  ## applied in state machine
    nextIndex = []
    matchIndex = []
    voteGranted = []
    for i in range(maxnum):
        nextIndex.append(1)
        matchIndex.append(0)
        voteGranted.append(False)

    voteGranted[servernum] = True
    followers = serverlist
    print(followers)
    ## the value is matchIndex 2,3,1,2,3, for example, if the leader is 0, then it means fellower 1 have replicated 3 entries
    ## and the fellower 2 have replicated 1 entires
    flag_follower = True
    flag_leader = False
    flag_crash = False
    flag_candidate = False
    period_heartbeat = 0.1  ##s
    period_election = 0.6  ##s
    period_follower=1.25
    reset_timer = 0  ###????
    global_var_lock = threading.Lock()
    fresh_frequence = 0.01
    candidate_exit=0
    candidate_time_out=0
    getvotes=0
    ##

    print("Attempting to start XML-RPC Server...")
    print(host, port)
    server = threadedXMLRPCServer((host, port), requestHandler=RequestHandler)
    server.register_introspection_functions()
    server.register_function(ping, "surfstore.ping")

    server.register_function(getfileinfomap, "surfstore.getfileinfomap")
    server.register_function(updatefile, "surfstore.updatefile")
    # Project 3 APIs
    server.register_function(isLeader, "surfstore.isLeader")
    server.register_function(crash, "surfstore.crash")
    server.register_function(restore, "surfstore.restore")
    server.register_function(isCrashed, "surfstore.isCrashed")
    server.register_function(requestVote, "surfstore.requestVote")
    server.register_function(append_Entries, "surfstore.append_Entries")
    server.register_function(tester_getversion, "surfstore.tester_getversion")
    print("Started successfully.")
    print("Accepting requests. (Halt program to stop.)")
    thread_as_server = _thread_as_server()
    thread_leader_fellower = _thread_leader_fellower()
    threaad_state_machine = threading.Thread(name="state_machine_running", target=state_machine_running)
    thread_as_server.start()
    thread_leader_fellower.start()
    threaad_state_machine.start()
    my_threads = []
    my_threads.append(thread_as_server)
    my_threads.append(thread_leader_fellower)
    my_threads.append(threaad_state_machine)
    for t in my_threads:
        t.join()







import _thread
import threading
import time
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from hashlib import sha256
import argparse
import logging

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s) %(message)s',)

##only check the fileinfomap

### parameter
currentTerm = 0
votedFor = None
id=0##for example
log = []  ##[term, index,] ,filename, version, hashlist depend on myself
log.append([0,0,"0",0,0])
commitIndex = 0## current log index
lastApplied = 0  ## applied in state machine
nextIndex = [0,0,0,0,0] ##
matchIndex = [1,1,1,01,0] ## the value is matchIndex 2,3,1,2,3, for example, if the leader is 0, then it means fellower 1 have replicated 3 entries
## and the fellower 2 have replicated 1 entires
flag_follower=1
flag_leader=False
flag_crash=False
flag_candidate=False
period_heartbeat=0.25##s
period_election=0.6 ##s
reset_timer=0
##
###rpc function
###
def ping():
    """A simple ping method"""
    print("Ping()")
    return True

# Gets a block, given a specific hash value
def getblock(h):
    """Gets a block"""
    print("GetBlock(" + h + ")")

    blockData = bytes(4)
    return blockData

# Puts a block
def putblock(b):
    """Puts a block"""
    print("PutBlock()")

    return True

# Given a list of hashes, return the subset that are on this server
def hasblocks(hashlist):
    """Determines which blocks are on this server"""
    print("HasBlocks()")

    return hashlist

# Retrieves the server's FileInfoMap
def getfileinfomap():
    """Gets the fileinfo map"""
    print("GetFileInfoMap()")

    return fileinfomap

# Update a file's fileinfo entry
def updatefile(filename, version, hashlist):
    """Updates a file's fileinfo entry"""
    print("UpdateFile("+filename+")")

    fileinfomap[filename] = [version, hashlist]

    return True

# PROJECT 3 APIs below

# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""

    print("IsLeader()")
    if flag_leader == 0:
        return True

# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    """Crashes this metadata store"""
    print("Crash()")
    global flag_candidate
    global flag_leader
    global flag_follower
    global flag_crash
    flag_crash=1
    flag_follower=0
    flag_leader=0
    flag_candidate=0
    return True

# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
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


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    print("IsCrashed()")
    if flag_crash==1:
        return True

# Requests vote from this server to become the leader
def requestVote(serverid, candidate_term,candidate_log,candidate_prevlogindex):
    """Requests vote to be the leader"""
    global currentTerm
    ## that is because candidate term must larger than follower, or it cannot get the vote from the follower
    if candidate_term>currentTerm:
        currentTerm=candidate_term
        if log[candidate_prevlogindex]==log[commitIndex]:## candidate log[commit index]== fellower log[commit_index]
            return currentTerm,True
    return currentTerm,False





def tester_getversion(filename):
    return fileinfomap[filename][0]

# Reads the config file and return host, port and store list of other servers
def readconfig(config, servernum):
    """Reads cofig file"""
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
            serverlist.append(hostport)


    return maxnum, host, port
##



if __name__ == "__main__":
    try:
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

        fileinfomap = dict()#filename return [version, hashlist]

        print("Attempting to start XML-RPC Server...")
        print(host, port)
        server = threadedXMLRPCServer((host, port), requestHandler=RequestHandler)
        server.register_introspection_functions()
        server.register_function(ping,"surfstore.ping")
        server.register_function(getblock,"surfstore.getblock")
        server.register_function(putblock,"surfstore.putblock")
        server.register_function(hasblocks,"surfstore.hasblocks")
        server.register_function(getfileinfomap,"surfstore.getfileinfomap")
        server.register_function(updatefile,"surfstore.updatefile")
        # Project 3 APIs
        server.register_function(isLeader,"surfstore.isLeader")
        server.register_function(crash,"surfstore.crash")
        server.register_function(restore,"surfstore.restore")
        server.register_function(isCrashed,"surfstore.isCrashed")
        server.register_function(requestVote,"surfstore.requestVote")
        server.register_function(append_Entries,"surfstore.append_Entries")
        server.register_function(tester_getversion,"surfstore.tester_getversion")
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")
        thread_as_server=_thread_as_server(name=thread_as_server)
        thread_leader_fellower = _thread_leader_fellower(name=thread_as_follower_leader)
        thread_as_server.start()
        thread_leader_fellower.start()
        my_threads=[]
        my_threads.append(thread_as_server)
        my_threads.append(thread_leader_fellower)
        for t in my_threads:
            t.join()


    except Exception as e:
        print("Server: " + str(e))

class _thread_as_server (threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        logging.debug("Thread: %s start", (self.name))
        server.serve_forever()


class _thread_leader_fellower (threading.Thread):
    def __init__(self, ):
        threading.Thread.__init__(self)
        self.lock = threading.Lock()
        self.leader_action_finished=threading.Event()
    def run(self):
        # logging.debug('event set: %s', event_is_set)
        logging.debug("Thread: %s start" , (self.name))
        while True:
            if isLeader():
                thread_leader_action=threading.Thread(name='thread_leader_action',
                                target=self.leader_action)
                thread_leader_action.start()
                leader_action_finished.wait()## don't finished until all the leader threads is finished

            if iscrash():
                self.crash_action()## don't finish until it is not the crash

            if isfollower():
                self.follower_action()

            if iscandidate():
                self.candidate_action()
    def follower_action(self):
        threading.Timer()
        threading.Timer(period_heartbeat, self.heartbeat_periodically,
                        (follower, leader_term)).start()


    def leader_action(self):

        global nextid
        global match_index
        nextid = [commitIndex + 1, commitIndex + 1, commitIndex + 1, commitIndex + 1, commitIndex + 1]
        match_index = [0, 0, 0, 0, 0]
        all_threads=[]
        client_signals=[]
        for follower in followers:
            client_signal=threading.Event()
            client_signals.append(client_signal)
            thread_append = threading.Thread(name='heartbeat_periodically',
                                             target=heartbeat_periodically,
                                             args=( follower,leader_term))
            thread_client_push = threading.Thread(name='client_push',
                                             target=client_push,
                                             args=( client_signal,follower,leader_term))
            thread_append.start()
            thread_client_push.start()
            all_threads.append(thread_append)
            all_threads.append(thread_client_push)
            ## leader_threads is the list of [thread_append, thread_control]

        while flag_leader==1:
            time.sleep(0.01)
        for client_signal in client_signals:
            client_signal.set()
        for thread in all_threads: ## cannot release directly, must wait at least one heartbeat
            thread.join()
        self.leader_action_finished.set()  ##how to make leader finished, 1. the follower term, 2. rpc call

    def client_push(self,client_signal,follower,leader_term):
        global flag_leader
        global nextid
        global match_index
        global flag_follower
        client_signal.wait()### when we close the leader, we need to set the client_signal
        if flag_leader==0:
            logging.debug("stop leader running")
            return
        prevlogindex = nextid[follower[0]] - 1
        prev_log = log[prevlogindex]
        if nextid[follower[0]] >= len(log):
            entries = []
        else:
            entries = log[nextid[follower[0]]]
        follower_rpc = follower[1]
        flag, match_index, follower_term = follower_rpc.append_Entries(prevlogindex, prev_log, entries, leader_term)
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
            nextid[follower[0]] = nextid[follower[0]] - 1
        else:
            match_index[follower[0]] = match_index
        self.lock.release()
        ##
        client_signal.clear()
        threading.Thread(name='client_push',target=self.client_push,
                        args=(client_signal, follower, leader_term)).start()



    def heartbeat_periodically(self,follower,leader_term):##
        global flag_leader
        global nextid
        global match_index
        global flag_follower
        global log
        if flag_leader == 0:
            ## if it doesn't run well, using global variable
            return

        prevlogindex = nextid[follower[0]] - 1
        prev_log = log[prevlogindex]
        if nextid[follower[0]] >= len(log):
            entries = []
        else:
            entries = log[nextid[follower[0]]]
        follower_rpc = follower[1]
        flag, match_index, follower_term = follower_rpc.append_Entries(prevlogindex, prev_log, entries, leader_term)
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
            nextid[follower[0]] = nextid[follower[0]] - 1
        else:
            match_index[follower[0]] = match_index
        self.lock.release()
        ##

        ### do that again
        threading.Timer(period_heartbeat, self.heartbeat_periodically,
                        ( follower,leader_term)).start()



    def timeout(self):
        flag_candidate = 1
        flag_follower = 0

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
        if flag_crash==0 and flag_leader==0 and flag_follower==1 and flag_candidate==0:
            return True


def crash():
    flag_crash=1
    flag_leader=0
    flag_follower=0
    flag_candidate=0




def append_Entries(prevlogindex, prev_log, entries, leader_term):
        if leader_term == term:
            if log[prevlogindex] != prev_log:
                match_index = 0
                return False, match_index, term
            if entries:  ## have the entries value
                if len(log) - 1 == prevlogindex:
                    log.append(entries)
                else:
                    log[prevlogindex + 1] = entries
                    while len(log) > prevlogindex + 2:
                        log.pop()
                match_index = prevlogindex + 1
                return True, match_index, term
            else:  ## is a heartbeat
                reset_timer = 1  ##reset the timer
                return True, match_index, term

## the next task I should do is to how to initiate the parameter, follower,
## and identify the function is right

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

### parameter
currentTerm = 0
votedFor = None
id=0##for example
log = []  ##[term, index,command]
commitIndex = 0## current log index
lastApplied = 0  ## applied in state machine
nextIndex = [0,0,0,0,0] ##
matchIndex = [0,0,0,0,0] ## the value is matchIndex 2,3,1,2,3, for example, if the leader is 0, then it means fellower 1 have replicated 3 entries
## and the fellower 2 have replicated 1 entires



flag_follower=False
flag_leader=False
flag_crash=False
flag_candidate=False
period_heartbeat=0.25##s
period_election=0.6 ##s
reset_timer=0
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
        server.register_function(appendEntries,"surfstore.appendEntries")
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
    def run(self):
        # logging.debug('event set: %s', event_is_set)
        logging.debug("Thread: %s start" , (self.name))
        while True:
            if isLeader():
                leader_action()## don't finished until it is not the leader

            if iscrash():
                crash_action()

            if isfollower():
                follower_action()

            if iscandidate():
                candidate_action()

    def leader_action(self):
        global nextid
        global match_index
        nextid = [commitIndex + 1, commitIndex + 1, commitIndex + 1, commitIndex + 1, commitIndex + 1]
        match_index = [0, 0, 0, 0, 0]

        leader_threads = {}
        time_signals = {}
        for follower in followers:
            prevlogindex = nextid[follower[0]] - 1
            prev_log = log[prevlogindex]
            follower_signal = threading.Event()
            time_signals[follower[0]] = follower_signal
            if nextid[follower[0]] >= len(log):
                entries = []
            else:
                entries = log[nextid[follower[0]]]
            thread_append = threading.Thread(name='periodically append',
                                             target=append_entries_to_follower,
                                             args=(follower_signal, follower, prevlogindex, prev_log, entries, currentTerm))
            thread_append.setDaemon(True)
            thread_append.start()
            thread_append_follower = []
            thread_append_follower.append(thread_append)
            thread_control = threading.Timer(period_heartbeat, leader_signal_periodically, (follower_signal,))
            thread_control.setDaemon(True)
            thread_control.start()
            thread_append_follower.append(thread_control)
            leader_threads[follower[0]] = thread_append_follower
            ## leader_threads is the list of [thread_append, thread_control]

        while flag_leader:
            pass


    def leader_signal_periodically(self,time_signal):

        time_signal.set()
        threading.Timer(period_heartbeat, leader_signal_periodically, (follower_signal,)).start()

    def append_entries_to_follower(self,time_signal,follower,prevlogindex, prev_log,entries,leader_term ):
        global flag_leader
        global nextid
        global match_index
        time_signal.wait()
        follower_rpc = follower[1]
        flag,match_index,follower_term=follower_rpc.append_Entries(prevlogindex, prev_log, entrie, leader_term)
        time_signal.clear()
        ###judge the case:
        if leader_term <follower_term:
            flag_leader=0
            flag_follower=1
            global currentTerm
            currentTerm=follower_term
            return

        if flag==False:
            nextid[follower[0]]=nextid[follower[0]] - 1
        else:
            match_index[follower[0]]=match_index
        ##

        prevlogindex = nextid[follower[0]] - 1
        prev_log = log[prevlogindex]
        self.append_entries_to_follower(time_signal, follower, prevlogindex, prev_log, entries, leader_term)



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


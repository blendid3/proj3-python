from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from hashlib import sha256
import argparse
import xmlrpc.client
from threading import Event, Thread

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

# Variables
_clients = None
_isCrashed = False
_isLeader = False
_isCandidate = False
_term = 0
_votedFor = None
_voteResults = []
_log = [(0,)]
_commitIndex = 0
_lastApplied = 0
_nextIndex = []
_matchIndex = []
_electionTimeoutEvent = Event()
_electionTimeout = 0.5
_heartbeatTimeoutEvent = Event()
_heartbeatTimeout = 0.25

# A simple ping, returns true
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
    if isCrashed() or not isLeader():
        raise BaseException()
    print("GetFileInfoMap()")

    return fileinfomap

# Update a file's fileinfo entry
def updatefile(filename, version, hashlist):
    """Updates a file's fileinfo entry"""
    global _log
    if isCrashed() or not isLeader():
        raise BaseException()
    clients = [xmlrpc.client.ServerProxy('http://' + serverlist[i]) for i in range(len(serverlist))]
    entry = (_term, filename, version)
    _log.append(entry)
    
    client.surfstore.appendEntries(servernum, _term, len(_log) - 2, _log[-2][0], [entry], _commitIndex)
    
    print("UpdateFile("+filename+")")
    if filename not in fileinfomap.keys():
        if version != 1:
            return False
    elif version != fileinfomap[filename][0] + 1:
        return False
        
    fileinfomap[filename] = [version, hashlist]

    return True

# PROJECT 3 APIs below

# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""
    print("IsLeader()")
    return _isLeader

# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    """Crashes this metadata store"""
    global _isCrashed
    global _isLeader
    global _isCandidate
    global _nextIndex
    global _matchIndex
    print("Crash()")
    _isCrashed = True
    _isLeader = False
    _isCandidate = False
    _nextIndex = []
    _matchIndex = []
    return True

# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    """Restores this metadata store"""
    global _isCrashed
    global _isLeader
    global _isCandidate
    global _nextIndex
    global _matchIndex
    print("Restore()")
    _isCrashed = False
    _isLeader = False
    _isCandidate = False
    _nextIndex = []
    _matchIndex = []
    return True


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    print("IsCrashed()")
    return _isCrashed

# Requests vote from this server to become the leader
def requestVote(serverid, term, last_log_index, last_log_term):
    """Requests vote to be the leader"""
    global _log
    global _votedFor
    global _isLeader
    global _isCandidate
    global _nextIndex
    global _matchIndex
    global _term
    if isCrashed():
        return None
    _electionTimeoutEvent.set()
    if term < _term:
        return [False, _term]
    if term > _term:
        _term = term
        _isLeader = False
        _isCandidate = False
        _nextIndex = []
        _matchIndex = []
    if _votedFor is not None and _votedFor != serverid:
        return [False, _term]
    if _log[-1][0] > last_log_term or (_log[-1][0] == last_log_term and len(_log) > last_log_index):
        return [False, _term]
    _votedFor = serverid
    return [True, _term]

# Updates fileinfomap
def appendEntries(serverid, term, prev_log_index, prev_log_term, entries, leader_commit):
    """Updates fileinfomap to match that of the leader"""
    global _log
    global _commitIndex
    global _isLeader
    global _isCandidate
    global _nextIndex
    global _matchIndex
    global _term
    if isCrashed():
        return None
    if term < _term:
        return [False, _term]
    _isCandidate = False
    if term > _term:
        _term = term
        _isLeader = False
        _nextIndex = []
        _matchIndex = []
    if len(_log) - 1 < prev_log_index or _log[prev_log_index][0] != term:
        return [False, _term]
    for i in range(len(entries)):
        entry_term = entries[i][0]
        new_index = prev_log_index + 1 + i
        if len(_log) < new_index + 1:
            break
        if _log[new_index][0] != entry_term:
            _log = _log[:new_index]
            break
    _log.extend(entries[i:])
    if leader_commit > _commitIndex:
        _commitIndex = min(leader_commit, len(_log) - 1)
    return [True, _term]

def tester_getversion(filename):
    return 0 if filename not in fileinfomap.keys() else fileinfomap[filename][0]

def rpc_append_entries(client, i, entries):
    global _voteResults
    global _term
    global _log
    global _isLeader
    global _isCandidate
    global _nextIndex
    global _matchIndex
    if i >= len(_nextIndex):
        return
    while True:
        rpc_results = client.surfstore.appendEntries(servernum, _term, _nextIndex[i] - 1, _log[_nextIndex[i] - 1][0], entries, _commitIndex)
        if rpc_results[1] > _term:
            _term = rpc_results[1]
            _isLeader = False
            _isCandidate = False
            _nextIndex = []
            _matchIndex = []
            break
        if rpc_results[0]:
            if not isLeader():
                break
            _matchIndex[i] = _nextIndex[i] - 1 + len(entries)
            _nextIndex[i] += len(entries)
            break
        elif not isLeader():
            break
        else:
            _nextIndex[i] -= 1

def rpc_request_vote(client, i):
    global _voteResults
    global _term
    global _log
    global _isLeader
    global _isCandidate
    global _nextIndex
    global _matchIndex
    if i >= len(_voteResults):
        return
    rpc_results = client.surfstore.requestVote(servernum, _term, len(_log) - 1, _log[-1][0])
    if rpc_results[1] > _term:
        _term = rpc_results[1]
        _isLeader = False
        _isCandidate = False
        _nextIndex = []
        _matchIndex = []
    _voteResults[i] = rpc_results[0]

def leader():
    global _log
    global _term
    global _votedFor
    global _voteResults
    global _matchIndex
    global _nextIndex
    global _heartbeatTimeoutEvent
    global _heartbeatTimeout
    global _clients
    global _isCandidate
    global _isLeader

    assert not _isCandidate
    _matchIndex = [0] * len(serverlist)
    _nextIndex = [len(_log)] * len(serverlist)
    while True:
        wait_thread = Thread(target = _heartbeatTimeoutEvent.wait, args = (_heartbeatTimeout, ))
        wait_thread.start()
        rpc_threads = [Thread(target = rpc_append_entries, args = (client, i, [])) for i, client in enumerate(_clients) if not _voteResults[i]]
        for thread in rpc_threads:
            thread.start()
        for thread in rpc_threads:
            thread.join()
        if not isLeader():
            _heartbeatTimeoutEvent.set()
            _heartbeatTimeoutEvent.clear()
            return
        N = sorted(_matchIndex)[len(_matchIndex) // 2]
        if _log[N][0] == _term:
            _commitIndex=max(_commitIndex, N)
        if sum(int(_voteResults)) * 2 >= len(_voteResults):
            _heartbeatTimeoutEvent.set()
            _isCandidate = False
            _isLeader = True
            _heartbeatTimeoutEvent.clear()
            return
        wait_thread.join()

def candidate():
    global _term
    global _votedFor
    global _voteResults
    global _matchIndex
    global _nextIndex
    global _heartbeatTimeoutEvent
    global _heartbeatTimeout
    global _clients
    global _isCandidate
    global _isLeader

    assert not isLeader()
    _term += 1
    _votedFor = servernum
    _voteResults = [False for _ in range(len(serverlist))]
    _matchIndex = []
    _nextIndex = []
    while sum(int(_voteResults)) * 2 < len(_voteResults):
        wait_thread = Thread(target = _heartbeatTimeoutEvent.wait, args = (_heartbeatTimeout, ))
        wait_thread.start()
        rpc_threads = [Thread(target = rpc_request_vote, args = (client, i)) for i, client in enumerate(_clients) if not _voteResults[i]]
        for thread in rpc_threads:
            thread.start()
        for thread in rpc_threads:
            thread.join()
        if not _isCandidate:
            _heartbeatTimeoutEvent.set()
            _heartbeatTimeoutEvent.clear()
            return
        if _electionTimeoutEvent.is_set():
            _electionTimeoutEvent.clear()
            _heartbeatTimeoutEvent.set()
            _heartbeatTimeoutEvent.clear()
            return
        if sum(int(_voteResults)) * 2 >= len(_voteResults):
            _heartbeatTimeoutEvent.set()
            _isCandidate = False
            _isLeader = True
            _heartbeatTimeoutEvent.clear()
            return
        wait_thread.join()

def follower(event, timeout):
    _matchIndex = []
    _nextIndex = []
    while event.wait(timeout):
        event.clear()

def raft_server():
    global _matchIndex
    global _nextIndex
    global _log
    global _electionTimeout
    global _electionTimeoutEvent
    global _term
    global _votedFor
    global _isCandidate
    global _clients
    global _voteResults
    _clients = [xmlrpc.client.ServerProxy('http://' + serverlist[i]) for i in range(len(serverlist))]
    _voteResults = [False for _ in range(len(serverlist))]

    while True:
        if isLeader():
            thread = Thread(target = leader)
            thread.start()
            thread.join()
        elif _isCandidate:
            thread = Thread(target = candidate)
            thread.start()
            thread.join(_electionTimeout)
            if thread.is_alive():
                _electionTimeoutEvent.set()
            thread.join()
        else:
            thread = Thread(target = follower, args = (_electionTimeoutEvent, _electionTimeout))
            thread.start()
            thread.join()
            if not isCrashed():
                _isCandidate = True
    pass

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

        fileinfomap = dict()

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
        
        thread = Thread(target = raft_server)
        thread.start()
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")
        server.serve_forever()
        thread.join()
    except Exception as e:
        print("Server: " + str(e))

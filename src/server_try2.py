import argparse
import random
import time
import xmlrpc.client
from hashlib import sha256
from socketserver import ThreadingMixIn
from threading import Event, Timer, Thread
from xmlrpc.server import SimpleXMLRPCRequestHandler, SimpleXMLRPCServer


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
_log = [(0, None, None, None)]
_commitIndex = 0
_lastApplied = 0
_nextIndex = []
_matchIndex = []
_electionTimeoutEvent = Event()
_electionTimeout = 1 / 2
_heartbeatTimeoutEvent = Event()
_heartbeatTimeout = 1 / 16
_short_time = 1 / 128

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
    if _isCrashed or not _isLeader:
        raise BaseException()
    print("GetFileInfoMap()")

    return fileinfomap

# Update a file's fileinfo entry


def updatefile(filename, version, hashlist):
    """Updates a file's fileinfo entry"""
    global _log
    global _short_time
    global _term

    if _isCrashed or not _isLeader:
        raise BaseException()
    print("UpdateFile("+filename+")")
    if (filename not in fileinfomap.keys() and version != 1) or (filename in fileinfomap.keys() and version != fileinfomap[filename][0] + 1):
        return False
    _log.append((_term, filename, version, hashlist))
    while filename not in fileinfomap.keys() or fileinfomap[filename][0] != version or fileinfomap[filename][1] != hashlist:
        time.sleep(_short_time)
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
    print("Crash()")
    _isCrashed = True
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
    global _isCrashed
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
    global _isCrashed
    if _isCrashed:
        raise BaseException()
    if not _isCandidate and not _isLeader:
        _electionTimeoutEvent.set()
    if term < _term:
        return [False, _term]
    if term > _term:
        _term = term
        _votedFor = None
        _isLeader = False
        _isCandidate = False
        _nextIndex = []
        _matchIndex = []
    if _votedFor is not None and _votedFor != serverid:
        return [False, _term]
    if _log[-1][0] > last_log_term or (_log[-1][0] == last_log_term and len(_log) > last_log_index + 1):
        return [False, _term]
    _votedFor = serverid
    return [True, _term]

# Updates fileinfomap


def appendEntries(serverid, term, prev_log_index, prev_log_term, entries, leader_commit):
    """Updates fileinfomap to match that of the leader"""
    global _log
    global _commitIndex
    global _lastApplied
    global _isLeader
    global _isCandidate
    global _nextIndex
    global _matchIndex
    global _term
    global _votedFor
    global _isCrashed
    if _isCrashed:
        raise BaseException()
    _electionTimeoutEvent.set()
    if term < _term:
        return [False, _term]
    _isCandidate = False
    if term > _term:
        _term = term
        _votedFor = None
        _isLeader = False
        _nextIndex = []
        _matchIndex = []
    if len(_log) - 1 < prev_log_index or _log[prev_log_index][0] != prev_log_term:
        return [False, _term]
    for i in range(len(entries)):
        entry_term = entries[i][0]
        new_index = prev_log_index + 1 + i
        if len(_log) < new_index + 1:
            break
        if _log[new_index][0] != entry_term:
            _log = _log[:new_index]
            break
    if len(entries) > 0:
        _log.extend(entries[i:])
    if leader_commit > _commitIndex:
        _commitIndex = min(leader_commit, len(_log) - 1)
    if _commitIndex > _lastApplied:
        for i in range(_lastApplied, _commitIndex):
            filename, version, hashlist = _log[i + 1][1:]
            fileinfomap[filename] = [version, hashlist]
        _lastApplied = _commitIndex
    return [True, _term]


def tester_getversion(filename):
    return 0 if filename not in fileinfomap.keys() else fileinfomap[filename][0]


def rpc_append_entries(client, i, entries_start, entries_end):
    global _voteResults
    global _votedFor
    global _term
    global _log
    global _isLeader
    global _isCandidate
    global _nextIndex
    global _matchIndex
    global _isCrashed

    if _isCrashed or i >= len(_nextIndex):
        return
    while not _isCrashed:
        assert entries_start >= 0 and entries_end >= 0
        entries = _log[entries_start:entries_end]
        rpc_results = client.surfstore.appendEntries(
            servernum, _term, _nextIndex[i] - 1, _log[_nextIndex[i] - 1][0], entries, _commitIndex)
        if rpc_results[1] > _term:
            _term = rpc_results[1]
            _votedFor = None
            _isLeader = False
            _isCandidate = False
            _nextIndex = []
            _matchIndex = []
            break
        if rpc_results[0]:
            if not _isLeader:
                break
            _matchIndex[i] = _nextIndex[i] - 1 + len(entries)
            _nextIndex[i] += len(entries)
            if _nextIndex[i] == len(_log):
                break
            entries_start += len(entries)
            entries_end += len(entries)
            if len(entries) == 0:
                entries_end += 1
        elif not _isLeader:
            break
        else:
            _nextIndex[i] -= 1
            entries_start -= 1
            entries_end -= 1


def rpc_request_vote(client, i):
    global _voteResults
    global _votedFor
    global _term
    global _log
    global _isLeader
    global _isCandidate
    global _nextIndex
    global _matchIndex
    global _isCrashed

    if _isCrashed or i >= len(_voteResults):
        return
    rpc_results = client.surfstore.requestVote(
        servernum, _term, len(_log) - 1, _log[-1][0])
    if rpc_results[1] > _term:
        _term = rpc_results[1]
        _votedFor = None
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
    global _isCandidate
    global _isLeader
    global _lastApplied
    global _commitIndex
    global _short_time
    global _isCrashed

    assert not _isCandidate
    _matchIndex = [0] * len(serverlist)
    _nextIndex = [len(_log)] * len(serverlist)
    while not _isCrashed:
        wait_thread = Thread(
            target=_heartbeatTimeoutEvent.wait, args=(_heartbeatTimeout, ))
        wait_thread.start()
        rpc_threads = [Thread(target=rpc_append_entries, args=(client, i, len(_log) - 1, len(_log) - 1))
                       for i, client in enumerate([xmlrpc.client.ServerProxy('http://' + serverlist[i]) for i in range(len(serverlist))])]
        for thread in rpc_threads:
            thread.start()
        while True:
            for thread in rpc_threads:
                thread.join(_short_time)
            if not _isLeader:
                _heartbeatTimeoutEvent.set()
                _heartbeatTimeoutEvent.clear()
                return
            N = sorted(_matchIndex)[len(_matchIndex) // 2]
            if _log[N][0] == _term:
                _commitIndex = max(_commitIndex, N)
            if _commitIndex > _lastApplied:
                for i in range(_lastApplied, _commitIndex):
                    filename, version, hashlist = _log[i + 1][1:]
                    fileinfomap[filename] = [version, hashlist]
                _lastApplied = _commitIndex
            if not wait_thread.is_alive():
                wait_thread.join()
                break


def candidate():
    global _term
    global _votedFor
    global _voteResults
    global _matchIndex
    global _nextIndex
    global _heartbeatTimeoutEvent
    global _heartbeatTimeout
    global _isCandidate
    global _isLeader
    global _short_time
    global _isCrashed

    t = Timer(_electionTimeout * (1 - random.random() / 3), _electionTimeoutEvent.set)
    t.start()
    assert not _isLeader
    _term += 1
    print("Server", servernum, "candidate for term", _term)
    _votedFor = servernum
    _voteResults = [False for _ in range(len(serverlist))]
    _matchIndex = []
    _nextIndex = []
    while (not _isCrashed) and sum(_voteResults) * 2 < len(_voteResults):
        wait_thread = Thread(
            target=_heartbeatTimeoutEvent.wait, args=(_heartbeatTimeout, ))
        wait_thread.start()
        rpc_threads = [Thread(target=rpc_request_vote, args=(client, i)) for i, client in enumerate(
            [xmlrpc.client.ServerProxy('http://' + serverlist[i]) for i in range(len(serverlist))]) if not _voteResults[i]]
        for thread in rpc_threads:
            thread.start()
        while True:
            for thread in rpc_threads:
                thread.join(_short_time)
            if not _isCandidate:
                _heartbeatTimeoutEvent.set()
                _heartbeatTimeoutEvent.clear()
                return
            if _electionTimeoutEvent.is_set():
                print(time.time())
                _electionTimeoutEvent.clear()
                _heartbeatTimeoutEvent.set()
                _heartbeatTimeoutEvent.clear()
                return
            if sum(_voteResults) * 2 >= len(_voteResults):
                _heartbeatTimeoutEvent.set()
                _isCandidate = False
                _isLeader = True
                print("Leader voted: ", servernum)
                _heartbeatTimeoutEvent.clear()
                return
            if not wait_thread.is_alive():
                wait_thread.join()
                break


def follower(event, timeout):
    _matchIndex = []
    _nextIndex = []
    while event.wait(timeout * (1 - random.random() / 3)):
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
    global _isCrashed
    global _short_time

    _clients = [xmlrpc.client.ServerProxy(
        'http://' + serverlist[i]) for i in range(len(serverlist))]
    _voteResults = [False for _ in range(len(serverlist))]

    while True:
        if _isCrashed:
            time.sleep(_short_time)
            continue
        elif _isLeader:
            leader()
        elif _isCandidate:
            candidate()
        else:
            follower(_electionTimeoutEvent, _electionTimeout)
            if not _isCrashed:
                _isCandidate = True

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
        server = threadedXMLRPCServer(
            (host, port), requestHandler=RequestHandler, logRequests=False)
        server.register_introspection_functions()
        server.register_function(ping, "surfstore.ping")
        server.register_function(getblock, "surfstore.getblock")
        server.register_function(putblock, "surfstore.putblock")
        server.register_function(hasblocks, "surfstore.hasblocks")
        server.register_function(getfileinfomap, "surfstore.getfileinfomap")
        server.register_function(updatefile, "surfstore.updatefile")
        # Project 3 APIs
        server.register_function(isLeader, "surfstore.isLeader")
        server.register_function(crash, "surfstore.crash")
        server.register_function(restore, "surfstore.restore")
        server.register_function(isCrashed, "surfstore.isCrashed")
        server.register_function(requestVote, "surfstore.requestVote")
        server.register_function(appendEntries, "surfstore.appendEntries")
        server.register_function(
            tester_getversion, "surfstore.tester_getversion")

        thread = Thread(target=raft_server)
        thread.start()
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")
        server.serve_forever()
        thread.join()
    except Exception as e:
        print("Server: " + str(e))

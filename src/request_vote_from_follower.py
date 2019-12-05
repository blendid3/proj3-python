def request_vote_from_follower(self, follower):
    global_var_lock.acquire()
    global currentTerm, flag_candidate, flag_follower, getvotes, voteGranted, flag_leader, candidate_threads, candidate_exit
    if candidate_exit == 1:
        global_var_lock.release()
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
        global_var_lock.release()
        return

    if follower_term > currentTerm:
        currentTerm = follower_term
        flag_candidate = 0
        flag_follower = 1
        candidate_exit = 1
    if flag == True:  ## vote to you
        voteGranted[follower[0]] = True
        getvotes += 1
    if flag == -1:  ## crash
        t = threading.Timer(period_heartbeat, self.request_vote_from_follower, (follower,))
        t.start()
        candidate_threads.append(t)
    print("I get votes:" + getvotes)
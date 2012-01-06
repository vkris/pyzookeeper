def election(zk_ip):
    from pyzookeeper.algos.election import LeaderElection
    return LeaderElection(zk_ip)


import logging

#log = logging.getLogger('pyzookeeper')

format_string = "%(asctime)s %(name)s [%(levelname)s]:%(message)s"
level = 'INFO'
logger = logging.getLogger('pyzookeeper')
logger.setLevel(level)
lhandler = logging.StreamHandler()
lhandler.setLevel(level)
fm = logging.Formatter(format_string)
lhandler.setFormatter(fm)
logger.addHandler(lhandler)
log = logger


def election(zk_ip, qname):
    from pyzookeeper.algos.election import LeaderElection
    return LeaderElection(zk_ip,qname)


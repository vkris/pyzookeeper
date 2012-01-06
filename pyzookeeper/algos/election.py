import zookeeper, threading
import sys, os, time
from threading import Thread
from pyzookeeper import log

ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"};

class LeaderElection(Thread):
    '''
    This class uses zookeeper library to do leader election. 
    Input :
        zookeeper's ip and port in ip:port[,ip2:port2,..] format. 
        node name. This will be a persistant node.
    '''
    def __init__(self, zk_ip, nodename):
        # Frequency to check for change of status ( in seconds )
        self.frequency = 10
        self.connected = False   
        # Name of the master znode
        self.nodename = nodename
        # Used for checking if connection is established.
        self.cv = threading.Condition()
        self.isLeader = False
        zookeeper.set_log_stream(open('/dev/null'))
        Thread.__init__(self) 
        # Call this watch method when something is executed.
        # Acquire condition variable so that the program waits until
        # notified by watcher.
        def watcher(handle, type, state, path):
            log.info("  Connected to zookeeper")
            self.cv.acquire()
            self.connected = True
            self.cv.notify()
            self.cv.release()

        self.cv.acquire()
        self.handle = zookeeper.init(zk_ip, watcher, 10000)
        # If watcher does not notify after 10 seconds, they proceed. It should probably error out
        # in the next check.
        self.cv.wait(10.0)
        if not self.connected:
            log.error("Cannot connect to the zookeeper instance. is a server running on "+zk_ip)
            sys.exit()
        self.cv.release()
        
        # Create the master zNode here.
        try:
            # master node cannot be ephemeral because you cannot create childrens for ephemeral nodes.
            zookeeper.create(self.handle,self.nodename,"Election Master Node", [ZOO_OPEN_ACL_UNSAFE],0)
        except zookeeper.NodeExistsException:
            log.debug("Election node already exists")
        except zookeeper.BadArgumentsException:
            log.error('Did you forget a / at the begging of the queue name?')
            sys.exit(0)

    def nominateCandidate(self):
        '''
        Creates a zNode inside the master zNode, it does not care what data is inside the zNode,
        it just needs the node.
        It is both sequential and ephemeral ( last parameter ).
        Sequential - to keep track for elections.  Ephemeral- to detect failure of a node.
        '''
        self.candidateName = zookeeper.create(self.handle, self.nodename+"/candidate-", "I don't care",
                             [ZOO_OPEN_ACL_UNSAFE],3)

    def leaderExists(self, handle, nodename):
        '''
        Check if leader exists
        '''
        return False if zookeeper.exists( handle, nodename) is None else True        

    def electLeader(self, handle, nodename, candidateName):
        '''
        This method elects the new leader from the list of candidates.
        The candidate with the lowest sequence Id gets to be the leader.
        '''
        candidates = zookeeper.get_children(handle, nodename)
        myName = candidateName.rsplit('/',1)[1]
        leader = sorted(candidates)[0]
        if ( myName == leader):
            try:
                zookeeper.create(handle, nodename+"/leader",myName,[ZOO_OPEN_ACL_UNSAFE],zookeeper.EPHEMERAL)
                log.info(" I won the elections, I am leader now :)")
                self.isLeader = True
            except zookeeper.NodeExistsException,e:
                log.info(e)
        else:
            log.info(" I failed in the elections :(")
            self.isLeader = False

    def run(self):
        '''
        This runs on a loop. If a leader does not exist, it calls for an election. Else sleeps for a while.
        '''
        self.nominateCandidate()
        while(True):
            if ( not self.leaderExists(self.handle, self.nodename+"/leader")):
                log.info(" No leader, Need an election right now..")
                self.electLeader(self.handle, self.nodename, self.candidateName)            
            time.sleep(float(self.frequency))

    def checkLeader(self):
        return self.isLeader

    def runElection(self):
        '''
        Starts the thread
        '''
        self.setDaemon(True)
        self.start()



if __name__=="__main__":
    pass

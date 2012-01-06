import zookeeper, threading
import sys, os, time
from threading import Thread

ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"};

class LeaderElection(Thread):
    '''
    This class uses zookeeper library to do leader election. 
    Input is the zookeeper's ip and port in ip:port format.
    '''
    def __init__(self, zk_ip):
        # Frequency to check for change of status ( in seconds )
        self.frequency = 10
        self.connected = False   
        # Name of the master znode
        self.queuename = '/spider-election'  
        # Used for checking if connection is established.
        self.cv = threading.Condition()
        self.isLeader = False
        zookeeper.set_log_stream(open('/dev/null'))
        Thread.__init__(self) 
        # Call this watch method when something is executed.
        # Acquire condition variable so that the program waits until
        # notified by watcher.
        def watcher(handle, type, state, path):
            print "Connected to zookeeper"
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
            print "Cannot connect to the zookeeper instance. is a server running on "+zk_ip
            sys.exit()
        self.cv.release()
        
        # Create the master zNode here.
        try:
            zookeeper.create(self.handle,self.queuename,"Election Master Node", [ZOO_OPEN_ACL_UNSAFE],0)
        except zookeeper.NodeExistsException:
            print "Election node already exists"

    def nominateCandidate(self):
        '''
        Creates a zNode inside the master zNode, it does not care what data is inside the zNode,
        it just needs the node.
        It is both sequential and ephemeral ( last parameter ).
        Sequential - to keep track for elections.  Ephemeral- to detect failure of a node.
        '''
        self.candidateName = zookeeper.create(self.handle, self.queuename+"/candidate-", "I don't care",
                             [ZOO_OPEN_ACL_UNSAFE],3)

    def leaderExists(self, handle, queuename):
        '''
        Check if leader exists
        '''
        return False if zookeeper.exists( handle, queuename) is None else True        

    def electLeader(self, handle, queuename, candidateName):
        '''
        This method elects the new leader from the list of candidates.
        The candidate with the lowest sequence Id gets to be the leader.
        '''
        candidates = zookeeper.get_children(handle, queuename)
        myName = candidateName.rsplit('/',1)[1]
        leader = sorted(candidates)[0]
        if ( myName == leader):
            try:
                zookeeper.create(handle, queuename+"/leader",myName,[ZOO_OPEN_ACL_UNSAFE],zookeeper.EPHEMERAL)
                print "I won, I am leader now :)"
                self.isLeader = True
            except zookeeper.NodeExistsException,e:
                print e
        else:
            print "I failed in the elections"
            self.isLeader = False

    def run(self):
        '''
        This runs on a loop. If a leader does not exist, it calls for an election. Else sleeps for a while.
        '''
        self.nominateCandidate()
        while(True):
            if ( not self.leaderExists(self.handle, self.queuename+"/leader")):
                print "No leader, Need an election right now.."
                self.electLeader(self.handle, self.queuename, self.candidateName)            
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
    # Leader will execute the parameter passed below
    if ( len( sys.argv) < 4):
        print 'Usage:\n python '+ sys.argv[0]+' zookeeperIP:Port leader_script candidate_script'
        sys.exit(0)
    ipAndPort = sys.argv[1]
    leader_script = sys.argv[2]
    cand_script = sys.argv[3]
    e = LeaderElection(ipAndPort)
    e.setDaemon(True)
    e.start() # runElection()
    # The below variable is to make sure the leader/candidate
    # script is executed only once every change of status.
    previousScript = ''
    while(True):
        if(e.checkLeader() and previousScript != 'leader' ):
            print 'Execute Leader script'
            os.system(leader_script)
            previousScript = 'leader'
        elif( not (e.checkLeader()) and previousScript != 'candidate'):
            print 'Execute candidate script'
            os.system(cand_script)
            previousScript = 'candidate'
        else:
            print "Nothing to do..so sleeping.."
            time.sleep(10)

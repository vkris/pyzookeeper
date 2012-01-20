import sys, os, time
sys.path[0:0] = [os.path.join(os.path.dirname(__file__), ".."),]
from pyzookeeper import election
from pyzookeeper import log

if __name__=="__main__":
    # Leader will execute the parameter passed below
    if ( len( sys.argv) < 3):
        print 'Usage:\n python '+ sys.argv[0]+' zookeeperIP:Port zNodeName failure_script'
        print 'zookeeperIP:Port - Point to a zookeeper instance'
        print 'zNodeName - A name to store in zookeeper'
        sys.exit(0)
    ipAndPort = sys.argv[1]
    zNodeName = '/'+sys.argv[2]
    failure_script = sys.argv[3]

    e = election(ipAndPort,zNodeName)
    # The below variable is to make sure the leader/candidate
    # script is executed only once every change of status.
    failed = False
    while(True):
        if ( not e.leaderExists(zNodeName) ):
            log.info("Leader not found, so executing failure script")
            os.system(failure_script)
            failed=True            
            time.sleep(900)            
        else:
            if(failed):
                log.info('Leader is now alive, back to normal')
                failed=False
            time.sleep(10)

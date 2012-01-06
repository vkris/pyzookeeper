import sys, os, time
sys.path[0:0] = [os.path.join(os.path.dirname(__file__), ".."),]
from pyzookeeper import election

if __name__=="__main__":
    # Leader will execute the parameter passed below
    if ( len( sys.argv) < 4):
        print 'Usage:\n python '+ sys.argv[0]+' zookeeperIP:Port leader_script candidate_script'
        sys.exit(0)
    ipAndPort = sys.argv[1]
    leader_script = sys.argv[2]
    cand_script = sys.argv[3]
    e = election(ipAndPort)
    e.runElection()
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

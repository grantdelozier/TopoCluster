import sys

if len(sys.argv) >= 3:
    print sys.argv
    args = sys.argv

    mode_arg = args[args.index("-mode")+1]
    print mode_arg
    if mode_arg.lower() == "loaddb":
        import LoadDBV1
        print "Starting DB Load Process"

    if "-help" in args:
        print "TopoCluster Run Modes:"
        print "-mode loadDB"
        print "-mode morans"
        print ""
        print "======================"
        print "=======Example========"
        print "======================"
        print "python TopoCluster.py -mode loadDB -tf /directory/trainfile.txt -dtbl document_table -conn \"dbname=someDB user=userID host='localhost'\" "
        
    

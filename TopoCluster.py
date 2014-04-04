import sys

if len(sys.argv) >= 3:
    print sys.argv
    args = sys.argv

    mode_arg = args[args.index("-mode")+1]
    print mode_arg

    ##########################Load a database with | Doc ID | Geometry | table#####################
    if mode_arg.lower() == "loaddb":
        import LoadDBV1 as loadDB
        print "Starting DB Load Process"

        if '-tf' in args:
            f = args[args.index("-tf")+1]
        elif '-df' in args:
            f = args[args.index("-df")+1]
        elif '-tstf' in args:
            f = args[args.index("-tstf")+1]
            
        try:
            ptbl = args[args.index("-dtbl")+1]
        except:
            print "You did not provide a name for a document table to output"
            sys.exit("Error")
            

        try:
            conn = args[args.index('-conn')+1]
        except:
            print "Problem parsing the connection information provided"
            sys.exit("Error")


    if "-help" in args:
        print "TopoCluster Run Modes:"
        print "-mode loadDB"
        print "-mode morans"
        print ""
        print "======================"
        print "=======Example========"
        print "======================"
        print "python TopoCluster.py -mode loadDB -tf /directory/trainfile.txt -dtbl document_table -conn \"dbname=someDB user=userID host='localhost'\" "
        
    

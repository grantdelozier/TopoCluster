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
            traintype = args[args.index("-traintype")+1]
        except:
            print "You did not provide a training file type (wiki or twitter)"
            sys.exit("Error")

        try:
            dtbl = args[args.index("-dtbl")+1]
        except:
            print "You did not provide a name for a document table to output"
            sys.exit("Error")
            

        try:
            conn = args[args.index('-conn')+1]
        except:
            print "Problem parsing the connection information provided"
            sys.exit("Error")

        loadDB.Load(f, dtbl, conn, traintype.lower())

    ##################Perform Moran's Calculations#################
    if mode_arg.lower() == "calc_morans":
        import MoransV1 as morans
        print "Starting Morans Coef. Calculation"
        
        #Train file, dev file, or tst file
        if '-tf' in args:
            f = args[args.index("-tf")+1]
        elif '-df' in args:
            f = args[args.index("-df")+1]
        elif '-tstf' in args:
            f = args[args.index("-tstf")+1]
            
        #Train type should be wiki or twitter
        try:
            traintype = args[args.index("-traintype")+1]
        except:
            print "You did not provide a training type for your train file (e.g. wiki or twitter)"
            sys.exit("Error")

        #Document table
        try:
            dtbl = args[args.index("-dtbl")+1]
        except:
            print "You did not provide a name for the input document table"
            sys.exit("Error")

        #Pointgrid table
        try:
            gtbl = args[args.index('-gtbl')+1]
        except:
            print "You did not provide a grid table argument"
            sys.exit("Error")         

        #DB connection Information
        #(in the future switch to a .config file)
        try:
            conn = args[args.index('-conn')+1]
        except:
            print "Problem parsing the connection information provided"
            sys.exit("Error")

        #Distance at which to aggregate documents
        try:
            agg_dist = args[args.index('-agg_dist')+1]
        except:
            print "Did not provide a bandwidth size for the agg_dist (aggregate distance) kernel"
            sys.exit("Error")

        #Distance that determines adjacency relation for Moran's weights
        try:
            kern_dist = args[args.index('-kern_dist')+1]
        except:
            print "Did not provide a bandwidth size for the kern_dist (Morans kernel distance) kernel"
            sys.exit("Error")

        #Out file with Moran's scores
        try:
            outf = args[args.index('-outf')+1]
        except:
            print "You did not provide an outfile for where the moran's scores will be written"
            sys.exit("Error")

        #Write aggregated language models option
        #Defaults to false if not provided
        try:
            if "-use_agg_lm" in args and args[args.index('-use_agg_lm')+1] != False:
                use_agg_lm = True
            else: use_agg_lm = False
        except:
            print "You did not specify whether you wanted to use an aggregated file, defaulting to False"
            use_agg_lm = False

        #Use aggregated LM
        #Defaults to false if not provided
        try:
            if "-write_agg_lm" in args and args[args.index('-write_agg_lm')+1] != "False":
                write_agg_lm = True
            else: write_agg_lm = False
        except:
            print "You did not provide a write aggregate option, defaulting to false"
            write_agg_lm = False

        #Write file for aggregated LM
        #Defaults to "tmp.txt" if not provided
        try:
            write_agg_file = args[args.index('-write_agg_file')+1]
        except:
            print "You did not provide a write aggregate outfile option, defaulting to tmp.txt"
            write_agg_file = "tmp.txt"

        try:
            sig_test = args[args.index('-sig_test')+1]
            if str(sig_test).lower() != "false":
                sig_test = True
        except:
            print "You did not provide a write aggregate outfile option, defaulting to tmp.txt"
            sig_test = False

        try:
            neighbor_ref_file = args[args.index('-nieghbor_file')+1]
        except:
            print "You did not provide a write aggregate outfile option, defaulting to tmp.txt"
            neighbor_ref_file = "None"

        try:
            mean_method = args[args.index('-mean_method')+1]
        except:
            print "Did not provide a mean_method argument, defaulting to 'appears'"
            mean_method = "appears"

        morans.calc(f, dtbl, gtbl, conn, outf, agg_dist, kern_dist, traintype.lower(), write_agg_lm, use_agg_lm, write_agg_file, sig_test, neighbor_ref_file, mean_method)


    if "-help" in args:
        print "TopoCluster Run Modes:"
        print "-mode loadDB"
        print "-mode calc_morans"
        print ""
        print "======================"
        print "=======Example========"
        print "======================"
        print "python TopoCluster.py -mode loadDB -tf /directory/trainfile.txt -dtbl document_table -conn \"dbname=someDB user=userID host='localhost'\" "
        
    

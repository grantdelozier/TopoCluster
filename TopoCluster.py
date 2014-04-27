import sys

if len(sys.argv) >= 3:
    print sys.argv
    args = sys.argv

    mode_arg = args[args.index("-mode")+1]
    print mode_arg

    ##########################Local Spatial Stastics Mode (Gi*, Zavg)#######################
    if mode_arg.lower() == "local_stats":
        import LocalSpatialStatsV1 as LSS
        print "Starting Local Spatial Statistics"

        #Trainfile / devfile / test file
        if '-tf' in args:
            f = args[args.index("-tf")+1]
        elif '-df' in args:
            f = args[args.index("-df")+1]
        elif '-tstf' in args:
            f = args[args.index("-tstf")+1]

        #Document Type: (wiki, twitter)
        try:
            traintype = args[args.index("-traintype")+1]
        except:
            print "You did not provide a training file type (wiki or twitter)"
            sys.exit("Error")

        #Document table name
        try:
            dtbl = args[args.index("-dtbl")+1]
        except:
            print "You did not provide a name for a document table to output"
            sys.exit("Error")

        #Pointgrid table name
        try:
            gtbl = args[args.index('-gtbl')+1]
        except:
            print "You did not provide a grid table argument"
            sys.exit("Error") 

        #Postgresql connection information
        try:
            conn_info = args[args.index('-conn')+1]
        except:
            print "Problem parsing the connection information provided"
            sys.exit("Error")

        #Which Statistic is going to be calculated
        try:
            statistic = args[args.index('-statistic')+1]
        except:
            print "You did not provide a statistic type"
            print "e.g. -statistic gi -statistic zavg -statistic gi,zavg"
            sys.exit("Error")

        #Kernel Bandwidth Distance for local stats
        try:
            kern_dist = args[args.index('-kern_dist')+1]
        except:
            print "Did not provide a bandwidth size for the kern_dist kernel"
            sys.exit("Error")

        #Out file with local stats
        try:
            outf = args[args.index('-outf')+1]
        except:
            print "You did not provide an outfile name for where scores will be written"
            outf = "tmp.txt"
            #sys.exit("Error")

        try:
            grid_min = args[args.index('-grid_freq_min')+1]
        except:
            print "Did not provide a grid_freq_min argument... defaulting to 1"
            grid_min = 0

        try:
            out_tbl = args[args.index('-out_tbl')+1]
        except:
            print "Did not provide a valid out table name"
            out_tbl = "localspatstat_tmp"

        #The number of cores you want to devote to multiprocessed Calcs
        try:
            cores = int(args[args.index('-cores')+1])
        except:
            print "Did not provide a -cores argument, defaulting to 1"
            cores = 1

        #Do you want to specify a whitelist to use for words (restircted vs any)
        try:
            listuse = args[args.index('-listuse')+1]
        except:
            print "Did not provide a valid listuse option, defaultign to any"
            listuse = "any"


        #Whitelist File path
        try:
            whitelist_file = args[args.index('-whitelist_file')+1]
        except:
            print "Did not provide a -whitelist_file option, defaulting to none"
            whitelist_file = "none"
            listuse = "any"

        #Kerntype for kernel function (uniform, linear, epanech)
        try:
            kerntype = args[args.index('-kerntype')+1]
        except:
            print "Did not provide a valid kerntype option, defaulting to uniform"
            kerntype = "uniform"


        LSS.calc(f, statistic, dtbl, gtbl, conn_info, outf, out_tbl, kern_dist, kerntype, traintype, listuse, whitelist_file, grid_min, cores)
        

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

        #Significance testing for Moran's Coef values (true, false)
        try:
            sig_test = args[args.index('-sig_test')+1]
            if str(sig_test).lower() != "false":
                sig_test = True
            else: sig_test = False
        except:
            print "You did not provide a significance test option, defaulting to false"
            sig_test = False

        #neighbor reference file mode, meant to make calculations independent of DB connection. Not yet implemented
        try:
            neighbor_ref_file = args[args.index('-nieghbor_file')+1]
        except:
            print "You did not provide a neighbor reference file, defaulting to None"
            neighbor_ref_file = "None"

        #Calculate means and moran's scores using only grid cells where a word is observed (appears, all)
        try:
            mean_method = args[args.index('-mean_method')+1]
        except:
            print "Did not provide a mean_method argument, defaulting to 'appears'"
            mean_method = "appears"

        try:
            grid_min = args[args.index('-grid_freq_min')+1]
        except:
            print "Did not provide a grid_freq_min argument... defaulting to 1"
            grid_min = 0

        #The number of iterations to perform on Monte Carlo Significance Simulation
        try:
            iterations = int(args[args.index('-iterations')+1])
        except:
            print "Did not provide a -iterations argument, defaulting to 0"
            iterations = 0

        #The number of cores you want to devote to multiprocessed significance testing
        try:
            cores = int(args[args.index('-cores')+1])
        except:
            print "Did not provide a -cores argument, defaulting to 1"
            cores = 1

        morans.calc(f, dtbl, gtbl, conn, outf, agg_dist, kern_dist, traintype.lower(), write_agg_lm, use_agg_lm, write_agg_file, sig_test, neighbor_ref_file, mean_method, int(grid_min), iterations, cores)


    if "--help" in args:
        print "TopoCluster Run Modes:"
        print "-mode loadDB"
        print "-mode calc_morans"
        print ""
        print "======================"
        print "=======Example========"
        print "======================"
        print "python TopoCluster.py -mode loadDB -tf /directory/trainfile.txt -dtbl document_table -conn \"dbname=someDB user=userID host='localhost'\" "
        
    

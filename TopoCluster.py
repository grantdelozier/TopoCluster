import sys
import os

PROJECT_ROOT = os.path.dirname(__file__)
sys.path.append(os.path.join(PROJECT_ROOT,"scripts"))
sys.path.append(os.path.join(PROJECT_ROOT,"experimental"))

if len(sys.argv) >= 3 and "--help" not in sys.argv:
    print sys.argv
    args = sys.argv

    mode_arg = args[args.index("-mode")+1]
    print mode_arg

    ####################Prepare a Table for visualization in a GIS###############
    if mode_arg.lower() == "prep_vis":
        import Visualize_Simple as Viz

        #Comma delimited listing of words to add together for Viz output
        #Example: -words Washington,Seahawks
        try:
            words = args[args.index('-words')+1]
        except: 
            print "You did not provide any words to visualize"
            sys.exit("Error")

        #Pointgrid table name
        #Example: globalgrid_5_clip_geog
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

        #Output Viz Table
        try:
            out_tbl = args[args.index('-out_tbl')+1]
        except:
            print "Did not provide a valid out table name"
            out_tbl = "localspatstat_tmp"

        #Statistics Table (Zavg/Gi*)
        try:
            stat_tbl = args[args.index("-stat_tbl")+1]
        except:
            print "You did not provide a name for a statistics table to use"
            sys.exit("Error")

        Viz.calc(words, gtbl, stat_tbl, out_tbl, conn_info)

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

        #PROCESS ID (short term solution to multiprocess problems)
        #try:
        #    procid = args[args.index("-procid")+1]
        #except:
        #    print "You did not provide process id"
        #    sys.exit("Error")

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

        #Should probabilities of zero be written to tbl? (yes for similarity scores, no for Top Resolver)
        try:
            include_zero = args[args.index('-include_zero')+1]
            if include_zero.lower() == "false":
                include_zero = False
            else: include_zero = True
        except:
            print "Did not provide include zero argument, defaulting to True"
            include_zero = True


        LSS.calc(f, statistic, dtbl, gtbl, conn_info, outf, out_tbl, kern_dist, kerntype, traintype, listuse, whitelist_file, grid_min, cores, include_zero)
        

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

    ################Weighted Jaccard Similarity##################
    if mode_arg.lower() == "jsim":
        print "Starting Weighted Jaccard Similarity Tests"
        import JacSimilarity as JS

        #Statistics Table (Zavg/Gi*)
        try:
            stat_tbl = args[args.index("-stat_tbl")+1]
        except:
            print "You did not provide a name for a statistics table to use"
            sys.exit("Error")

        #Statistics Table (Zavg/Gi*)
        try:
            stat_tbl_func = args[args.index("-stat_tbl_func")+1]
        except:
            print "You did not provide a name for a statistics table to use"
            sys.exit("Error")

        #Synset file
        try:
            synfile = args[args.index("-synfile")+1]
        except:
            print "You did not provide a file name to retrieve synsets from"
            sys.exit("Error")

        #Out file with p values
        try:
            outf = args[args.index('-outf')+1]
        except:
            print "You did not provide an outfile name for where scores will be written"
            sys.exit("Error")

        #Postgresql connection information
        try:
            conn_info = args[args.index('-conn')+1]
        except:
            print "Problem parsing the connection information provided"
            sys.exit("Error")

        #How many random word comparisons to make in building similarity score distribution
        #Used to derive P-value for given pair
        try:
            randits = int(args[args.index('-randits')+1])
        except:
            print "Did not provide -randits argument, defaulting to 100"
            randits = 100

        #Select a certain percentile of the obs to compare... not yet implemented
        try:
            pct = args[args.index('-pct')+1]
        except:
            pct = "Not Implemented"

        JS.calc(stat_tbl, synfile, conn_info, pct, randits, outf, stat_tbl_func)

   ###################Test Toponym Resolver on XML###################
   if mode_arg.lower() = "test_xml":
    try:
        try:
            in_domain_stat_tbl = args[args.index("-indomain_stat_tbl")+1]
        except:
            print "You did not provide a name for an in domain statistics table to use"
            print "Defaulting to None"
            in_domain_stat_tbl = "None"
            #sys.exit("Error")
            
        #Statistics Table (Zavg/Gi*) for out of domain statistics
        try:
            out_domain_stat_tbl = args[args.index("-outdomain_stat_tbl")+1]
        except:
            print "You did not provide a name for an out of domain statistics table to use"
            print "Defaulting to None"
            out_domain_stat_tbl = "None"
            if in_domain_stat_tbl == "None" and out_domain_stat_tbl == "None":
                print "Error:"
                print "You provided neither an in domain or out of domain stat table"
                sys.exit("Error")       
        #Lambda weight applied to Gi* vectors from the in domain statistics table
        try:
            in_domain_lambda = args[args.index("-in_domain_lambda")+1]
        except:
            print ""
            print "You did not provide a value for in domain lambda, defaulting to 0.0"
            print ""
            in_domain_lambda = 0.0

        try:
            out_domain_lambda = args[args.index("-out_domain_lambda")+1]
        except:
            print ""
            print "You did not provide a value for out domain lambda, defaulting to 0.0"
            print ""
            out_domain_lambda = 0.0
            if float(in_domain_lambda) == 0.0 and float(out_domain_lambda) == 0.0:
                print "Error:"
                print "A value of 0.0 was provided for both -in_domain_lambda and -out_domain_lambda"
                sys.exit("Error")

        #tst file
        try: 
            f = args[args.index("-tstf")+1]
        except:
            print "Error:"
            print "test file directory not given (-tstf)"

        #Postgresql connection information
        try:
            conn_info = args[args.index('-conn')+1]
        except:
            print "Problem parsing the connection information provided"
            sys.exit("Error")

        import TestResolver_XML

        TestResolver_XML.calc(in_domain_stat_tbl, out_domain_stat_tbl, f, conn_info)

   ###################Test Toponym Resolver#####################
   #Only used to test annotated datasets of TRCONLL, LGL, CWar
    if mode_arg.lower() == "topo_test":

        #The domain of texts that will be tested
        #
        try:
            which_test = args[args.index("-test_domain")+1]

            if which_test.strip().lower() == "lgl":
                import TestResolver_LGL as tstr
            if which_test.strip().lower() == "trconll":
                import TestResolver_TRCONLL as tstr
            if which_test.strip().lower() == "cwar":
                import TestResolver_CWar as tstr
        except:
            print "-mode topo_test requires you to specify an addition -test_domain argument"
            print "current options allow for -test_domain <lgl, trconll, cwar>"
            print sys.exit("Error: exiting. See above")
        
        #print "Starting test of topo resolver on TRConll"

        #Statistics Table (Zavg/Gi*) for in domain statistics
        try:
            in_domain_stat_tbl = args[args.index("-indomain_stat_tbl")+1]
        except:
            print "You did not provide a name for an in domain statistics table to use"
            print "Defaulting to None"
            in_domain_stat_tbl = "None"
            #sys.exit("Error")

        #Statistics Table (Zavg/Gi*) for out of domain statistics
        try:
            out_domain_stat_tbl = args[args.index("-outdomain_stat_tbl")+1]
        except:
            print "You did not provide a name for an out of domain statistics table to use"
            print "Defaulting to None"
            out_domain_stat_tbl = "None"
            if in_domain_stat_tbl == "None" and out_domain_stat_tbl == "None":
                print "Error:"
                print "You provided neither an in domain or out of domain stat table"
                sys.exit("Error")

        #Lambda weight applied to Gi* vectors from the in domain statistics table
        try:
            in_domain_lambda = args[args.index("-in_domain_lambda")+1]
        except:
            print ""
            print "You did not provide a value for in domain lambda, defaulting to 0.0"
            print ""
            in_domain_lambda = 0.0

        try:
            out_domain_lambda = args[args.index("-out_domain_lambda")+1]
        except:
            print ""
            print "You did not provide a value for out domain lambda, defaulting to 0.0"
            print ""
            out_domain_lambda = 0.0
            if float(in_domain_lambda) == 0.0 and float(out_domain_lambda) == 0.0:
                print "Error:"
                print "A value of 0.0 was provided for both -in_domain_lambda and -out_domain_lambda"
                sys.exit("Error")

        #Train file, dev file, or tst file
        if '-tf' in args:
            f = args[args.index("-tf")+1]
        elif '-df' in args:
            f = args[args.index("-df")+1]
        elif '-tstf' in args:
            f = args[args.index("-tstf")+1]

        #Postgresql connection information
        try:
            conn_info = args[args.index('-conn')+1]
        except:
            print "Problem parsing the connection information provided"
            sys.exit("Error")

        #Pointgrid table name
        try:
            gtbl = args[args.index('-gtbl')+1]
        except:
            print "You did not provide a grid table argument"
            sys.exit("Error")

        #context window size
        try:
            window = int(args[args.index('-window')+1])
        except:
            print "You did not provide a window argument, defaulting to 15"
            window = 15

        #percentile of selection
        try:
            percentile = float(args[args.index('-percentile')+1])
        except:
            print "You did not provide a window argument, defaulting to .5"
            percentile = .5

        #Weight applied to Gi* Vector of main toponym being evaluated
        try:
            main_topo_weight = float(args[args.index('-main_topo_weight')+1])
        except:
            print "You did not provide a main topo weight, defaulting to 10"
            main_topo_weight = 10.0

        #Weight applied to Gi* Vector of other toponyms in context
        try:
            other_topo_weight = float(args[args.index('-other_topo_weight')+1])
        except:
            print "You did not provide an other topo weight, defaulting to 3"
            other_topo_weight = 3.0

        #Weight applied to Gi* Vector of other toponyms in context
        try:
            other_word_weight = float(args[args.index('-other_word_weight')+1])
        except:
            print "You did not provide an other word weight, defaulting to 1"
            other_word_weight = 1.0


        #Test Table Name
        try:
            tst_tbl = args[args.index('-tst_tbl')+1]
        except:
            print "You did not provide a test table argument"
            sys.exit("Error")

        #Country Table Name
        try:
            country_tbl = args[args.index('-country_tbl')+1]
        except:
            print "You did not provide a country table argument"
            sys.exit("Error")

        #Region Table Name
        try:
            region_tbl = args[args.index('-region_tbl')+1]
        except:
            print "You did not provide a region table argument"
            sys.exit("Error")

        #State Table Name
        try:
            state_tbl = args[args.index('-state_tbl')+1]
        except:
            print "You did not provide a region table argument"
            sys.exit("Error")

        #Geonames Table Name
        try:
            geonames_tbl = args[args.index('-geonames_tbl')+1]
        except:
            print "You did not provide a geonames table argument"
            sys.exit("Error")

        #US Prominent Table Name
        #try:
        #    us_prom_tbl = args[args.index('-us_prom_tbl')+1]
        #except:
        #    print "You did not provide a prominent US city table argument"
        #    sys.exit("Error")

        #US Prominent Table Name
        #try:
        #    world_prom_tbl = args[args.index('-world_prom_tbl')+1]
        #except:
        #    print "You did not provide a prominent WORLD city table argument"
        #    sys.exit("Error")

        #Geonames Table Name
        try:
            results_file = args[args.index('-results_file')+1]
        except:
            print "You did not provide a results file name, defaulting to TestResults.txt"
            results_file = "TestResults.txt"

        tstr.calc(in_domain_stat_tbl, out_domain_stat_tbl , f, conn_info, gtbl, window, percentile, 
            float(main_topo_weight), float(other_topo_weight), float(other_word_weight), country_tbl, region_tbl,
             state_tbl, geonames_tbl, tst_tbl, float(in_domain_lambda), float(out_domain_lambda), results_file)

    ####################################################################################################################################
    ###################Test Toponym Resolver Using NER##################################################################################
    ####################################################################################################################################

    if mode_arg.lower() == "topo_test_ner":
        import TestResolverV4_NER as tstr
        print "Starting test of topo resolver on TRConll"

        #Statistics Table (Zavg/Gi*)
        try:
            stat_tbl = args[args.index("-stat_tbl")+1]
        except:
            print "You did not provide a name for a statistics table to use"
            sys.exit("Error")

        #Path containing stanford NER jar file
        try:
            stan_path = args[args.index("-stan_path")+1]
        except:
            print "You did not a directory for the stanford NER jar"
            sys.exit("Error")

        #Train file, dev file, or tst file
        if '-tf' in args:
            f = args[args.index("-tf")+1]
        elif '-df' in args:
            f = args[args.index("-df")+1]
        elif '-tstf' in args:
            f = args[args.index("-tstf")+1]

        #Postgresql connection information
        try:
            conn_info = args[args.index('-conn')+1]
        except:
            print "Problem parsing the connection information provided"
            sys.exit("Error")

        #Pointgrid table name
        try:
            gtbl = args[args.index('-gtbl')+1]
        except:
            print "You did not provide a grid table argument"
            sys.exit("Error")

        #context window size
        try:
            window = int(args[args.index('-window')+1])
        except:
            print "You did not provide a window argument, defaulting to 15"
            window = 15

        #percentile of selection
        try:
            percentile = float(args[args.index('-percentile')+1])
        except:
            print "You did not provide a window argument, defaulting to .5"
            percentile = .5

        #percentile of selection
        try:
            place_name_weight = float(args[args.index('-place_name_weight')+1])
        except:
            print "You did not provide a window argument, defaulting to 10"
            place_name_weight = 10.0

        #Test Table Name
        try:
            tst_tbl = args[args.index('-tst_tbl')+1]
        except:
            print "You did not provide a test table argument"
            sys.exit("Error")

        #Country Table Name
        try:
            country_tbl = args[args.index('-country_tbl')+1]
        except:
            print "You did not provide a country table argument"
            sys.exit("Error")

        #Region Table Name
        try:
            region_tbl = args[args.index('-region_tbl')+1]
        except:
            print "You did not provide a region table argument"
            sys.exit("Error")

        #State Table Name
        try:
            state_tbl = args[args.index('-state_tbl')+1]
        except:
            print "You did not provide a region table argument"
            sys.exit("Error")

        #Geonames Table Name
        try:
            geonames_tbl = args[args.index('-geonames_tbl')+1]
        except:
            print "You did not provide a geonames table argument"
            sys.exit("Error")

        #US Prominent Table Name
        #try:
        #    us_prom_tbl = args[args.index('-us_prom_tbl')+1]
        #except:
        #    print "You did not provide a prominent US city table argument"
        #    sys.exit("Error")

        #US Prominent Table Name
        #try:
        #    world_prom_tbl = args[args.index('-world_prom_tbl')+1]
        #except:
        #    print "You did not provide a prominent WORLD city table argument"
        #    sys.exit("Error")
        

        tstr.calc(stat_tbl , f, conn_info, gtbl, window, percentile, place_name_weight, country_tbl, region_tbl, state_tbl, geonames_tbl, tst_tbl, stan_path)

    if mode_arg.lower() == "plain_topo_resolve":
        import TestResolver_PlainText_NER as tr
        #print "Starting test of topo resolver on TRConll"

        #Path containing stanford NER jar file
        try:
            stan_path = args[args.index("-stan_path")+1]
        except:
            print "You did not a directory for the stanford NER jar"
            sys.exit("Error")

        #Statistics Table (Zavg/Gi*)
        try:
            in_domain_stat_tbl = args[args.index("-indomain_stat_tbl")+1]
        except:
            print "You did not provide a name for an in domain statistics table to use"
            print "Defaulting to None"
            in_domain_stat_tbl = "None"
            #sys.exit("Error")

        try:
            out_domain_stat_tbl = args[args.index("-outdomain_stat_tbl")+1]
        except:
            print "You did not provide a name for an out of domain statistics table to use"
            print "Defaulting to None"
            out_domain_stat_tbl = "None"
            if in_domain_stat_tbl == "None" and out_domain_stat_tbl == "None":
                print "Error:"
                print "You provided neither an in domain or out of domain stat table"
                sys.exit("Error")

        #Lambda weight applied to Gi* vectors from the in domain statistics table
        try:
            in_domain_lambda = args[args.index("-in_domain_lambda")+1]
        except:
            print ""
            print "You did not provide a value for in domain lambda, defaulting to 0.0"
            print ""
            in_domain_lambda = 0.0

        try:
            out_domain_lambda = args[args.index("-out_domain_lambda")+1]
        except:
            print ""
            print "You did not provide a value for out domain lambda, defaulting to 0.0"
            print ""
            out_domain_lambda = 0.0
            if float(in_domain_lambda) == 0.0 and float(out_domain_lambda) == 0.0:
                print "Error:"
                print "A value of 0.0 was provided for both -in_domain_lambda and -out_domain_lambda"
                sys.exit("Error")

        #Train file, dev file, or tst file
        if '-tf' in args:
            f = args[args.index("-tf")+1]
        elif '-df' in args:
            f = args[args.index("-df")+1]
        elif '-tstf' in args:
            f = args[args.index("-tstf")+1]

        #Postgresql connection information
        try:
            conn_info = args[args.index('-conn')+1]
        except:
            print "Problem parsing the connection information provided"
            sys.exit("Error")

        #Pointgrid table name
        try:
            gtbl = args[args.index('-gtbl')+1]
        except:
            print "You did not provide a grid table argument"
            sys.exit("Error")

        #context window size
        try:
            window = int(args[args.index('-window')+1])
        except:
            print "You did not provide a window argument, defaulting to 15"
            window = 15

        #percentile of selection
        try:
            percentile = float(args[args.index('-percentile')+1])
        except:
            print "You did not provide a window argument, defaulting to .5"
            percentile = .5

        #Weight applied to Gi* Vector of main toponym being evaluated
        try:
            main_topo_weight = float(args[args.index('-main_topo_weight')+1])
        except:
            print "You did not provide a main topo weight, defaulting to 10"
            main_topo_weight = 10.0

        #Weight applied to Gi* Vector of other toponyms in context
        try:
            other_topo_weight = float(args[args.index('-other_topo_weight')+1])
        except:
            print "You did not provide an other topo weight, defaulting to 3"
            other_topo_weight = 3.0

        #Weight applied to Gi* Vector of other toponyms in context
        try:
            other_word_weight = float(args[args.index('-other_word_weight')+1])
        except:
            print "You did not provide an other word weight, defaulting to 1"
            other_word_weight = 1.0

        #Country Table Name
        try:
            country_tbl = args[args.index('-country_tbl')+1]
        except:
            print "You did not provide a country table argument"
            sys.exit("Error")

        #Region Table Name
        try:
            region_tbl = args[args.index('-region_tbl')+1]
        except:
            print "You did not provide a region table argument"
            sys.exit("Error")

        #State Table Name
        try:
            state_tbl = args[args.index('-state_tbl')+1]
        except:
            print "You did not provide a region table argument"
            sys.exit("Error")

        #Geonames Table Name
        try:
            geonames_tbl = args[args.index('-geonames_tbl')+1]
        except:
            print "You did not provide a geonames table argument"
            sys.exit("Error")

        #US Prominent Table Name
        #try:
        #    us_prom_tbl = args[args.index('-us_prom_tbl')+1]
        #except:
        #    print "You did not provide a prominent US city table argument"
        #    sys.exit("Error")

        #US Prominent Table Name
        #try:
        #    world_prom_tbl = args[args.index('-world_prom_tbl')+1]
        #except:
        #    print "You did not provide a prominent WORLD city table argument"
        #    sys.exit("Error")

        #Geonames Table Name
        try:
            results_file = args[args.index('-results_file')+1]
        except:
            print "You did not provide a results file name, defaulting to TestResults.txt"
            results_file = "TestResults.txt"

        tr.calc(in_domain_stat_tbl, out_domain_stat_tbl, f, conn_info, gtbl, window, percentile, float(main_topo_weight), float(other_topo_weight), float(other_word_weight), country_tbl, region_tbl, state_tbl, geonames_tbl, float(in_domain_lambda), float(out_domain_lambda), results_file, stan_path)




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
else:
    print "TopoCluster Run Modes:"
    print "-mode loadDB"
    print "-mode calc_morans"
    print "-mode plain_topo_resolve"
    print "-mode topo_test"
    print "-mode topo_test_ner"
    print "-mode local_stats"
    print "========================"
    print "Please visit https://github.com/grantdelozier/TopoCluster/blob/master/README.md for more information"  

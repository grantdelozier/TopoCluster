import psycopg2

###Main Method###
def calc(words, gtbl, stat_tbl, out_tbl, conn_info):
    print "Local Spatial Statistics Parameters"
    print "Words to add: ", words
    print "Grid Table Name: ", gtbl
    print "Connection Information: ", conn_info
    print "Vis ready table output: ", out_tbl
    print "Local Statistic Table Name: ", stat_tbl

    word_list = []
    word_insert = ""
    if ',' in words:
    	word_list = words.strip().split(',')
    	for i in words.strip().split(','):
    		word_insert = word_insert + i 
    else: 
    	word_list.append(words.strip())
    	word_insert = words.strip()

    conn = psycopg2.connect(conn_info)
    print "DB Connection Success"

    cur = conn.cursor()

    SQL_TableSetup = "CREATE TABLE IF NOT EXISTS %s (gid varchar(20) primary key, word text, stat float, latit float, longit float, geog Geography(Point, 4326)) ;" % (out_tbl)

    cur.execute(SQL_TableSetup)

    SQL_InsertZeros = "INSERT INTO %s (SELECT p1.gid, %s, 0.0, ST_Y(p1.geog::geometry), ST_X(p1.geog::geometry), p1.geog FROM %s as p1);" % (out_tbl, '%s', gtbl)
    cur.execute(SQL_InsertZeros, (word_insert, ))

    for word in word_list:
    	SQL_Update = "UPDATE %s SET stat = %s.stat + p1.stat FROM %s as p1 WHERE %s.gid = p1.gid and p1.word = %s;" % (out_tbl, out_tbl, stat_tbl, out_tbl, '%s')
    	cur.execute(SQL_Update, (word, ))

    conn.commit()
    print "Done Adding Word Vectors"
    print "Check results in ", out_tbl



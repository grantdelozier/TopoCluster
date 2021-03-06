import sys

#sys.path.append('/home/02608/grantdel/pythonlib/lib/python2.7/site-packages')
import psycopg2
import datetime
import io

class Document:

    userID = ""
    userLat = ""
    userLong = ""

    fileFrom = ""

    Feature_Freq = {}
    total_words = 0
    #Feature_Prob = {}

    def __init__(self, ID, latit, longit, file_from):
        self.userID = ID
        self.userLat = latit
        self.userLong = longit
        #self.Feature_Freq = F_Freq
        self.fileFrom = file_from
        
    

def Load(tf, tbl_name, conn_info, traintype):

    #Connecting to Database
    conn = psycopg2.connect(conn_info)
    print "DB Connection Success"

    trainFile = tf
    
    #openTrain = codecs.open(trainFile, 'r', encoding='utf-8')
    filename = trainFile[trainFile.rfind('/')+1:]
    print filename
    begin_time = datetime.datetime.now()
    docList = []
    x = 0
    y = 0

    read_time_begin = datetime.datetime.now()
    z = 0
    
    with io.open(trainFile, 'r', encoding='utf-8') as f:
        for person in f:
            x += 1
            #print "####NEW Doucment####"
            #print userID, latit, longit
            try:
                row = person.strip().split('\t')
                if traintype == "twitter":
                    #print row[0]
                    userID = row[0]
                    latit = row[1].split(',')[1]
                    longit = row[1].split(',')[0]
                elif traintype == "wiki":
                    userID = row[0]
                    page_name = row[1]
                    latit = row[2].split(',')[1]
                    longit = row[2].split(',')[0]
                    #F_Freq = row[9]
                newDocument = Document(userID, latit, longit, filename)
                docList.append(newDocument)
            except:
                print "@@@@@error reading user@@@@@@"
                print row
                print z
                #break
            z += 1
            if z % 5000 == 0:
                print z
                print datetime.datetime.now()

    print "------Done reading in the data-------"
    read_time_end = datetime.datetime.now()
    print read_time_end - read_time_begin

    cur = conn.cursor()

    cur.execute("CREATE TABLE IF NOT EXISTS %s (gid varchar(20) primary key, latit float, longit float, geog Geography(Point, 4326), filefrom varchar(50));" % (tbl_name, ))

    cur.execute("DELETE FROM %s" % tbl_name)

    print "Inserting points to dtbl"
   
    for p in docList:
        SQL1 = "INSERT INTO %s VALUES (%s, %s, %s, ST_GeographyFromText('SRID=4326;POINT(%s %s)'), %s);" % (tbl_name, '%s', '%s', '%s', '%s', '%s', '%s')
        data = (p.userID, float(p.userLat), float(p.userLong), float(p.userLong), float(p.userLat), p.fileFrom)
        cur.execute(SQL1, data)


    cur.execute("CREATE INDEX %s_gist ON %s USING gist(geog);" % (tbl_name, tbl_name))

    conn.commit()
    conn.close()

    print "Number of documents/points loaded: ", len(docList)

    print "Done Loading ", tbl_name

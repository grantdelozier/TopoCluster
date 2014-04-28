import psycopg2
import datetime


#Epanechnikov Kernel function, values past dist threshold are zeroed
def Epanech(ptbl, pid, dist, cur, pointgrid="None"):
    #print "Beginning Epanechinov Kernel"
    if pointgrid=="None":
        cur.execute("SELECT p2.uid, .75 * (1 - power((ST_Distance(p1.geog, p2.geog)/%s), 2)) FROM %s as p1, %s as p2 WHERE p1.uid = %s and ST_DWithin(p1.geog, p2.geog, %s); " % (dist, ptbl, ptbl, '%s', dist), (pid, ) )
    else:
        cur.execute("SELECT p1.uid, .75 * (1 - power((ST_Distance(p1.geog, p2.geog)/%s), 2)) FROM %s as p1, %s as p2 WHERE p2.gid = %s and (ST_Distance(p1.geog, p2.geog) < %s);" % (dist, ptbl, pointgrid, '%s', dist), (pid, ))
    return cur.fetchall()

#Quartic Biweight Kernel function, values past dist threshold are zeroed
def Quartic_biweight(ptbl, pid, dist, cur, pointgrid="None"):
    #print "Beginning Quartic-biweight Kernel"
    if pointgrid=="None":
        cur.execute("SELECT p2.uid, power(1 - power((ST_Distance(p1.geog, p2.geog)/%s), 2), 2) FROM %s as p1, %s as p2 WHERE p1.uid = %s and ST_DWithin(p1.geog, p2.geog, %s); " % (dist, ptbl, ptbl, '%s', dist), (pid, ) )
    else:
        cur.execute("SELECT %s.uid, power(1 - power((ST_Distance_Sphere(%s.geom, %s.coord)/%s), 2), 2) FROM %s, %s WHERE %s.id = %s and (ST_Distance_Sphere(%s.geom, %s.coord) > %s);" % (ptbl, pointgrid, ptbl, dist, ptbl, pointgrid, pointgrid, '%s', pointgrid, ptbl, dist), (pid, ))
    return cur.fetchall()

#Uniform kernel function, values past dist threshold are zeroed
def Uniform(ptbl, pid, dist, cur, pointgrid="None"):
    #print "Beginning Uniform Kernel"
    if pointgrid=="None":
        cur.execute("SELECT p2.uid, 1.0 FROM %s as p1, %s as p2 WHERE p1.uid = %s and ST_DWithin(p1.geog, p2.geog, %s); " % (ptbl, ptbl, '%s', dist), (pid, ) )
    elif pointgrid=="Only":
        cur.execute("SELECT p2.gid, 1.0 FROM %s as p1, %s as p2 WHERE p1.gid = %s and ST_DWithin(p1.geog, p2.geog, %s); " % (ptbl, ptbl, '%s', dist), (pid, ) )
    else:
        cur.execute("SELECT p2.uid, 1.0 FROM %s as p1, %s as p2 WHERE p1.gid = %s and ST_DWithin(p1.geog, p2.geog, %s); " % (pointgrid, ptbl, '%s', dist), (pid, ) )
    return cur.fetchall()


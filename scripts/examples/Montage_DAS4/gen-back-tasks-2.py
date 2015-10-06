import os
import sys

images = {}
center = {}
parameters = {}
imagetbl=sys.argv[1]
corrtbl =sys.argv[2]
binary = "/home/costache/Montage_v3.3/bin/mBackground"
mount = "/local/costache/memfs2"

f = open(imagetbl, "r")
line = f.readline()
line = f.readline()
line = f.readline()
while True:
    line = f.readline()
    if not line:
        break

    parselist = line.split()
    images[int(parselist[0])+1]=parselist[23]

g = open(corrtbl, "r")
line = g.readline()
line = g.readline()
line = g.readline()
while True:
    line = g.readline()
    if not line:
        break
    parselist = line.split()
    par = parselist[1] + " " +parselist[2] + " " + parselist[3]
    parameters[parselist[0]]=par
    inf = images[int(parselist[0])]
    basename = os.path.basename(inf)
    bbasename = basename.split('.')[0]
    outf = mount+"/projdir/"+basename
    outff = mount+"/projdir/"+bbasename+"_area.fits"
    out = mount+"/corrdir/"+basename
    print "exec=%s" % binary
    print "params=%s %s %s" % (outf, out, par)
    print "inputs=%s %s/corrections.tbl %s %s" % (imagetbl, mount, outf, outff)
    print "outputs=%s" % (out) 

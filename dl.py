import urllib.request
import argparse
import time
import urllib
import threading
from queue import Queue
import multiprocessing
import os
import gzip
import tarfile
import io
lock = threading.Lock()
def downloadItem(theCount):
   fn = ("games"+str(theCount)+".tar.gz") 
   # urllib.request.urlretrieve("https://s3.amazonaws.com/lczero/training/games"+str(theCount)+".tar.gz", os.path.join(os.getcwd()+"/game/", fn))
   # tar = tarfile.open(os.path.join(os.getcwd()+"/game/", fn), "r:gz")
   response = io.BytesIO(urllib.request.urlopen("https://s3.amazonaws.com/lczero/training/games"+str(theCount)+".tar.gz").read())
   tar = tarfile.open(mode="r:gz",fileobj=response)
   for tarinfo in tar:
        #f = tar.extractfile(member)
        if tarinfo.isreg():
#            mpHandle(tar,tarinfo)
            exect=tar.extractfile(tarinfo.name)
            f = gzip.open(os.getcwd()+'/pack/'+tarinfo.name+'.gz','wb')
            f.write(exect.read())
            f.close()
   tar.close()

#def eAnds(tar,tarinfo):
#    exect=tar.extractfile(tarinfo.name)
#    f = gzip.open(os.getcwd()+'/pack/'+tarinfo.name+'.gz','wb')
#    f.write(exect.read())
#    f.close()

#def mpHandle(a,b):
#    q = multiprocessing.Queue()
#    #p = multiprocessing.Pool(1)
#    p=multiprocessing.Process(target=eAnds(a,b), args=(q,))
#    p.start()

def worker():
    while True:
        item = q.get()
        downloadItem(item)
        q.task_done()

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--start', dest='start')
parser.add_argument('--end', dest='end')
parser.add_argument('--t', dest='threads')
args = parser.parse_args()
start= args.start
end= args.end
count = start
q = Queue()
numThreads = int(args.threads)
for i in range(numThreads):
    t = threading.Thread(target=worker)
    t.daemon = True  # thread dies when main thread (only non-daemon thread) exits.
    t.start()

start = time.perf_counter()
while (int(count) <= int(end)):
        q.put(count)
        count = int(count) + int(10000)
q.join()       # block until all tasks are done

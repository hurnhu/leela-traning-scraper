import urllib.request
import argparse
import time
import urllib
import threading
from multiprocessing import Process, Queue
import random
import os
import gzip
import tarfile
import io
lock = threading.Lock()
def downloadItem(theCount):
   #t3 = Process(target=worker2,args=(q2,))
   #t3.start()
   #t3 = Process(target=worker2,args=(q2,))
   #t3.start()
   print(str(theCount))
   dateToUse = time.strftime("%Y%m%d");
   if date is not None:
      dateToUse = date
   fn = ("games"+str(theCount)+".tar.gz") 
   # urllib.request.urlretrieve("https://s3.amazonaws.com/lczero/training/games"+str(theCount)+".tar.gz", os.path.join(os.getcwd()+"/game/", fn))
   # tar = tarfile.open(os.path.join(os.getcwd()+"/game/", fn), "r:gz")
   print("http://data.lczero.org/files/training-" + str(time.strftime("%Y%m%d")) + "-" + str(theCount).zfill(2) + "17.tar")
   downloaded = 0
   chunkSize = 4096
   urlObj = urllib.request.urlopen("http://data.lczero.org/files/training-" + str(time.strftime("%Y%m%d")) + "-" + str(theCount).zfill(2) + "17.tar")
   length = urlObj.getheader('content-length')
   buf = io.BytesIO()
   size = 0
   count = 0
   now = time.time()
   while True:
       buf1 = urlObj.read(chunkSize)
       if not buf1:
           break
       buf.write(buf1)
       size += len(buf1)
#       print(count)
       #print(millis - int(round(time.time() * 1000)))
       if (int(now - time.time()) % 60 > 2):
           now = time.time()
           print(str('{:.6f} /training-' + str(time.strftime("%Y%m%d")) + '-' + str(theCount).zfill(2) + '17.tar ').format(int(size)/int(length)), end='')
           print('------')

#   response = io.BytesIO(urllib.request.urlopen("http://data.lczero.org/files/training-" + str(time.strftime("%Y%m%d")) + "-" + str(theCount).zfill(2) + "17.tar").read())
   print('downloaded')
   tar = tarfile.open(mode="r",fileobj=response)
   for tarinfo in tar:
        #f = tar.extractfile(member)
        if tarinfo.isreg():
             data = (tar,tarinfo)
             q2.put(data)  
#            mpHandle(tar,tarinfo)
#            exect=tar.extract(tarinfo.name, os.getcwd()+'/pack/'+tarinfo.name)
#            f = tarfile.open(os.getcwd()+'/pack/'+tarinfo.name+'.tar','wb')
#            f.write(exect.read())
#            f.close()
   tar.close()
   print('closed')
   #q2.close()
   #q2.join_thread()


def extractIt(data):
    data[0].extract(data[1].name, os.getcwd()+'/pack/'+data[1].name)
    print('writing data')


def eAnds(tar,tarinfo):
    exect=tar.extractfile(tarinfo.name)
    f = gzip.open(os.getcwd()+'/pack/'+tarinfo.name+'.gz','wb')
    f.write(exect.read())
    f.close()

def worker2(tq):
    keepRun = True

    while keepRun:
        item = tq.get()
        if item is None:
            print('done!')
    #        keepRun = False
            break
        extractIt(item)
        tq.task_done()


def worker(tq):
    keepRun = True

    while True:
        item = tq.get()
        if item is None:
   #         keepRun = False
            break
        downloadItem(item)
        tq.task_done()

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--start', dest='start')
parser.add_argument('--end', dest='end')
parser.add_argument('--date', dest='date')
parser.add_argument('--t', dest='threads')
args = parser.parse_args()
start= args.start
end= args.end
count = start
date = args.date
q = Queue()
q2 = Queue()
numThreads = int(args.threads)
for i in range(numThreads):
    t = Process(target=worker,args=(q,))
    t.start()

start = time.perf_counter()
while (int(count) <= int(end)):
        q.put(count)
        count = int(count) + 2

t3 = Process(target=worker2,args=(q2,))
t3.start()
t3 = Process(target=worker2,args=(q2,))
t3.start()

q.close()
q.join_thread()
q2.close()
q2.join_thread()
t.join()

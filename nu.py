import Queue

from stuff import Process


PROC_COUNT = 3


if __name__ == '__main__':

  # init
  queues = []
  for i in range(0, PROC_COUNT):
    q = Queue.Queue()
    queues.append(q)
  procs = []
  for i in range(0, PROC_COUNT):
    p = Process(i, queues)
    procs.append(p)

  # start
  for proc in procs:
    proc.start()

  # input
  procs[0].pl.send(1, 'penis')
  while True:
    command = raw_input('Yes, master?\n')
    print 'So be it!'
    if command == 'exit':
      break
    else:
      print command

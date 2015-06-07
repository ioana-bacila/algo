import Queue

from stuff import Process


PROC_COUNT = 3


if __name__ == '__main__':

  # init
  queues = []
  procs = []
  for i in range(0, PROC_COUNT):
    q = Queue.Queue()
    queues.append(q)
    p = Process(i, queues, procs)
    procs.append(p)

  # start
  for proc in procs:
    proc.start()

  # input
  print 'Yes, master?'
  while True:
    command = raw_input()
    print 'So be it!'
    command = command.split(' ')
    if command[0] == 'k':
      pid = int(command[1])
      procs[pid].crashed = True
    elif command[0] == 'b':
      pid = int(command[1])
      message = command[2]
      procs[pid].beb.broadcast(message)
    elif command[0] == 'q':
      pid = int(command[1])
      print procs[pid].hc.proposal

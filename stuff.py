import logging
import threading
import time
import uuid


# ========================================================================
# PROCESS
# ========================================================================

class Process(threading.Thread):
  def __init__(self, pid, queues, procs):
    threading.Thread.__init__(self)
    self.setName('Process-{}'.format(pid))

    # logging
    self.logger = logging.getLogger(self.name)
    self.logger.setLevel(logging.DEBUG)
    # formatter = logging.Formatter('%(threadName)s %(message)s')
    formatter = logging.Formatter('%(message)s')
    # handler = logging.StreamHandler()
    handler = logging.FileHandler('p{}.log'.format(pid))
    handler.setFormatter(formatter)
    self.logger.addHandler(handler)

    # init params
    self.pid = pid
    self.queues = queues
    self.procs = procs

    # init defaults
    self.crashed = False

  def run(self):
    self.pl = PerfectLink(self, self.procs)
    self.pfd = PerfectFailureDetector(self, self.pl, self.procs)
    self.beb = BestEffortBroadcast(self, self.pl, self.procs)
    self.aaar = AllAllAtomicRegister(self, self.beb, self.pfd, self.pl, self.procs)

    while not self.crashed:
      if not self.queues[self.pid].empty():
        message = self.queues[self.pid].get()
        self.logger.debug('{} from {} - {}'.format(
          message['type'],
          message['src'],
          message['content'],
          message['id'])
        )
        # if we received a SEND message from X, we need to reply to X with a DELIVER message
        if message['type'] == 'SEND':
          dest = message['src']
          message = {
            'id': message['id'],
            'type': 'DELIVER',
            'src': self.pid,
            'content': message['content'],
          }
          self.queues[dest].put(message)
        elif message['type'] == 'DELIVER':
          if message['id'] not in self.pl.delivered:
            self.pl.delivered.add(message['id'])
            self.pl.deliver(message['src'], message)
      time.sleep(0.1)

# ========================================================================
# AAAR
# ========================================================================

class AllAllAtomicRegister():
  def __init__(self, process, beb, pfd, pl, procs):
    self.process = process
    self.beb = beb
    self.pfd = pfd
    self.pl = pl
    self.procs = procs

    self.val = None
    self.writing = False
    self.readlist = []
    self.regs = []
    for i in range(0, len(self.procs)):
      reg = OneAllAtomicRegister(self.process, self.beb, self.pfd, self.pl, self.procs, i)
      self.regs.append(reg)

  def write_return(self):
    self.process.logger.debug('AAAR WRITE_RETURN with value {}'.format(self.val))

  def write(self, v):
    self.val = v
    self.writing = True
    for reg in self.regs:
      reg.read()

  def read_return(self, val):
    print val

  def read(self):
    for reg in self.regs:
      reg.read()

# ========================================================================
# OAAR (Read-Impose Write-All)
# ========================================================================

class OneAllAtomicRegister():
  def __init__(self, process, beb, pfd, pl, procs, rid):
    self.process = process
    self.beb = beb
    self.pfd = pfd
    self.pl = pl
    self.procs = procs
    self.rid = rid

    self.ts = ''
    self.val = None
    self.correct = self.pfd.alive
    self.writeset = set()
    self.readval = None
    self.reading = False

  def read_return(self, pid, ts, val):
    self.process.aaar.readlist.append((ts, pid, val))
    if len(self.process.aaar.readlist) == len(self.procs):
      self.process.aaar.readlist.sort(reverse=True)
      ts, pid, val = self.process.aaar.readlist[0]
      self.process.aaar.readlist = []
      if self.process.aaar.writing == True:
        self.process.aaar.writing = False
        self.write(self.process.aaar.val)
      else:
        self.process.aaar.read_return(val)
    # print val

  def read(self):
    self.reading = True
    self.readval = self.val
    self.beb.broadcast('WRITE_{}_{}_{}'.format(self.ts, self.val, self.rid))

  def write_return(self):
    self.process.logger.debug('WRITE_RETURN with value {}'.format(self.val))
    self.process.aaar.write_return()

  def write(self, v):
    self.beb.broadcast('WRITE_{}_{}_{}'.format(str(uuid.uuid1()), v, self.rid))

# ========================================================================
# BEB
# ========================================================================

class BestEffortBroadcast():
  def __init__(self, process, pl, procs):
    self.process = process
    self.pl = pl
    self.procs = procs

  def broadcast(self, message):
    if not self.process.crashed:
      for i in range(0, len(self.procs)):
        self.pl.send(i, message)

  def deliver(self, pid, message):
    if 'WRITE_' in message['content']:
      ts = message['content'].split('_')[1]
      val = message['content'].split('_')[2]
      rid = int(message['content'].split('_')[3])
      if ts > self.procs[pid].aaar.regs[rid].ts:
        self.procs[pid].aaar.regs[rid].ts = ts
        self.procs[pid].aaar.regs[rid].val = val
        # also set the aaar value
        self.procs[pid].aaar.val = val
      self.pl.send(pid, 'ACK_{}'.format(rid))
    self.process.logger.debug('BEB_DELIVER from {} - {}'.format(pid, message['content']))

# ========================================================================
# PFD
# ========================================================================

class PerfectFailureDetector():
  def __init__(self, process, pl, procs):
    self.process = process
    self.pl = pl
    self.procs = procs

    self.alive = set([x.name for x in procs])
    self.detected = set()
    self.thread = threading.Timer(0, self.run)
    self.thread.setName('PFD-{}'.format(self.process.pid))
    self.thread.start()

  def run(self):
    while not self.process.crashed:
      for i in range(0, len(self.procs)):
        if self.procs[i].name not in self.alive and self.procs[i].name not in self.detected:
          self.detected.add(self.procs[i].name)
          self.crash(i)
        self.pl.send(i, 'HEARTBEAT_REQUEST')
      self.alive.clear()
      time.sleep(10)

  def crash(self, pid):
    # code duplicated to emulate UPON CONDITION
    for reg in self.process.aaar.regs:
      if reg.correct <= reg.writeset:
        reg.writeset = set()
        if reg.reading:
          reg.reading = False
          reg.read_return(self.process.pid, self.process.oarr.ts, reg.readval)
        else:
          reg.write_return()
    self.process.logger.debug('process {} has crashed'.format(pid))

# ========================================================================
# PL
# ========================================================================

class PerfectLink():
  def __init__(self, process, procs):
    self.process = process
    self.procs = procs

    self.delivered = set()

  def deliver(self, pid, message):
    if message['content'] == 'HEARTBEAT_REQUEST':
      self.send(message['src'], 'HEARTBEAT_REPLY')
    elif message['content'] == 'HEARTBEAT_REPLY':
      self.process.pfd.alive.add(self.procs[pid].name)
    elif 'ACK' in message['content']:
      rid = int(message['content'].split('_')[1])
      self.process.aaar.regs[rid].writeset.add(self.procs[pid].name)
      # code duplicated to emulate UPON CONDITION
      if self.process.aaar.regs[rid].correct <= self.process.aaar.regs[rid].writeset:
        self.process.aaar.regs[rid].writeset = set()
        if self.process.aaar.regs[rid].reading:
          self.process.aaar.regs[rid].reading = False
          self.process.aaar.regs[rid].read_return(self.process.pid, self.process.aaar.regs[rid].ts, self.process.aaar.regs[rid].readval)
        else:
          self.process.aaar.regs[rid].write_return()
    else:
      self.process.beb.deliver(pid, message)

  def send(self, pid, message):
    message = {
      'id': str(uuid.uuid1()),
      'type': 'SEND',
      'src': self.process.pid,
      'content': message,
    }
    self.process.queues[pid].put(message)

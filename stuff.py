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
    self.hc = HierarchicalConsensus(self, self.beb, self.pfd, self.procs)

    delay = 5
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
      delay = delay - 0.1
      if delay <= 0:
        self.hc.work()
      time.sleep(0.1)

# ========================================================================
# HIERARCHICAL CONSENSUS
# ========================================================================

class HierarchicalConsensus():
  def __init__(self, process, beb, pfd, procs):
    self.process = process
    self.beb = beb
    self.pfd = pfd
    self.procs = procs

    self.detectedranks = set()
    self.round = 0
    self.proposal = None
    self.proposer = -1
    self.delivered = [None] * len(self.procs)
    self.broadcast = False

  def work(self):
    self.proposal = str(uuid.uuid1())
    if self.round < len(self.procs):
      if self.round == self.process.pid and self.proposal != None and self.broadcast == False:
        self.broadcast = True
        self.process.beb.broadcast('DECIDED_{}'.format(self.proposal))
        self.process.logger.debug('DECIDED by {} - {}'.format(self.process.pid, self.proposal))
      if self.round in self.detectedranks or self.delivered[self.round] == True:
        self.round += 1
      time.sleep(0.1)

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
    rank = pid
    v = message['content'].split('_')[1]
    if self.process.pid < rank and self.process.pid > self.procs[pid].hc.proposer:
      self.procs[pid].hc.proposal = v
      self.procs[pid].hc.proposer = rank
      self.procs[pid].logger.debug('ADOPTED {} from {}'.format(v, self.process.pid))
    self.process.hc.delivered[rank] = True
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
      self.alive = set()
      time.sleep(5)

  def crash(self, pid):
    self.process.logger.debug('process {} has crashed'.format(pid))
    self.process.hc.detectedranks.add(pid)

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
    else:
      self.process.beb.deliver(pid, message)

  def send(self, pid, message):
    message = {
      'id': uuid.uuid4().hex,
      'type': 'SEND',
      'src': self.process.pid,
      'content': message,
    }
    self.process.queues[pid].put(message)

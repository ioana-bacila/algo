import logging
import threading
import time


class Process(threading.Thread):
  def __init__(self, pid, queues):
    threading.Thread.__init__(self)
    self.setName('Process-{}'.format(pid))

    # logging
    self.logger = logging.getLogger(self.name)
    self.logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(message)s', datefmt='%H:%M:%S')
    handler = logging.StreamHandler()
    # handler = logging.FileHandler('p{}.log'.format(pid))
    handler.setFormatter(formatter)
    self.logger.addHandler(handler)

    # init params
    self.pid = pid
    self.queues = queues

    # init defaults
    self.crashed = False
    self.q = queues[pid]
    self.pl = PerfectLink(self)

  def run(self):
    self.pl.run()




class PerfectLink():
  def __init__(self, process):
    self.process = process

    self.delivered = set()

  def run(self):
    while not self.process.crashed:
      if not self.process.q.empty():
        message = self.process.q.get()
        if message['type'] in ['SEND', 'DELIVER']:
          indent = ''
        else:
          indent = '  '
        self.process.logger.debug(indent + '{} got {} from {} - {}  #{}'.format(
          self.process.name,
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
            'src': self.process.pid,
            'content': None,
          }
          self.process.queues[dest].put(message)
        elif message['type'] == 'DELIVER':
          if message['id'] not in self.delivered:
            self.delivered.add(message['id'])
            self.deliver(message['src'], message)
      time.sleep(0.1)

  def deliver(self, pid, message):
    pass

  def send(self, pid, message):
    message = {
      'id': '{}-{}'.format(time.time(), message),
      'type': 'SEND',
      'src': self.process.pid,
      'content': message,
    }
    self.process.queues[pid].put(message)

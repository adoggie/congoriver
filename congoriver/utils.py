#coding:utf-8

import time
from threading import Thread

def singleton(cls):
  instances = {}

  def _singleton(*args, **kw):
    if cls not in instances:
      instances[cls] = cls(*args, **kw)
    return instances[cls]

  return _singleton

class TimedTask(object):
  SECOND = 1
  MINUTE = SECOND * 60

  def __init__(self, action, user_data=None, timeout=SECOND):
    self.action = action
    self.timeout = timeout
    self.start_time = 0
    self.user = user_data
    self.times = 0
    self.timer = None
    self.running = False



  def _run(self,*args):
    self.running = True
    self.start_time = time.time()
    while self.running:
      time.sleep(1)
      if time.time() - self.start_time > self.timeout:
        self.start_time = time.time()
        self.action(*args)


  def start(self):
    self.timer = Thread(target= self._run,args= (self,))
    self.timer.setDaemon(True)
    self.timer.start()

  def stop(self):
    self.start_time = 0
    self.running = False

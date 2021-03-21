#coding:utf-8

import traceback
import struct
import json
from elabs.service.bitpot import bowl
from elabs.utils.useful import object_assign

class MessageType(object):
  CongoRiver = '1000'  #

class Message(object):
  def __init__(self):
    self.type = type
    self.content = {}
    self.source = {}


  def marshall(self):
    # return struct.pack('!H',self.type)
    # return str(self.type)[:4]
    return ''

  @staticmethod
  def unmarshall( data,cls):
    m = None
    try:
      values = data
      if isinstance(data,str):
        values = json.loads(data)
      m = cls()
      object_assign(m,values,add_new=True)
    except:
      m = None
    return m


class MessageLog(Message):
  type = MessageType.CongoRiver
  def __init__(self):
    Message.__init__(self)
    self.message =''
    self.content = {}
    self.sequence = ''

  def json(self):
    return str(self.__dict__)

  def marshall(self):
    data = json.dumps(self.__dict__)
    return Message.marshall(self) + data

class MessageSTAlert(Message):
  type = MessageType.StAlert
  def __init__(self):
    Message.__init__(self)
    self.acname =''
    self.name = ''
    self.product = ''
    self.time = '' # str yyyy-mm-dd mm:hh:ss
    self.period = ''
    self.service_id = ''
    self.service_type = ''
    self.host_ip = ''
    self.level = ''    # 告警级别
    self.detail = ''   # 告警明细

    # self.host_ip = ''
  def json(self):
    return str(self.__dict__)

  def marshall(self):
    data = json.dumps(self.__dict__)
    return Message.marshall(self) + data



class MessageSTLogText(Message):
  type = MessageType.StLogText
  def __init__(self):
    Message.__init__(self)
    # level,time,service_id,service_type,st_acname,st_name,host_ip,detail
    self.st_acname =''
    self.st_name = ''
    self.st_product = ''
    self.time = '' # str yyyy-mm-dd mm:hh:ss
    self.service_id = ''
    self.service_type = ''
    self.level = ''
    self.time = ''
    self.host_ip = ''
    self.detail = ''

  def marshall(self):
    data = json.dumps(self.__dict__)
    return Message.marshall(self) + data


class MessageStPositionSignal(Message):
  type = MessageType.StPositionSignal
  def __init__(self):
    Message.__init__(self)
    self.acname =''
    self.name = ''
    self.product = ''
    self.ps = 0
    self.if_update = False
    self.start = ''
    self.end = ''
    self.close = 0
    self.time = ''
    self.host_ip = ''
    self.service_id = ''
    self.service_type = ''
    self.exc_start = ''
    self.exc_end = ''

  def marshall(self):
    product = self.product[:8] + '\0'*(8-len(self.product))
    acname = self.acname[:95] + '\0'*(95-len(self.acname))
    name = self.name[:25] + '\0'*(25-len(self.name))
    ps = struct.pack('!d',self.ps)
    stream = product + acname + name + ps
    return Message.marshall(self) + stream

  @staticmethod
  def unmarshall(bytes, cls =None):
    try:
      m = MessageStPositionSignal()
      m.product = bytes[:8].replace('\0','')
      m.acname = bytes[8:8+95].replace('\0','')
      m.name = bytes[8+95:8+95+25].replace('\0','')
      m.ps = bytes[8+95+25:8+95+25+8]
      m.ps, = struct.unpack('!d',m.ps)
    except:
      traceback.print_exc()
      m = None
    return m


class MessageStPositionSignalExt(MessageStPositionSignal):
  type = MessageType.StPositionSignalExt
  def __init__(self):
    MessageStPositionSignal.__init__(self)


  def marshall(self):
    data = json.dumps(self.__dict__)
    return Message.marshall(self) + data


class MessageStProcessCommand(Message):
  type = MessageType.StProcessCommand
  StatusQuery = 'status_query'

  def __init__(self):
    Message.__init__(self)
    self.command = ''
    self.params =  {}

  @staticmethod
  def unmarshall(bytes,cls):
    try:
      data = json.loads(bytes)
      m = MessageStProcessCommand()
      m.command = data.get('command','')
      m.params = data
    except:
      m = None
    return m

  def marshall(self):
    data = dict(command = self.command , params = self.params)
    return Message.marshall(self) + json.dumps(data)

class MessageStBase(Message):
  def __init__(self):
    Message.__init__(self)
    self.acname =''
    self.name = ''

  @property
  def id(self):
    return '{}-{}'.format(self.acname,self.name)

  @staticmethod
  def unmarshall(bytes,cls = None):
    try:
      data = json.loads(bytes)
      m = MessageStBase()
      m.acname = data.get('acname', '')
      m.name = data.get('name','')
    except:
      m = None
    return m

  def marshall(self):
    data = dict(acname=self.acname, name=self.name)
    return Message.marshall(self) + json.dumps(data)

class MessageStPlay(MessageStBase):
  type = MessageType.StPlay
  def __init__(self):
    MessageStBase.__init__(self)

class MessageStPause(MessageStBase):
  type = MessageType.StPause
  def __init__(self):
    MessageStBase.__init__(self)

class MessageStTerm(MessageStBase):
  type = MessageType.StTerm
  def __init__(self):
    MessageStBase.__init__(self)


class MessageStProcessTerm(Message):
  type = MessageType.StProcessTerm
  def __init__(self):
    Message.__init__(self)
    self.pid =''

  @staticmethod
  def unmarshall(bytes,cls):
    try:
      data = json.loads(bytes)
      m = MessageStProcessTerm()
      m.pid = data.get('pid', '')
    except:
      m = None
    return m

  def marshall(self):
    data = dict(acname=self.pid)
    return Message.marshall(self) + json.dumps(data)


class MessageStProcessStatusReport(Message):
  type = MessageType.StProcessStatusReport
  def __init__(self):
    Message.__init__(self)
    self.service_id = ''
    self.service_type = ''
    self.pid =''        # process id
    self.host_ip = ''
    self.host_time = ''
    self.start_time = ''
    self.ver = ''
    self.st_list = []

  @staticmethod
  def unmarshall(bytes,cls=None):
    try:
      data = json.loads(bytes)
      m = MessageStProcessTerm()
      m.pid = data.get('pid', '')
    except:
      m = None
    return m

  def marshall(self):
    data = dict(acname=self.pid)
    return Message.marshall(self) + json.dumps(data)



def parseMessage(raw):
  np = bowl.NetworkPayload.parse(raw)
  if not np:
    return

  msgtype = np.head[:4]

  if msgtype == MessageType.StProcessCommand:
    return MessageStProcessCommand.unmarshall(np.body)

  if msgtype == MessageStTerm.type:  # stop strategy
    return MessageStTerm.unmarshall(np.body)

  if msgtype == MessageSTMonitorReport.type:
    return Message.unmarshall(np.body,MessageSTMonitorReport)

  if msgtype == MessageType.StLogText:
    return Message.unmarshall(np.body,MessageSTLogText)

  if msgtype == MessageSTAlert.type:
    return Message.unmarshall(np.body,MessageSTAlert)

  if msgtype == MessageStPositionSignal.type:
    return MessageStPositionSignal.unmarshall(np.body)

  if msgtype == MessageStPositionSignalExt.type:
    return Message.unmarshall(np.body,MessageStPositionSignalExt)



def test():
  m = MessageStPositionSignal()
  m.acname = 'Aaa'
  m.name = 'BB'
  m.product ='A'
  m.ps = 2
  ss = m.marshall()
  m = MessageStPositionSignal.unmarshall(ss)
  print m.__dict__
  print '='*30
  print m.acname ,m.name ,m.product,m.ps

if __name__ == '__main__':
  test()




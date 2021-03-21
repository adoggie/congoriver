#coding:utf-8


import os,os.path,datetime,time,traceback,json,sys
import threading,copy
import struct
import yaml
import zmq

from .utils import singleton,TimedTask
from .bowl import NetworkPayload
from .version import VERSION

ctx = zmq.Context()

class  Topic(object):
  def __init__(self,name,url,broker,pubsub='sub',encoding='json'):
    self.name = name
    self.url = url
    self.broker = broker
    self.sock = None
    self.pubsub = pubsub
    self.encoding = encoding
    self.lock = threading.Lock()

    if pubsub == 'sub':
      self.sock = ctx.socket(zmq.SUB)
      url = NetworkPayload.for_message(head=url,encoding=self.encoding)
      self.sock.setsockopt(zmq.SUBSCRIBE, url.marshall())
    else:
      self.sock = ctx.socket(zmq.PUB)
    self.sock.connect(self.broker)


  def send_data(self,data):
    self.lock.acquire()
    try:
      # print(repr(data))
      self.sock.send( data )
    except:
      traceback.print_exc()
    self.lock.release()

@singleton
class CongoRiver(object):
  def __init__(self):
    self.confs = {}
    self.topics = {}

    self.timers = {}
    self.running = True

    cf = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml')
    if os.path.exists(cf):
      self.confs = yaml.safe_load(open(cf).read())

  def init(self,**kwargs):
    kwargs['callback_msg_recv'] = kwargs.get('callback_msg_recv', self._callback_msg_recv)
    kwargs['callback_timer'] = kwargs.get('callback_timer', self._callback_timer)
    self.confs.update(**kwargs)
    data = self.confs['data_specs']['local']
    data['start_time'] = str(datetime.datetime.now())
    data['pid'] = os.getpid()

    return self

  def init_mx(self):
    for name,topic in self.confs['topics'].items():
      url = self.format_topic_url( topic['url'] , **self.confs['data_specs']['local'])
      broker_addr = self.confs['brokers'].get(topic['broker'])
      encoding = topic.get('encoding','json')
      topic = Topic(name,url, broker_addr,topic['pubsub'],encoding)
      self.topics[name] = topic

    return self

  def _on_timer(self,timer):
    timer_name = timer.user.get('name','')
    name  = timer.user.get('topic')
    topic = self.topics[name]
    # print 'on_timer..' , timer.user

    m = self.confs['topics'][name].get('message',{})

    source = self.confs['data_specs'].get('local',{})
    if 'system_time' in source:
      source['system_time'] = str(datetime.datetime.now())

    data = {
      'type': m.get('type',''),
      'source': source,
      'content': m,
      'delta':{}
    }
    # timer_data = copy.copy(data)
    callback_timer = self.confs.get('callback_timer')
    r = True
    r = callback_timer( data ,timer_name)

    self.send_data( topic , data )
    return r

  def send_data(self,topic,data_dict,source={}):
    import pickle
    data_dict['ver'] = VERSION
    if isinstance( topic , str):
      topic = self.topics[topic]

    url = self.format_topic_url(topic.url,**source)
    raw = None
    if topic.encoding == 'json':
      raw = json.dumps(data_dict)
    elif topic.encoding == 'pickle':
      raw = pickle.dumps(data_dict,protocol=2)
      # raw = msgpack.dumps(data_dict)
    else:
      # raise "topic.encoding unusable!"
      print("Error: topic.encoding unknown - ", topic.encoding)
    np = NetworkPayload.for_message(head= url,body= raw, encoding= topic.encoding)
    topic.send_data(np.marshall())

  def _send_data(self,topic,data_dict,**kwargs):
    data_dict['ver'] = VERSION
    if isinstance( topic , str):
      topic = self.topics[topic]

    url = self.format_topic_url(topic.url,**kwargs)

    np = NetworkPayload.for_message(head= url,body= json.dumps(data_dict))
    topic.send_data(np.marshall())

  def init_timer(self):
    for name,timer_data in self.confs['timers'].items():
      if not timer_data['interval']:
        continue
      timer_data['name'] = name
      timed = TimedTask(self._on_timer,user_data= timer_data,timeout= timer_data['interval'])
      self.timers[name] = timed

  def init_logs(self):
    pass

  def open(self):
    self.init_mx()
    self.init_logs()
    self.init_timer()

    for timer in self.timers.values():
      timer.start()
    thread = threading.Thread(target=self._run)
    thread.setDaemon(True)
    thread.start()


  def close(self):
    for timer in self.timers.values():
      timer.stop()
    self.running = False

  def _run(self):
    import pickle

    poller = zmq.Poller()
    socks = []
    # print self.topics
    socket_topics = {}
    for name,topic in self.topics.items():
      if topic.pubsub == 'sub':
        socks.append(topic.sock)
        socket_topics[topic.sock] = topic
        poller.register(topic.sock, zmq.POLLIN)
    if len(socks) == 0:
      return
    while self.running:
      events = dict(poller.poll(1000))
      try:
        for sock in socks:
          if sock in events:

            message = sock.recv()

            packet = NetworkPayload.parse(message)
            if not packet:
              continue
            # print(packet.__dict__)
            body = packet.body
            callback_msg_recv = self.confs['callback_msg_recv']
            raw = None
            # print( repr(packet.encoding),repr(NetworkPayload.BODY_PICKLE) )
            if packet.encoding ==  NetworkPayload.BODY_JSON:
              raw = json.loads(body)
            if packet.encoding == NetworkPayload.BODY_PICKLE:
              if sys.version_info.major == 2:
                raw = pickle.loads(body)
              else:
                raw = pickle.loads(body,encoding='latin1')
              # print('pick data',raw)
            if raw:
              callback_msg_recv(raw,socket_topics[sock])
      except:
        traceback.print_exc()

  def get_confs(self,path=''):
    if not path:
      return self.confs
    args = path.split('/')

    itr = self.confs
    while True:
      if len(args) == 1:
        k = args[0]
        return itr[k]

      itr = itr[args[0]]
      del args[0]

    return {}

  def set_confs(self,path,value):
    args = path.split('/')
    args.append(value)

    itr = self.confs
    while True:
      if len(args) == 2:
        k,v = args
        itr[k] = v
        break
      itr = itr[args[0]]
      del args[0]

    return self

  def set_confs_local(self,**kwargs):
    data = self.confs['data_specs']['local']
    data.update(**kwargs)
    return self

  def set_confs_topic(self,name,**kwargs):
    data = self.confs['topics'].get(name,{})
    data.update(**kwargs)
    self.confs['topics'][name] = data
    return self

  def send_log(self,*args,**kwargs):
    kvs = []
    category = kwargs.get('category', '')
    level = kwargs.get('level','DEBUG')
    delta = kwargs.get('delta',{})
    source = kwargs.get('source',{})
    if kwargs.has_key('level'):
      del kwargs['level']
    if kwargs.has_key('delta'):
      del kwargs['delta']
    if kwargs.has_key('category'):
      del kwargs['category']
    if kwargs.has_key('source'):
      del kwargs['source']

    for k, v in kwargs.items():
      kvs.append('{}={}'.format(k, v))
    text = ','.join(map(lambda s: str(s), args))

    # timestr = str(datetime.datetime.now()).split('.')[0]
    detail = '{} {}'.format(  text, ' '.join(kvs))

    data = {
      'level': level,
      'detail': detail,
      'time': datetime.datetime.now(),
      'category':category
    }
    self.send_any(data,'logs','logs',source,delta)

  def send_status(self, **kwargs):
    """
     kwargs:  k1=v1,k2=v2,source={service_type='abc',..} ,
              delta={ db='sheep',table='001'}
    :param kwargs:
    :return:
    """
    delta = kwargs.get('delta', {})
    source = kwargs.get('source', {})
    if kwargs.has_key('source'):
      del kwargs['source']
    if kwargs.has_key('delta'):
      del kwargs['delta']
    self.send_any(kwargs,'status', 'status',source,delta)

  # def send_alert(self,**kwargs):
  #   delta = kwargs.get('delta', {})
  #   source = kwargs.get('source', {})
  #   if kwargs.has_key('source'):
  #     del kwargs['source']
  #   if kwargs.has_key('delta'):
  #     del kwargs['delta']
  #   self.send_message('alert', kwargs, 'alert',source,delta)

  # def send_message(self, msg_type, data_dict, topic_name,source=None,delta=None, **kwargs):
  #   self.send_any(data_dict,msg_type,topic_name,source,delta,**kwargs)

  def send_any(self,data,msg_type='any',topic_name='any',source=None,delta=None):
    source_ = self.confs['data_specs'].get('local', {})
    source_ = copy.deepcopy(source_)
    if 'system_time' in source_:
      source_['system_time'] = str(datetime.datetime.now())
    if source:
      source_.update(**source)


    if not delta:
      delta = {}
    # delta['data_pickle'] = data_pickle
    data = {
      'type': msg_type,
      'source': source_,
      'dest': {},
      'content': data,
      'delta': delta
    }

    topic = self.topics.get(topic_name)
    if not topic:
      return False

    self.send_data(topic,data,source_)


  def _send_any(self,data,topic_name='any',data_pickle=True,source=None,delta=None,**others):
    import pickle
    import base64
    source_ = self.confs['data_specs'].get('local', {})
    source_ = copy.deepcopy(source_)
    if 'system_time' in source_:
      source_['system_time'] = str(datetime.datetime.now())
    if source:
      source_.update(**source)

    encode_data = data
    if data_pickle:
      encode_data = pickle.dumps(data,protocol=3)
      encode_data = base64.b64encode(encode_data)
    if not delta:
      delta = {}
    delta['data_pickle'] = data_pickle
    data = {
      'type': 'any',
      'source': source_,
      'dest': {},
      'content': encode_data.decode(),
      'delta': delta
    }

    topic = self.topics.get(topic_name)
    if not topic:
      return False

    self.send_data(topic,data,**others)

  def _send_message(self,msg_type,msg_content, topic_name, **kwargs):
    """
      send_message('keepalive', dict(..), 'peer', service_type='mysvc', service_id ='001')

    """
    source = self.confs['data_specs'].get('local', {})
    source = copy.deepcopy(source)
    if 'system_time' in source:
      source['system_time'] = str(datetime.datetime.now())
    if kwargs.has_key('service_type'):
      source['service_type'] = kwargs['service_type']
    if kwargs.has_key('service_id'):
      source['service_id'] = kwargs['service_id']

    data = {
      'type': msg_type,
      'source': source,
      'dest':{},
      'content': msg_content
    }

    topic = self.topics.get(topic_name)
    if not topic:
      return False

    self.send_data(topic,data,**kwargs)

    # url = topic.url
    # url = self.format_topic_url(url,**kwargs)
    # np = NetworkPayload.for_message(head=url, body=json.dumps(data))
    # topic.sock.send(np.marshall())
    return True

  def format_topic_url(self,url,**kwargs):
    for k,v in kwargs.items():
      m = '{%s}'%k
      url = url.replace(m, str(v))
    return url

  def _callback_msg_recv(self,data):
    pass

  def _callback_timer(self,data,timer_name):
    return True


# def congo_init(**kwargs):
#   """
#     - callback_msg_recv (data,ctx)
#     - callback_timer( data )
#
#   :param kwargs:
#   :return:
#   """
#   cf = os.path.join( os.path.dirname(os.path.abspath(__file__)) , 'config.yaml')
#   cfgs = yaml.load( open(cf).read() )
#   cfgs.update(**kwargs)
#   return CongoRiver().init(**cfgs)

#coding:utf-8

# from elabs.congoriver.congo import CongoRiver
import  time,traceback
# from pprint import pprint
import fire

from congoriver.congo import  CongoRiver

def pprint(*args,**kwargs):
  pass

def callback_msg_recv( data,topic):
    print(data)

def callback_timer( data,timer_name):
    print(data)

def test_confs():
  cr = CongoRiver().init(callback_timer=callback_timer,
                         callback_msg_recv = callback_msg_recv,
                         new_params = [1,2,3,4]
                         )

  # pprint( cr.get_confs() )
  cr.set_confs('brokers',dict( pub ='tcp://127.0.0.1:15556',sub = 'tcp://127.0.0.1:15555'))
  cr.set_confs('brokers/peer','tcp://127.0.0.1:15556')

  cr.set_confs('brokers/pub','tcp://127.0.0.1:15556')
  cr.set_confs('topics/logs/broker','pub')
  cr.set_confs('topics/peer/broker','peer')

  cr.set_confs('data_specs/local/service_type','myservice')
  cr.set_confs('data_specs/local/service_id','1001')
  cr.set_confs('data_specs/local/name','first-service')
  cr.set_confs('data_specs/local/version','1.0.1')

  pprint( cr.get_confs('brokers') )
  pprint( cr.get_confs('timers/heartbeat') )
  cr.set_confs('timers/heartbeat/interval',5)
  pprint( cr.get_confs('timers/heartbeat/interval') )

  pprint( cr.get_confs('new_params'))
  cr.open()
  time.sleep(1000)
  cr.close()
  print('--end --')


def test_callback_heartbeat():

  def callback_timer(data,timer_name):
    # 定时器触发，此处可以扩展新的属性到心跳包
    print(data)
    data['content']['timestamp'] = int(time.time())
    print(cr.get_confs('timers/{}'.format(timer_name)))

  cr = CongoRiver().init(callback_timer=callback_timer)
  cr.set_confs('brokers/pub','tcp://127.0.0.1:15556' )   # 设置发送目的zmq地址
  cr.set_confs('data_specs/local/service_type','myservice')
  cr.set_confs('data_specs/local/service_id','1001')
  cr.set_confs('data_specs/local/name','first-service')
  cr.set_confs('data_specs/local/version','1.0.1')
  cr.set_confs('data_specs/local/ip','x.x.x.x')

  cr.set_confs('timers/heartbeat/interval',5) # 设置心跳时间

  cr.open()
  time.sleep(1000)
  cr.close()
  print('--end --')

def test_simple_heartbeat():

  cr = CongoRiver().init()
  cr.set_confs('brokers/pub','tcp://127.0.0.1:15556' )   # 设置发送目的zmq地址
  cr.set_confs_local(service_type ='myservice',service_id='1001',name='first',version='1.1',ip='x.x.x')
  cr.set_confs('timers/heartbeat/interval',3) # 设置心跳时间

  cr.open()
  time.sleep(50)
  CongoRiver().close()
  print('--end --')


def test_simple_message_log():
  cr = CongoRiver().init()
  cr.set_confs('brokers/pub', 'tcp://127.0.0.1:15556')  # 设置发送目的zmq地址
  cr.set_confs_local(service_type='myservice', service_id='1001', name='first', version='1.1', ip='x.x.x')
  cr.set_confs('timers/heartbeat/interval', 3)  # 设置心跳时间
  cr.set_confs_topic('peer', broker='pub',url='/el/svc/{service_type}/{service_id}',pubsub='pub')
  cr.set_confs_topic('logs', broker='pub',url='/el/svc/logs',pubsub='pub')
  cr.open()

  for n in range(1000):
    cr.send_message('keepalive','-book'*20,'peer',service_type='myservice',service_id='1002')
    cr.send_log( "The Log Text" ,level='INFO')
    time.sleep(1)


  time.sleep(50)
  CongoRiver().close()
  print('--end --')

# def test_simple_service_status( dest = 'tcp://127.0.0.1:15556'):
def test_simple_service_status( dest = 'tcp://192.168.20.133:15556'):
  cr = CongoRiver().init()
  cr.set_confs('brokers/pub', dest )  # 设置发送目的zmq地址
  cr.set_confs('brokers/sub', 'tcp://192.168.20.133:15555' )  # 设置发送目的zmq地址
  cr.set_confs_local(service_type='myservice', service_id='1001', name='first', version='1.1', ip='x.x.x')
  cr.set_confs('timers/heartbeat/interval', 0)  # 设置心跳时间
  # cr.set_confs_topic('topics/status/pub','pub')
  cr.open()

  for n in range(1000):
    cr.send_status(status=1,detail='-'*20)
    time.sleep(1)
    print('send status..')


  time.sleep(50)
  CongoRiver().close()
  print('--end --')

if __name__ == '__main__':
  # fire.Fire()

  # test_simple_message_log()
  test_simple_service_status()
  # test_simple_heartbeat()
  # test_confs()
#coding:utf-8

# from elabs.congoriver.congo import CongoRiver
import  time,traceback
from pprint import pprint
import fire

from congoriver.congo import  CongoRiver




def test_simple_message_recv():

  def callback_msg_recv(data,topic):
    print(data)

  cr = CongoRiver().init(callback_msg_recv=callback_msg_recv)
  cr.set_confs('brokers/sub', 'tcp://127.0.0.1:15555')
  cr.set_confs_local(service_type='myservice', service_id='1002', name='first', version='1.1', ip='x.x.x')
  cr.set_confs('timers/heartbeat/interval', 3)  # 设置心跳时间
  cr.set_confs_topic('unicast', broker='sub',url='/el/svc/myservice/1002',pubsub='sub')
  cr.set_confs_topic('broadcast', broker='sub',url='/el/svc/myservice',pubsub='sub')  # 订阅

  cr.open()

  time.sleep(50000)
  CongoRiver().close()
  print('--end --')

if __name__ == '__main__':
  test_simple_message_recv()

  # test_simple_message()
  # test_simple_heartbeat()
  # test_confs()
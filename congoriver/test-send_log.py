#coding:utf-8

# from elabs.congoriver.congo import CongoRiver
import  time,traceback
# from pprint import pprint
import fire

from congoriver.congo import  CongoRiver


def test_send_log( dest = 'tcp://127.0.0.1:15556'):
  import datetime
  cr = CongoRiver().init()
  cr.set_confs('brokers/pub', dest )  # 设置发送目的zmq地址
  # cr.set_confs('brokers/sub', 'tcp://10.0.2.15:15555' )  # 设置发送目的zmq地址
  cr.set_confs_local(service_type='myservice', service_id='1001', name='first', version='1.1', ip='x.x.x')
  cr.set_confs('timers/heartbeat/interval', 0)  # 设置心跳时间
  # cr.set_confs_topic('topics/status/broker','pub')
  cr.open()

  for n in range(1000):
    # cr.send_log('This is Log Text',level='INFO',category='A1', delta='user')
    # cr.send_status(speed=10,depth='high')
    cr.send_any(dict(id='sk-01',price=100,amount=999.23,time = datetime.datetime.now()))
    time.sleep(1)
    print('send_log..')



  time.sleep(50)
  CongoRiver().close()
  print('--end --')


def test_log_server():
  # 接收 日志 和 心跳 上报
  def callback_msg_recv(data,topic):
    print(data)
    # print(data['type'],data['source'],data['content'],data['ver'])
    # c = data['content']
    # print('--',c['level'],c['time'],c['detail'],c['category'],c['delta'])


  cr = CongoRiver().init(callback_msg_recv=callback_msg_recv)
  cr.set_confs('brokers/sub', 'tcp://127.0.0.1:15555')
  cr.set_confs_local(service_type='server', service_id='1', name='first', version='1.1', ip='x.x.x')
  cr.set_confs('timers/heartbeat/interval', 0)  # 设置心跳时间
  cr.set_confs_topic('status', broker='sub', url='/el/svc/status', pubsub='sub') # 心跳订阅
  cr.set_confs_topic('logs', broker='sub', url='/el/svc/logs', pubsub='sub') # 心跳订阅
  cr.set_confs_topic('any', broker='sub', url='/el/svc/any', pubsub='sub', encoding='pickle')  # 心跳订阅

  cr.open()

  time.sleep(50000)
  CongoRiver().close()
  print('--end --')


if __name__ == '__main__':
  fire.Fire()

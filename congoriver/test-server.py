#coding:utf-8

# from elabs.congoriver.congo import CongoRiver
import  time,traceback,json
from pprint import pprint
import fire

from congoriver.congo import  CongoRiver


def test_simple_server():
  # 接收 日志 和 心跳 上报
  def callback_msg_recv(data,topic):
    # print data['type'],data['source']['system_time'],data['source']['service_id']
    pprint( json.dumps(data) )

  cr = CongoRiver().init(callback_msg_recv=callback_msg_recv)
  cr.set_confs('brokers/sub', 'tcp://127.0.0.1:15555')
  cr.set_confs_local(service_type='server', service_id='1', name='first', version='1.1', ip='x.x.x')
  cr.set_confs('timers/heartbeat/interval', 0)  # 设置心跳时间
  cr.set_confs_topic('logs', broker='sub', url='/el/svc/logs', pubsub='sub')  # 日志订阅
  cr.set_confs_topic('heartbeat', broker='sub', url='/el/svc/hb', pubsub='sub') # 心跳订阅
  cr.set_confs_topic('status', broker='sub', url='/el/svc/status', pubsub='sub') # 心跳订阅
  cr.open()

  time.sleep(50000)
  CongoRiver().close()
  print '--end --'

if __name__ == '__main__':
  test_simple_server()

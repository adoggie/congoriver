#coding:utf-8

# from elabs.congoriver.congo import CongoRiver
import  time,traceback,datetime
# from pprint import pprint
import fire
import pymongo
from congoriver.congo import  CongoRiver,Topic


def test_send_any( dest = 'tcp://127.0.0.1:15556'):
  cr = CongoRiver().init()
  cr.set_confs('brokers/pub', dest )  # 设置发送目的zmq地址
  cr.set_confs_local(service_type='myservice', service_id='1001', name='first', version='1.1', ip='x.x.x')
  cr.set_confs('timers/heartbeat/interval', 0)  # 设置心跳时间
  cr.set_confs_topic('any',encoding='pickle')
  cr.open()

  for n in range(1000):
    data = dict(id= n ,time = datetime.datetime.now(),name='service-jetline',text='Run..')
    # cr.send_any( data )
    #
    cr.send_any(data, topic_name='any', source=dict(service_type='coww',service_id='001'),
                delta= dict( database = 'Stiller',table='Combo1') )
    time.sleep(1)
    print('send_log..')

  time.sleep(50)
  CongoRiver().close()
  print('--end --')


conn = pymongo.MongoClient(host='192.168.20.133',port=27017)

# play as logserver collector
def test_any_server():


  def callback_msg_recv(data,topic=Topic):
    print('data from:',topic.__dict__)
    print(data['type'],data['source'],data['ver'],data['delta'])

  # data['content'],\
    c = data['content']
    print(c)

    # insert into nosql db
    db = conn[data['delta']['database']]
    coll = db[data['delta']['table']]
    coll.insert_one(c)


  cr = CongoRiver().init(callback_msg_recv=callback_msg_recv)
  cr.set_confs('brokers/sub', 'tcp://127.0.0.1:15555') # subscribe
  cr.set_confs_local(service_type='server', service_id='1', name='first', version='1.1', ip='x.x.x')
  cr.set_confs('timers/heartbeat/interval', 0)  # 设置心跳时间
  cr.set_confs_topic('any', broker='sub', url='/el/svc/any', pubsub='sub',encoding='pickle') # 心跳订阅
  cr.open()

  time.sleep(50000)
  CongoRiver().close()
  print('--end --')


if __name__ == '__main__':
  fire.Fire()

#coding:utf-8

import os,os.path,datetime,time,traceback,json
import zmq
import config
import time
import fire
from congoriver.bowl import NetworkPayload

MX_PUB_ADDR = 'tcp://127.0.0.1:15556'

def heartbeat():

  topic = '/el/svc/hb'
  url = NetworkPayload.for_message(head= topic).marshall()

  ctx = zmq.Context()
  sub_sock = ctx.socket(zmq.SUB)
  sub_sock.setsockopt(zmq.SUBSCRIBE, url)   # 订阅所有品种
  sub_sock.connect(config.MX_PUB_ADDR)
  while True:
    data = sub_sock.recv()
    print data
    np = NetworkPayload.parse(data)
    if np:
      print json.loads(np.body)

if __name__ == '__main__':
  # fire.Fire()
  heartbeat()
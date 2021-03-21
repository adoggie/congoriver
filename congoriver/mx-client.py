#coding:utf-8


import zmq
import config
import time
import fire

def do_sub():
  symbol = 'A'
  ctx = zmq.Context()
  sub_sock = ctx.socket(zmq.SUB)
  # sub_sock.setsockopt(zmq.SUBSCRIBE, b"elabs/CTP_XZ/au/K/1m")  # 订阅指定品种
  # sub_sock.setsockopt(zmq.SUBSCRIBE, config.TOPIC_REDIRECT_ST.format(symbol))   # 订阅所有品种
  sub_sock.setsockopt(zmq.SUBSCRIBE, b'')   # 订阅所有品种
  sub_sock.connect(config.MX_PUB_ADDR)
  while True:
    message = sub_sock.recv_string()
    print message


def do_pub():
  ctx = zmq.Context()
  pub_sock = ctx.socket(zmq.PUB)
  # pub_sock.connect(config.MX_SUB_ADDR)
  pub_sock.connect('tcp://192.168.20.133:15556')
  # pub_sock.setsockopt(zmq.XPUB_VERBOSE, 1)
  text = 'elabs/CTP_XZ/K/1m || UR,2021-01-25 13:53:00,2021-01-25,13:53:00,1971,1971,1971,1971,52,197016,346801770.0,2021-01-25 13:53:22.420681'
  n = 1
  while True:

    pub_sock.send(str(n) + text)
    print 'wait a while..'
    time.sleep(2)
    n+=1


if __name__ == '__main__':
  fire.Fire()
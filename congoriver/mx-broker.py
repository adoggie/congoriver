#coding:utf-8

"""
https://gist.github.com/minrk/4667957
"""

import os
import string
import sys
import time
from random import randint
import zmq

import fire

MX_PUB_ADDR = "tcp://127.0.0.1:15555"
MX_SUB_ADDR = "tcp://127.0.0.1:15556"


ctx = zmq.Context()

def run(pub_addr=MX_PUB_ADDR,sub_addr = MX_SUB_ADDR):
    xpub_url = pub_addr
    xsub_url = sub_addr

    print 'Pub:', xpub_url
    print 'Sub:', xsub_url

    xpub = ctx.socket(zmq.XPUB)
    xpub.bind(xpub_url)
    xsub = ctx.socket(zmq.XSUB)
    xsub.bind(xsub_url)

    poller = zmq.Poller()
    poller.register(xpub, zmq.POLLIN)
    poller.register(xsub, zmq.POLLIN)
    while True:
        events = dict(poller.poll(1000))
        if xpub in events:
            message = xpub.recv_multipart()
            print "[BROKER] subscription message: %r" % message[0]
            xsub.send_multipart(message)
        if xsub in events:
            message = xsub.recv_multipart()
            print "publishing message: %r" % message
            xpub.send_multipart(message)


if __name__ == '__main__':
    fire.Fire()
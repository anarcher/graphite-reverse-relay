import os 
import logging
try:
    import simplejson as json
except ImportError:
    import json

import cPickle as pickle
#import pickle

from twisted.internet.protocol import Factory,Protocol
from twisted.internet import reactor, defer
from twisted.protocols.basic import LineReceiver,Int32StringReceiver
from twisted.internet.endpoints import TCP4ClientEndpoint, connectProtocol
from twisted.internet.protocol import Protocol, ClientFactory


log = logging.getLogger(__name__)

class MetricLineProtocol(Protocol):
    def lineFed(self, data):
        self.transport.write(data+"\n")

class MetricPickleReceiver(Int32StringReceiver):
    MAX_LENGTH = 2 ** 20

    def __init__(self,backend_port):
        self.send_queue = defer.DeferredQueue()

        self.backend_point = TCP4ClientEndpoint(reactor, "localhost", backend_port)
        self.backend = MetricLineProtocol()
        self.backend_defer = connectProtocol(self.backend_point,self.backend)
        self.backend_defer.addCallback(self.backendConnectionMade)


    def backendConnectionMade(self,p):
        self.send_queue.get().addCallback(self.sendLineToBackend)

    def sendLineToBackend(self,line):
        self.backend.lineFed(line)

    def connectionMade(self):
        self.peerName = self.getPeerName()
        log.info('Connection from: %s' % self.transport)

    def connectionLost(self, reason):
        log.info('Connection lost: %s(%s)' % (self.transport, reason))

    def stringReceived(self, data):
        try:
            datapoints = pickle.loads(data)
        except:
            log.info('invalid pickle received from %s, ignoring' % self.peerName)
            return

        for raw in datapoints:
            try:
                (metric, (value, timestamp)) = raw
            except Exception, e:
                log.error('Error decoding pickle: %s' % e)

            newline = "%s %s %s" % (metric,value,timestamp)
            self.send_queue.put(newline)

    def getPeerName(self):
        if hasattr(self.transport, 'getPeer'):
            peer = self.transport.getPeer()
            return "%s:%d" % (peer.host, peer.port)
        else:
            return "peer"


class Proxy(Factory):
    def __init__(self,backend_port):
        self.backend_port = backend_port

    def buildProtocol(self,addr):
        protocol = MetricPickleReceiver(self.backend_port)
        protocol.factory = self
        return protocol



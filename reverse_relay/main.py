import sys
import logging
from twisted.internet import reactor
from twisted.internet.endpoints import TCP4ServerEndpoint
from proxy import Proxy


def main():
    logging.basicConfig(level=logging.DEBUG)
    port = 5555 if len(sys.argv)<2 else int(sys.argv[1])
    backend_port = 2073 if len(sys.argv) < 3 else int(sys.argv[2])

    #endpoint
    endpoint = TCP4ServerEndpoint(reactor, port)
    endpoint.listen(Proxy(backend_port=backend_port))

    print "Starting proxy."
    reactor.run()

if __name__ == '__main__':
    main()

import json

from twisted.internet import protocol
from categorizers import all_categorizers


class SnowCat(protocol.Protocol):
    def dataReceived(self, data):
        data = json.loads(data)['data']

        for cat in all_categorizers:
            cat.run.delay(data)

        ret = json.dumps({'return': 'ok'})
        self.transport.write(ret)


class SnowCatFactory(protocol.Factory):
    def buildProtocol(self, addr):
        return SnowCat()

if __name__ == '__main__':
    port = 6767
    from twisted.internet import reactor
    reactor.listenTCP(port, SnowCatFactory())
    print 'listening on port {port}'.format(port=port)
    reactor.run()
from twisted.internet.defer import DeferredQueue, inlineCallbacks
from twisted.internet import reactor
from vumi.utils import LogFilterSite
from vumi.tests.utils import MockResource


def xml_compare(x1, x2, reporter=None):
    if x1.tag != x2.tag:
        if reporter:
            reporter('Tags do not match: %s and %s' % (x1.tag, x2.tag))
        return False
    for name, value in x1.attrib.items():
        if x2.attrib.get(name) != value:
            if reporter:
                reporter('Attributes do not match: %s=%r, %s=%r'
                         % (name, value, name, x2.attrib.get(name)))
            return False
    for name in x2.attrib:
        if name not in x1.attrib:
            if reporter:
                reporter('x2 has an attribute x1 is missing: %s'
                         % name)
            return False
    if not text_compare(x1.text, x2.text):
        if reporter:
            reporter('text: %r != %r' % (x1.text, x2.text))
        return False
    if not text_compare(x1.tail, x2.tail):
        if reporter:
            reporter('tail: %r != %r' % (x1.tail, x2.tail))
        return False
    cl1 = x1.getchildren()
    cl2 = x2.getchildren()
    if len(cl1) != len(cl2):
        if reporter:
            reporter('children length differs, %i != %i'
                     % (len(cl1), len(cl2)))
        return False
    i = 0
    for c1, c2 in zip(cl1, cl2):
        i += 1
        if not xml_compare(c1, c2, reporter=reporter):
            if reporter:
                if (len(c1) and c1[0].tag == 'name'):
                    reporter('children %i do not match: %s'
                             % (i, c1[0].text))
                else:
                    reporter('children %i do not match: %s'
                             % (i, c1.tag))
            return False
    return True


def text_compare(t1, t2):
    if not t1 and not t2:
        return True
    if t1 == '*' or t2 == '*':
        return True
    return (t1 or '').strip() == (t2 or '').strip()


class Reporter:
    def __init__(self):
        self.report = []

    def __call__(self, message):
        self.report.insert(0, message)

    def tostring(self):
        summary = ""
        for message in self.report:
            summary = summary + message + ".\n"
        return summary

class MockAllResource(MockResource):

    def render_DELETE(self, request):
        return self.handler(request)


class MockHttpServer(object):

    def __init__(self, handler=None):
        self.queue = DeferredQueue()
        self._handler = handler or self.handle_request
        self._webserver = None
        self.addr = None
        self.url = None

    def handle_request(self, request):
        self.queue.put(request)

    @inlineCallbacks
    def start(self):
        root = MockAllResource(self._handler)
        site_factory = LogFilterSite(root)
        self._webserver = yield reactor.listenTCP(
            0, site_factory, interface='127.0.0.1')
        self.addr = self._webserver.getHost()
        self.url = "http://%s:%s/" % (self.addr.host, self.addr.port)

    @inlineCallbacks
    def stop(self):
        yield self._webserver.stopListening()
        yield self._webserver.loseConnection()

from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.internet.defer import inlineCallbacks

from vumi import log

def make_resource_worker(cls, publish_func, path_key):
    resource = cls(publish_func)
    return (resource, path_key)


class MoResource(Resource):
    isLeaf = True

    def __init__(self, publish_mo_func):
        log.msg("Init IConceptMoResource")
        self.publish_mo_func = publish_mo_func

    @inlineCallbacks
    def do_render(self, request):
        try:
            yield self.publish_mo_func(request)
        except Exception as ex:
            reason = "Error processing the request: %s" % (ex.message,)
            log.msg(reason)
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error("%r" % traceback.format_exception(
                exc_type, exc_value, exc_traceback))
        request.finish()

    def render(self, request):
        self.do_render(request)
        return NOT_DONE_YET
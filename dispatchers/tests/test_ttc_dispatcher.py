from datetime import datetime

from twisted.internet.defer import inlineCallbacks

from vumi.tests.helpers import VumiTestCase, MessageHelper
from vumi.dispatchers.tests.helpers import DispatcherHelper
from vumi.dispatchers.base import BaseDispatchWorker

from tests.utils import MessageMaker
from vusion.message import DispatcherControl

from dispatchers.ttc_dispatcher import DynamicDispatchWorker


class TestDynamicDispatcherWorker(VumiTestCase, MessageMaker):

    def setUp(self):
        self.disp_helper = self.add_helper(
            DispatcherHelper(DynamicDispatchWorker))

    def get_dispatcher(self, **config_extras):
        config = {
            'dispatcher_name': 'vusion',
            'router_class': 'dispatchers.VusionMainRouter',
            'exposed_names': ['app1', 'fallback_app'],
            'keyword_mappings': {'app1': 'keyword1'},
            'rules': [{
                'app': 'app1',
                'keyword': 'keyword2',
                'to_addr': '8181'}],
            'transport_names': ['transport1', 'forward_http'],
            'transport_mappings': {
                'http_forward': 'forward_http',
                'sms': {
                    '8181': 'transport1'}},
            'fallback_application': 'fallback_app',
            'expire_routing_memory': '1',
            'middleware': [
                {'mw1': 'vumi.middleware.tests.utils.RecordingMiddleware'},
                {'mw2': 'vumi.middleware.tests.utils.RecordingMiddleware'}]
        }
        config.update(config_extras)
        self.config = config
        return self.disp_helper.get_dispatcher(config)

    def make_dispatch_control(self, **kwargs):
        control = self.mkmsg_dispatcher_control(**kwargs)
        rkey = '.'.join([self.config['dispatcher_name'], 'control'])
        return self.disp_helper.dispatch_raw(rkey, control)

    def ch(self, connector_name):
        return self.disp_helper.get_connector_helper(connector_name)

    def mk_middleware_records(self, rkey_in, rkey_out):
        records = []
        for rkey, direction in [(rkey_in, False), (rkey_out, True)]:
            endpoint, method = rkey.split('.', 1)
            mw = [[name, method, endpoint] for name in ("mw1", "mw2")]
            if direction:
                mw.reverse()
            records.extend(mw)
        return records

    def assert_inbound(self, dst_conn, src_conn, msg):
        [dst_msg] = self.disp_helper.get_dispatched_inbound(dst_conn)
        middleware_records = self.mk_middleware_records(
            src_conn + '.inbound', dst_conn + '.inbound')
        self.assertEqual(dst_msg.payload.pop('record'), middleware_records)
        self.assertEqual(msg, dst_msg)

    def assert_event(self, dst_conn, src_conn, msg):
        [dst_msg] = self.disp_helper.get_dispatched_events(dst_conn)
        middleware_records = self.mk_middleware_records(
            src_conn + '.event', dst_conn + '.event')
        self.assertEqual(dst_msg.payload.pop('record'), middleware_records)
        self.assertEqual(msg, dst_msg)

    def assert_outbound(self, dst_conn, src_conn_msg_pairs):
        dst_msgs = self.disp_helper.get_dispatched_outbound(dst_conn)
        for src_conn, msg in src_conn_msg_pairs:
            dst_msg = dst_msgs.pop(0)
            middleware_records = self.mk_middleware_records(
                src_conn + '.outbound', dst_conn + '.outbound')
            self.assertEqual(dst_msg.payload.pop('record'), middleware_records)
            self.assertEqual(msg, dst_msg)
        self.assertEqual([], dst_msgs)

    def assert_no_inbound(self, *conns):
        for conn in conns:
            self.assertEqual([], self.disp_helper.get_dispatched_inbound(conn))

    def assert_no_outbound(self, *conns):
        for conn in conns:
            self.assertEqual(
                [], self.disp_helper.get_dispatched_outbound(conn))

    def assert_no_events(self, *conns):
        for conn in conns:
            self.assertEqual([], self.disp_helper.get_dispatched_events(conn))

    @inlineCallbacks
    def test_unmatching_routing(self):
        yield self.get_dispatcher()
        msg = yield self.ch('transport1').make_dispatch_inbound(
            "keyword3")
        self.assert_inbound('fallback_app', 'transport1', msg)

    @inlineCallbacks
    def test_control_add_remove_exposed(self):
        yield self.get_dispatcher()
        
        ## there is no rule for keyword3 => inbound fallback_app
        msg_in = yield self.ch('transport1').make_dispatch_inbound(
            'keyword3 1st message')
        self.assert_inbound('fallback_app', 'transport1', msg_in)
        self.assert_no_inbound('app1')
        
        ## the rule for keyword3 is added => inbound app2
        self.disp_helper.clear_all_dispatched()
        yield self.make_dispatch_control(
            action='add_exposed',
            exposed_name='app2',
            rules=[
                {'app': 'app2', 'keyword': 'keyword2'},
                {'app': 'app2', 'keyword': 'keyword3'},
            ])
        msg_in = yield self.ch('transport1').make_dispatch_inbound(
            'keyword3 2nd messsage')
        self.assert_inbound('app2', 'transport1', msg_in)
        self.assert_no_inbound('app1')        
        
        ## the rule for keyword3 is removed => inbound fallback_app
        self.disp_helper.clear_all_dispatched()
        yield self.make_dispatch_control(
            action='remove_exposed',
            exposed_name='app2')
        msg_in = yield self.ch('transport1').make_dispatch_inbound(
            'keyword3 3rd message')
        self.assert_inbound('fallback_app', 'transport1', msg_in)
        self.assert_no_inbound('app1')

    @inlineCallbacks
    def test_control_add_exposed_append_mapping_only_once(self):
        yield self.get_dispatcher()
        yield self.make_dispatch_control(
            action='add_exposed',
            exposed_name='app2',
            rules=[
                {'app': 'app2', 'keyword': 'keyword2', 'to_addr': '8181'},
                {'app': 'app2', 'keyword': 'keyword3'},
            ])
        yield self.make_dispatch_control(
            action='add_exposed',
            exposed_name='app2',
            rules=[
                {'app': 'app2', 'keyword': 'keyword2', 'to_addr': '8181'},
                {'app': 'app2', 'keyword': 'keyword3'},
            ])
        msg_in = yield self.ch('transport1').make_dispatch_inbound(
            'keyword3')
        self.assert_inbound('app2', 'transport1', msg_in)
        self.assert_no_inbound('app1', 'fallback_app')

    @inlineCallbacks
    def test_control_add_exposed_append_mapping_replace(self):
        yield self.get_dispatcher()
        ## first message is routed to app2
        yield self.make_dispatch_control(
            action='add_exposed',
            exposed_name='app2',
            rules=[
                {'app': 'app2', 'keyword': 'keyword2', 'to_addr': '8181'},
                {'app': 'app2', 'keyword': 'keyword3'},
            ])
        msg_in = yield self.ch('transport1').make_dispatch_inbound(
            'keyword3 1nd messsage', to_addr='8282')        
        self.assert_inbound('app2', 'transport1', msg_in)
        self.assert_no_inbound('app1', 'fallback_app')

        ## second message is not matching one condition => fallback_app
        self.disp_helper.clear_all_dispatched()        
        yield self.make_dispatch_control(
            action='add_exposed',
            exposed_name='app2',
            rules=[
                {'app': 'app2', 'keyword': 'keyword2', 'to_addr': '8181'},
                {'app': 'app2', 'keyword': 'keyword3', 'to_addr': '8181'},
            ])
        msg_in = yield self.ch('transport1').make_dispatch_inbound(
            'keyword3 2nd messsage', to_addr='8282')
        self.assert_inbound('fallback_app', 'transport1', msg_in)
        self.assert_no_inbound('app1', 'app2')

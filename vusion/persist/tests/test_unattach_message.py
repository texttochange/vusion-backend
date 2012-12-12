from twisted.trial.unittest import TestCase

from vusion.persist import UnattachMessage

from tests.utils import ObjectMaker

class TestUnattachMessage(TestCase, ObjectMaker):
    
    def test_upgrade(self):
        unattach_raw = {
            'name' : 'test',
            'to': 'all participants',
            'content': 'a message',
            'type-schedule': 'fixed-time',
            'fixed-time': '2012-01-01T12:12:00'}
        
        unattach = UnattachMessage(**unattach_raw)

        self.assertTrue(unattach is not None)
        self.assertEqual(
            UnattachMessage.MODEL_VERSION,
            unattach['model-version'])
        self.assertEqual(
            ['all-participants'],
            unattach['to'])

        unattach_raw = {
            'object-type': 'unattached-message',
            'model-version': '1',
            'name' : 'test',
            'to': 'all participants',
            'content': 'a message',
            'type-schedule': 'fixed-time',
            'fixed-time': '2012-01-01T12:12:00'}
        
        unattach = UnattachMessage(**unattach_raw)

        self.assertTrue(unattach is not None)
        self.assertEqual(
            UnattachMessage.MODEL_VERSION,
            unattach['model-version'])

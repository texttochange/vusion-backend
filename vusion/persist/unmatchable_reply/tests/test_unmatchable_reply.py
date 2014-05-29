
from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.persist import UnmatchableReply


class TestUnmatchableReply(TestCase, ObjectMaker):
    
    def test_upgrade(self):
        unmatchable_reply = UnmatchableReply(**{
            'participant-phone': '+24566666',
            'to': '254-8181',
            'direction': 'incoming',
            'message-content': 'Hello',
            'timestamp': '2014-04-22T10:10:10',
        })
        self.assertEqual(
            unmatchable_reply['model-version'], UnmatchableReply.MODEL_VERSION)
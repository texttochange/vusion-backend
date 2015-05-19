import re
from math import ceil

from vusion.error import MissingField, VusionError, InvalidField, MissingData
from vusion.persist import Model
from vusion.persist.participant.participant import Participant
from vusion.const import TAG_REGEX, LABEL_REGEX
from vusion.utils import clean_phone
from vumi.log import log


class Action(Model):

    MODEL_TYPE = 'action'
    MODEL_VERSION = '2'
    
    ACTION_TYPE = None

    ##TODO add subcondition on type action
    fields = {
        'subconditions': {
            'required': False,
            'valid_conditions': lambda v: getattr(v, 'valid_conditions')(),
            },
        'condition-operator': {
            'required': False,
            'valid_value': lambda v: v['condition-operator'] in ['all-subconditions', 'any-subconditions']
            },
        'set-condition': {
            'required': True,
            'valid_value': lambda v: v['set-condition'] in [None, 'condition'],
            'required_subfield': lambda v: getattr(v, 'required_subfields')(
                v['set-condition'],
                {'condition':['subconditions', 'condition-operator']}),
            },        
        'type-action': {
            'required': True,
            'valid_value': lambda v: v['type-action'] in [
                'optin', 
                'optout',
                'reset',
                'feedback',
                'unmatching-answer',
                'tagging',
                'enrolling',
                'profiling',
                'delayed-enrolling',
                'proportional-tagging',
                'proportional-labelling',
                'remove-question',
                'remove-reminders',
                'remove-deadline',
                'offset-conditioning',
                'message-forwarding',
                'url-forwarding',
                'sms-forwarding',
                'sms-invite',
                'save-content-variable-table']},
        }

    subcondition_fields = {
        'subcondition-field': {
            'required': True,
            },
        'subcondition-operator': {
            'required': True,
            },
        'subcondition-parameter': {
            'required': True,
        }
    }
        
    subcondition_values = {
        'tagged':{
            'with': '.*',
            'not-with': '.*'},
        'labelled':{
            'with': '.*:.*',
            'not-with': '.*:.*'},
    }

    def __init__(self, **kwargs):
        kwargs.update({'type-action':self.ACTION_TYPE})
        super(Action, self).__init__(**kwargs)

    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            kwargs['set-condition'] = None
            kwargs['model-version'] = '2'
        return kwargs

    def __eq__(self, other):
        if isinstance(other, Action):
            return self.payload == other.payload
        return False

    def __str__(self):
        return "Do:%s payload=%s" % (self.get_type(), repr(self.payload))

    def __repr__(self):
        return str(self)

    def __getitem__(self, key):
        return self.payload[key]

    def __setitem__(self, key, value):
        self.payload[key] = value

    def process_fields(self, fields):
        return fields

    def validate_fields(self):
        self._validate(self, self.fields)

    def valid_conditions(self):
        if not 'subconditions' in self:
            return True
        for subcondition in self['subconditions']:
            self._validate(subcondition, self.subcondition_fields)
            self.valid_subcondition_value(subcondition)
        return True

    def valid_subcondition_value(self, subconditon):
        if not subconditon['subcondition-field'] in self.subcondition_values:
            raise InvalidField("%s=%s is not valid" % ('subcondition-field', subconditon['subcondition-field']))
        operators = self.subcondition_values[subconditon['subcondition-field']]
        if not subconditon['subcondition-operator'] in operators:
            raise InvalidField("%s=%s is not valid" % ('subcondition-operator', subconditon['subcondition-operator']))
        parameter_regex = re.compile(operators[subconditon['subcondition-operator']])
        if not parameter_regex.match(subconditon['subcondition-parameter']):
            raise InvalidField("%s=%s is not valid" % ('subcondition-parameter', subconditon['subcondition-parameter']))
        
    def get_type(self):
        return self.ACTION_TYPE

    def assert_field_present(self, *fields):
        for field in fields:
            if field not in self.payload:
                raise MissingField(field)

    def assert_subfield_present(self, field, *subfields):
        for subfield in subfields:
            if subfield not in self[field]:
                raise MissingField(subfield)

    def assert_list_field_present(self, elements, *fields):
        for element in elements:
            for field in fields:
                if field not in element:
                    raise MissingField(field)

    def get_as_dict(self):
        action_dict = {'type-action': self.get_type()}
        for key in self.payload:
            action_dict[key] = self.payload[key]
        return action_dict

    def has_condition(self):
        return self['set-condition'] == 'condition'

    def get_condition_mongodb(self):
        if not self.has_condition():
            return {}
        return Participant.from_conditions_to_query(
            self['condition-operator'], self['subconditions'])
        
    def get_condition_mongodb_for(self, phone, session_id):
        query = self.get_condition_mongodb()
        if '$and' in query:
            query['$and'].insert(0, {'phone': phone, 'session-id': session_id})
        elif '$or' in query:
            query = {'$and': [{'phone': phone,'session-id': session_id},
                              query]}
        else:
            query.update({'phone': phone,
                          'session-id': session_id})
        return query


class OptinAction(Action):

    ACTION_TYPE = 'optin'

    def validate_fields(self):
        super(OptinAction, self).validate_fields()        


class OptoutAction(Action):

    ACTION_TYPE = 'optout'

    def validate_fields(self):
        super(OptoutAction, self).validate_fields()


class ResetAction(Action):

    ACTION_TYPE = 'reset'
    
    def before_validate(self):
        if not 'keep-tags' in self.payload:
            self['keep-tags'] = None
        if not 'keep-labels' in self.payload:
            self['keep-labels'] = None        
        super(ResetAction, self).before_validate()    

    def validate_fields(self):
        super(ResetAction, self).validate_fields()
        self.assert_field_present('keep-tags', 'keep-labels')
        
    def get_keep_tags(self, participant_tags):
        tags = []
        if len(participant_tags) > 0:
            if self['keep-tags'] is not None and len(self['keep-tags']) > 0:
                for tag in participant_tags:
                    if tag in self['keep-tags']:
                        tags.append(tag)
        return tags
            
        
    def get_keep_labels(self, participant_labels):
        labels = []
        if len(participant_labels) > 0:
            if self['keep-labels'] is not None and len(self['keep-labels']) > 0:
                for participant_label in participant_labels:
                    if participant_label['label'] in self['keep-labels']:
                        labels.append(participant_label)
        return labels


class FeedbackAction(Action):

    ACTION_TYPE = 'feedback'

    def validate_fields(self):
        super(FeedbackAction, self).validate_fields()        
        self.assert_field_present('content')


class UnMatchingAnswerAction(Action):

    ACTION_TYPE = 'unmatching-answer'

    def validate_fields(self):
        super(UnMatchingAnswerAction, self).validate_fields()        
        self.assert_field_present('answer')


class TaggingAction(Action):

    ACTION_TYPE = 'tagging'

    def validate_fields(self):
        super(TaggingAction, self).validate_fields()        
        self.assert_field_present('tag')


class EnrollingAction(Action):

    ACTION_TYPE = 'enrolling'

    def validate_fields(self):
        super(EnrollingAction, self).validate_fields()        
        self.assert_field_present('enroll')


class DelayedEnrollingAction(Action):

    ACTION_TYPE = 'delayed-enrolling'

    def validate_fields(self):
        super(DelayedEnrollingAction, self).validate_fields()
        self.assert_field_present(
            'enroll',
            'offset-days')
        self.assert_subfield_present(
            'offset-days',
            'days',
            'at-time')


class ProfilingAction(Action):

    ACTION_TYPE = 'profiling'

    def validate_fields(self):
        super(ProfilingAction, self).validate_fields()
        self.assert_field_present('label', 'value')
    

class RemoveQuestionAction(Action):

    ACTION_TYPE = 'remove-question'

    def validate_fields(self):
        super(RemoveQuestionAction, self).validate_fields()
        self.assert_field_present('dialogue-id', 'interaction-id')


class RemoveRemindersAction(Action):

    ACTION_TYPE = 'remove-reminders'

    def validate_fields(self):
        super(RemoveRemindersAction, self).validate_fields()
        self.assert_field_present('dialogue-id', 'interaction-id')


class RemoveDeadlineAction(Action):

    ACTION_TYPE = 'remove-deadline'

    def validate_fields(self):
        super(RemoveDeadlineAction, self).validate_fields()        
        self.assert_field_present('dialogue-id', 'interaction-id')


class OffsetConditionAction(Action):

    ACTION_TYPE = 'offset-conditioning'

    def validate_fields(self):
        super(OffsetConditionAction, self).validate_fields()        
        self.assert_field_present('interaction-id', 'dialogue-id')


class ProportionalAction(Action):
    pass


class ProportionalTagging(ProportionalAction):
    
    ACTION_TYPE = 'proportional-tagging'
    
    def validate_fields(self):
        super(ProportionalTagging, self).validate_fields()
        self.assert_field_present('proportional-tags')
        self.assert_list_field_present(self['proportional-tags'], *['tag', 'weight'])

    def get_proportional_tags(self):
        return self['proportional-tags']
    
    def set_tag_count(self, tag, count):
        for i, proportional_tag in enumerate(self['proportional-tags']):
            if proportional_tag['tag'] == tag:
                proportional_tag.update({'count': count})
                self['proportional-tags'][i] = proportional_tag
                break
    
    def get_tags(self):
        tags = []
        for proportional_tag in self['proportional-tags']:
            tags.append(proportional_tag['tag'])
        return tags
    
    def get_totals(self):
        weight_total = 0
        count_total =0
        for proportional_tag in self['proportional-tags']:
            weight_total = weight_total + (int(proportional_tag['weight']) or 0)
            count_total = count_total + (proportional_tag['count'] if 'count' in proportional_tag else 0)
        return weight_total, count_total
    
    def get_tagging_action(self):
        weight_total, count_total = self.get_totals()
        for proportional_tag in self['proportional-tags']:
            weight_tag = int(proportional_tag['weight'])
            count_expected = ceil(count_total * weight_tag / weight_total)
            count_tag = (proportional_tag['count'] if 'count' in proportional_tag else 0)
            if count_expected >= count_tag:
                return TaggingAction(**{'tag': proportional_tag['tag']})
        return TaggingAction(**{'tag': self['proportional-tags'][0]['tag']})


class ProportionalLabelling(ProportionalAction):

    ACTION_TYPE = 'proportional-labelling'

    def validate_fields(self):
        super(ProportionalLabelling, self).validate_fields()
        self.assert_field_present('proportional-labels')
        self.assert_list_field_present(self['proportional-labels'], *['label-value', 'weight'])
        self.assert_field_present('label-name')

    def get_proportional_labels(self):
        return self['proportional-labels']

    def get_label_name(self):
        return self['label-name']

    def set_count(self, label_value, count):
        for i, proportional_label in enumerate(self['proportional-labels']):
            if proportional_label['label-value'] == label_value:
                proportional_label.update({'count': count})
                self['proportional-labels'][i] = proportional_label
                break

    def get_labels(self):
        labels = []
        for proportional_label in self['proportional-labels']:
            labels.append({
                'label': self['label-name'],
                'value': proportional_label['label-value']})
        return labels

    def get_totals(self):
        weight_total = 0
        count_total = 0
        for proportional_label in self['proportional-labels']:
            weight_total = weight_total + (int(proportional_label['weight']) or 0)
            count_total = count_total + (proportional_label['count'] if 'count' in proportional_label else 0)
        return weight_total, count_total

    def get_labelling_action(self):
        weight_total, count_total = self.get_totals()
        for proportional_label in self['proportional-labels']:
            weight_label = int(proportional_label['weight'])
            count_expected = ceil(count_total * weight_label / weight_total)
            count_label = (proportional_label['count'] if 'count' in proportional_label else 0)
            if count_expected >= count_label:
                return ProfilingAction(**{
                    'label': self['label-name'],
                    'value': proportional_label['label-value']})
        return ProfilingAction(**{
            'label': self['label-name'],
            'value': self['proportional-labels'][0]['label-value']})

class UrlForwarding(Action):
    
    ACTION_TYPE = 'url-forwarding'
    
    def validate_fields(self):
        super(UrlForwarding, self).validate_fields()
        self.assert_field_present('forward-url')
      
      
class SmsForwarding(Action):
    
    ACTION_TYPE = 'sms-forwarding'

    def before_validate(self):
        if not 'set-forward-message-condition' in self.payload:
            self['set-forward-message-condition'] = None
        super(SmsForwarding, self).before_validate()

    def validate_fields(self):
        super(SmsForwarding, self).validate_fields()
        self.assert_field_present(
            'forward-to', 
            'forward-content',
            'set-forward-message-condition')

    def get_forward_message_condition(self, context):
        if self['set-forward-message-condition'] is None:
            return {}
        if self['forward-message-condition-type'] == 'phone-number':
            second_word = context.get_message_second_word()
            if second_word is None:
                return {'phone': ''}
            return {'phone': clean_phone(second_word)}
        return {}

    def has_no_participant_feedback(self):
        if self['set-forward-message-condition'] is None:
            return False
        if ('forward-message-no-participant-feedback' not in self.payload 
            or self['forward-message-no-participant-feedback'] is None
            or self['forward-message-no-participant-feedback'] == ''):
            return False
        return True

    def get_no_participant_feedback(self):
        if 'forward-message-no-participant-feedback' in self:
            return self['forward-message-no-participant-feedback']
        return None

    def get_query_selector(self, participant, context):
        ##replace custom part of the selector
        customized_selector = (self['forward-to'] or '')
        custom_regexp = re.compile(r'\[participant.(?P<key1>[^\.\]]+)\]')
        matches = re.finditer(custom_regexp, customized_selector)
        for match in matches:
            match = match.groupdict() if match is not None else None
            if match is None:
                continue
            if participant is None:
                raise MissingData('No participant supplied for this message.')
            participant_label_value = participant.get_data(match['key1'])
            if not participant_label_value:
                raise MissingData("Participant %s doesn't have a label %s" % 
                                  (participant['phone'], match['key1']))
            replace_match = '[participant.%s]' % match['key1']
            customized_selector = self['forward-to'].replace(
                replace_match, participant_label_value) 
        selectors = [selector.strip() for selector in customized_selector.split(",")]
        ##build the query
        query = self.get_forward_message_condition(context)
        self.add_condition_to_query(query, {'session-id': {'$ne': None}})
        self.add_condition_to_query(query, {'phone': {'$ne': participant['phone']}})
        for selector in selectors:
            if re.match(TAG_REGEX, selector):
                self.add_condition_to_query(query, {'tags': selector})
            elif re.match(LABEL_REGEX, selector):
                profile = selector.split(':')
                self.add_condition_to_query(query, {'profile': {'$elemMatch': {'label': profile[0], 'value': profile[1]}}})
        return query

    def add_condition_to_query(self, query, conditions):
        for key, value in conditions.iteritems():
            if key in query:
                if '$and' in query:
                    query['$and'].append({key: value})
                else:
                    query['$and'] = [{key: query[key]}, {key: value}]
                    query.pop(key, None)
            else:
                query[key] = value


class SmsInviteAction(Action):

    ACTION_TYPE = 'sms-invite'

    def validate_fields(self):
        super(SmsInviteAction, self).validate_fields()
        self.assert_field_present(
            'invite-content',
            'invitee-tag',
            'feedback-inviter')


class SaveContentVariableTable(Action):

    ACTION_TYPE = 'save-content-variable-table'

    def validate_fields(self):
        super(SaveContentVariableTable, self).validate_fields()
        self.assert_field_present(
            'scvt-attached-table',
            'scvt-row-keys',
            'scvt-col-key-header',
            'scvt-col-extras')
        self.assert_list_field_present(
            self['scvt-row-keys'], *['scvt-row-header', 'scvt-row-value'])
        self.assert_list_field_present(
            self['scvt-col-extras'], *['scvt-col-extra-header', 'scvt-col-extra-value'])

    def get_match(self):
        i = 1
        match = {}
        for key in self['scvt-row-keys']:
            match.update({'key%s' % i: key['scvt-row-value']})
            i = i + 1
        match.update({'key%s' % i: self['scvt-col-key-header']})
        return match

    def get_extra_matchs(self):
        i = 1
        row_match = {}
        for key in self['scvt-row-keys']:
            row_match.update({'key%s' % i: key['scvt-row-value']})
            i = i + 1
        matchs = []
        for extra_cv in self['scvt-col-extras']:
            j = i
            match = row_match.copy()
            match.update({'key%s' % i: extra_cv['scvt-col-extra-header']})
            matchs.append((match, extra_cv['scvt-col-extra-value']))
        return matchs

    def get_table_id(self):
        return self['scvt-attached-table']

def action_generator(**kwargs):
    # Condition to be removed when Dialogue structure freezed
    if 'type-action' not in kwargs:
        kwargs['type-action'] = kwargs['type-answer-action']
    if kwargs['type-action'] == 'optin':
        return OptinAction(**kwargs)
    elif kwargs['type-action'] == 'optout':
        return OptoutAction(**kwargs)
    elif kwargs['type-action'] == 'reset':
        return ResetAction(**kwargs)
    elif kwargs['type-action'] == 'enrolling':
        return EnrollingAction(**kwargs)
    elif kwargs['type-action'] == 'delayed-enrolling':
        return DelayedEnrollingAction(**kwargs)
    elif kwargs['type-action'] == 'tagging':
        return TaggingAction(**kwargs)
    elif kwargs['type-action'] == 'profiling':
        return ProfilingAction(**kwargs)
    elif kwargs['type-action'] == 'feedback':
        return FeedbackAction(**kwargs)
    elif kwargs['type-action'] == 'unmatching-answer':
        return UnMatchingAnswerAction(**kwargs)
    elif kwargs['type-action'] == 'remove-reminders':
        return RemoveRemindersAction(**kwargs)
    elif kwargs['type-action'] == 'remove-deadline':
        return RemoveDeadlineAction(**kwargs)
    elif kwargs['type-action'] == 'offset-conditioning':
        return OffsetConditionAction(**kwargs)
    elif kwargs['type-action'] == 'proportional-tagging':
        return ProportionalTagging(**kwargs)
    elif kwargs['type-action'] == 'proportional-labelling':
        return ProportionalLabelling(**kwargs)
    elif kwargs['type-action'] in ['message-forwarding', 'url-forwarding']:
        return UrlForwarding(**kwargs)
    elif kwargs['type-action'] == 'sms-forwarding':
        return SmsForwarding(**kwargs)
    elif kwargs['type-action'] == 'sms-invite':
        return SmsInviteAction(**kwargs)
    elif kwargs['type-action'] == 'save-content-variable-table':
        return SaveContentVariableTable(**kwargs)
    raise VusionError("%r not supported" % kwargs)


class Actions():

    def __init__(self):
        self.actions = []

    def append(self, action):
        if action.get_type() in ["optin", "enrolling", "reset"]:
            i = 0
            if action.get_type() == "enrolling":
                i = self.get_position_after(["optin", "reset"])
            if action.get_type() == "reset":
                i = self.get_position_after(["optin"])
            self.actions.insert(i, action)
        else:
            self.actions.append(action)

    def get_position_after(self, action_types):
        if len(self.actions) == 0:
            return 0
        i = 0
        while (self.actions[i].get_type() in action_types):
            i = i + 1
            if len(self.actions) <= i:
                return i
        return i

    def extend(self, actions):
        for action in actions:
            self.append(action)

    def contains(self, action_type):
        for action in self.actions:
            if action.get_type() == action_type:
                return True
        return False

    def items(self):
        return self.actions.__iter__()

    def __getitem__(self, key):
        return self.actions[key]

    def get_priority_action(self):
        return self.actions.pop(0)

    def __len__(self):
        return len(self.actions)

    def __eq__(self, other):
        if not isinstance(other, Actions):
            return False
        if len(self.actions) != len(other.actions):
            return False
        for i in range(0, len(self)): 
            if not self.actions[i] == other.actions[i]:
                return False
        return True

    def clear_all(self):
        self.actions = []

    def keep_only_remove_action(self):
        for action in self.actions:
            if (action.get_type() != 'remove-reminders' and
                    action.get_type() != 'remove-deadline' and
                    action.get_type() != 'remove-question'):
                self.actions.remove(action)
                

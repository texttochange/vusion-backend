# -*- test-case-name: tests.test_ttc -*-

import sys

from twisted.python import log
from twisted.internet.defer import (inlineCallbacks, Deferred)
from twisted.enterprise import adbapi
from twisted.internet import task

import pymongo

from datetime import datetime, time, date, timedelta

from vumi.application import ApplicationWorker
from vumi.message import Message, TransportUserMessage
from vumi.application import SessionManager


class TtcGenericWorker(ApplicationWorker):

    def databaseAccessSuccess(self, result):
        log.msg("Databasee Access Succeed %s" %result)

    def databaseAccessFailure(self, failure):
        log.msg("Databasee Access Succeed %s" % failure)


    @inlineCallbacks
    def startWorker(self):
        self._d = Deferred()
        log.msg("One Generic Worker is starting")
        super(TtcGenericWorker, self).startWorker()
        self.control_consumer = yield self.consume(
            '%(control_name)s.control' % self.config,
            self.consume_control,
            message_class=Message)

        #config
        self.transport_name = self.config['transport_name']
        self.control_name = self.config['control_name']
        self.transport_type = 'sms'

        #some basic local recording
        self.record = []

        # Try to Access Redis
        #self.redis = SessionManager(db=0, prefix="test")

        self.sender = None
        self.program_name = None
        
        self._d.callback(None)

    def consume_user_message(self, message):
        log.msg("User message: %s" % message['content'])

    def get_current_script(self):
        for script in self.collection_scripts.find({"activated":1}).sort("modified",pymongo.DESCENDING).limit(1):
            return script['script']
        log.msg("Fatal Error: no active script found on in the database")
        

    def init_program_db(self, database_name):
        log.msg("Initialization of the program")
        self.database_name = database_name
        log.msg("Connecting to database: %s"% self.database_name)
        
        #Initilization of the database
        connection = pymongo.Connection("localhost",27017)
        self.db = connection[self.database_name]
        
        #Declare collection for retriving script
        collection_scripts_name = "scripts"
        if not(collection_scripts_name in self.db.collection_names()):
            log.msg("Error collection not initialized: %s"% collection_scripts_name)
            raise Exception("Collection error", collection_scripts_name)
        self.collection_scripts = self.db[collection_scripts_name]
        
        #Declare collection for retriving participants
        collection_participants_name = "participants"
        if not(collection_participants_name in self.db.collection_names()):
            log.msg("Error collection not initialized: %s"% collection_participants_name)
            raise Exception("Collection error", collection_participants_name)
        self.collection_participants = self.db[collection_participants_name]
        
        #Declare collection for scheduling messages
        collection_schedules_name = "schedules"
        if ( collection_schedules_name in self.db.collection_names()):
            self.collection_schedules = self.db[collection_schedules_name]
        else:
            self.collection_schedules = self.db.create_collection(collection_schedules_name)

        #Declare collection for loging messages
        collection_status_name = "status"
        if ( collection_status_name in self.db.collection_names()):
            self.collection_status = self.db[collection_status_name]
        else:
            self.collection_status = self.db.create_collection(collection_status_name)

    #@inlineCallbacks
    def consume_control(self, message):
        log.msg("Control message!")
        #data = message.payload['data']
        #self.record.append(('config',message))
        if (message.get('program')):
            program = message['program']
            log.msg("Receive a config message: %s" % program['name'])
            #MongoDB#
            self.init_program_db(program['database-name'])
            #self.db.programs.save(program)
            #if 'participants' in program:
                #self.schedule_participants_dialogue(program['participants'], 
                                                    #self.get_dialogue(program,"0"))

            #Redis#
            #self.redis.create_session("program")
            #self.redis.save_session("program", program)
            #session = self.redis.load_session("program")
            #log.msg("Message stored and retrieved %s" % session.get('name'))

            #UglyModel#
            #self.db.runInteraction(ParticipantModel.create_item,68473)        
            #name = program.get('name')
            #group = program.get('group',{})
            #number = group.get('number')
            #log.msg("Control message %s to be add %s" % (number,name))

            #Twistar#                
            #def failure(error):
                #log.msg("failure while saving %s" %error)

            #if(program.get('dialogues')):
                #yield self.saveDialoguesDB(program.get('dialogues'))
            #if(program.get('participants')):
                #yield self.saveParticipantsDB(program.get('participants'))
        
        #elif (message.get('participants')):
            #program = self.db.programs.find_one({"name":self.program_name})
            #if 'participants' in program: 
                #program['participants'] = program['participants'] + message.get('participants')
            #else:
                #program['participants'] = message.get('participants')
            #self.db.programs.save(program)
            #self.record.append(('config',message))
            #yield self.saveParticipantsDB(message.get("participants"))

        elif (message.get('action')=='resume' or message.get('action')=='start'):
            log.msg("Getting an action: "+message['action'])
            #self.init_program_db(message.get('content'))
            #reconstruct the scheduling by replaying all the program for each participant
            self.collection_schedules.remove()
            script = self.get_current_script()
            self.schedule_participants_dialogue(
                self.collection_participants.find(),
                script['dialogues'][0])
            

        #start looping process of the scheduler
        if (self.sender == None):
            self.sender = task.LoopingCall(self.send_scheduled)
            self.sender.start(30.0)


    def dispatch_event(self, message):
        log.msg("Event message!")

    #TODO: fire error feedback if the dialogue do not exit anymore
    #TODO: if dialogue is deleted, need to remove the scheduled message (or they are also canceled if cannot find the dialogue)
    @inlineCallbacks
    def send_scheduled(self):
        log.msg('Sending Scheduled message start')
        #if (self.program_name == None):
            #log.msg("Error scheduling starting but worker is not yet initialized")
            #return
        toSends = self.collection_schedules.find(spec={"datetime":{"$lt":datetime.now().isoformat()}},sort=[("datetime",1)])
        for toSend in toSends:
            self.collection_schedules.remove({"_id":toSend.get('_id')})
            program = self.get_current_script()
            try:
                interaction = self.get_interaction(program, toSend['dialogue-id'], toSend['interaction-id'])
                log.msg("Send scheduled message %s to %s" % (interaction['content'], toSend['participant-phone'])) 
                message = TransportUserMessage(**{'from_addr':program['shortcode'],
                                                  'to_addr':toSend.get('participant-phone'),
                                                  'transport_name':self.transport_name,
                                                  'transport_type':self.transport_type,
                                                  'transport_metadata':'',
                                                  'content':interaction['content']
                                                  })
                yield self.transport_publisher.publish_message(message);
                self.collection_status.save({"datetime":datetime.now().isoformat(),
                                           "message":message.payload,
                                           "participant-phone": toSend['participant-phone'],
                                           "dialogue-id": toSend['dialogue-id'],
                                           "interaction-id": toSend['interaction-id']})
            except:
                log.msg("Error while getting schedule reference, scheduled message dumpted: %s - %s" % (toSend['dialogue-id'],toSend['interaction-id']))
                log.msg("Exception is %s" % sys.exc_info()[0])
    
    #MongoDB do not support fetching a subpart of an array
    #may not be necessary in the near future
    #https://jira.mongodb.org/browse/SERVER-828
    #https://jira.mongodb.org/browse/SERVER-3089
    def get_interaction(self, program, dialogue_id, interaction_id):
        for dialogue in program['dialogues']:
            if dialogue["dialogue-id"]== dialogue_id:
                for interaction in dialogue["interactions"]:
                    if interaction["interaction-id"]==interaction_id:
                        return interaction

    def get_dialogue(self, program, dialogue_id):
        for dialogue in program['dialogues']:
            if dialogue["dialogue-id"]== dialogue_id:
                return dialogue

    def schedule_participants_dialogue(self, participants, dialogue):
        for participant in participants:
            self.schedule_participant_dialogue(participant, dialogue)


    #def find_earlyest_log(self, logs):
        #earliest_time = None
        #for log in logs:
            #if (earliest_time is None || earliest_time < log.datetime):
                #earliest_time = log.datetime
        #return earliest_time
            

    #TODO: manage multiple timezone
    #TODO: manage other schedule type
    #TODO: decide which id should be in an schedule object
    def schedule_participant_dialogue(self, participant, dialogue):
        #schedules = self.collection_schedules
        previousSendDateTime = None
        schedules = []
        for interaction in dialogue.get('interactions'):
            schedule = self.collection_schedules.find_one({
                                       "participant-phone": participant['phone'],
                                       "dialogue-id":dialogue["dialogue-id"],
                                       "interaction-id":interaction["interaction-id"]})
            if (schedule is not None): #Is the interaction already schedule
                continue
            status = self.collection_status.find_one({
                                       "participant-phone": participant['phone'],
                                       "dialogue-id":dialogue["dialogue-id"],
                                       "interaction-id":interaction["interaction-id"]},
                                            sort=[("datetime",pymongo.ASCENDING)])
            if (status is None): #Has the interaction has not been send
                try:
                    schedule = {"datetime":None, 
                                "participant-phone": participant['phone'], 
                                "dialogue-id":dialogue['dialogue-id'], 
                                "interaction-id":interaction["interaction-id"]}
                    if (interaction['type-schedule']=="immediately"):
                        sendingDateTime = datetime.now()
                    elif (interaction['type-schedule']=="wait"):
                        sendingDateTime = previousSendDateTime + timedelta(minutes=int(interaction['minutes']))
                    elif (interaction['type-schedule']=="fixed-time"):
                        sendingDateTime = datetime(int(interaction['year']),
                                                   int(interaction['month']),
                                                   int(interaction['day']),
                                                   int(interaction['hour']),
                                                   int(interaction['minute']))
                   
                   #Scheduling a date already in the past is forbidden. 
                    if (sendingDateTime < datetime.now() and 
                        datetime.now() - sendingDateTime > timedelta(minutes=10)): 
                        self.collection_status.save({"datetime": datetime.now().isoformat(),
                                                 "type": "schedule-fail",
                                                 "participant-phone": participant['phone'], 
                                                 "dialogue-id": dialogue['dialogue-id'], 
                                                 "interaction-id": interaction["interaction-id"]})
                        continue
                    
                    schedule["datetime"] = sendingDateTime.isoformat()[:19]
                    schedules.append(schedule)
                    previousSendDateTime = sendingDateTime
                    self.collection_schedules.save(schedule)
                    log.msg("Schedule has been saved: %s" % schedule);
                except:
                    log.msg("Scheduling exception with time: %s" % interaction)
                    log.msg("Exception is %s" % sys.exc_info()[0])
            else:
                previousSendDateTime = datetime.strptime(status["datetime"],"%Y-%m-%dT%H:%M:%S.%f")
        return schedules
            #schedules.save(schedule)

    #Deprecated
    @inlineCallbacks
    def saveParticipantsDB(self, participants):
        log.msg("received a list of phone")
        for participant in participants:
            oParticipant = yield Participant.find(where=['phone = ?', participant.get('phone')],limit=1)
            if (oParticipant == None):
                oParticipant = yield Participant(phone=participant.get('phone'),
                                                 name=participant.get('name')).save()
            else:
                #if (participant.get('name')):
                oParticipant.name = participant.get('name')
                yield oParticipant.save()

    #Deprecated
    @inlineCallbacks
    def saveDialoguesDB(self, dialogues):
        for dialogue in dialogues:
            #oDialogue = yield Dialogue(name=dialogue.get('name'),type=dialogue.get('type')).save().addCallback(onDialogueSave,dialogue.get('interactions')).addErrback(failure)
            oDialogue = yield Dialogue.find(where=['name = ?',dialogue.get('name')], limit=1)
            if (oDialogue==None):
                oDialogue = yield Dialogue(name=dialogue.get('name'),type=dialogue.get('type')).save().addErrback(failure)
            else:
                oDialogue.name = dialogue.get('name')
                yield oDialogue.save()
            for interaction in dialogue.get('interactions'):
                if interaction.get('type')== "announcement":
                    oInteraction = yield Interaction.find(where=['name = ?',interaction.get('name')], limit=1)
                    if (oInteraction==None):
                        oInteraction = yield Interaction(content=interaction.get('content'),
                                                         name=interaction.get('name'),
                                                         schedule_type=interaction.get('schedule-type'), 
                                                         dialogue_id=oDialogue.id).save().addErrback(failure)
                    else:
                        oInteraction.content = interaction.get('content')
                        oInteraction.name = interaction.get('name')
                        oInteraction.schedule_type=interaction.get('schedule-type')
                        yield oInteraction.save()
                    #yield Schedule(type=interaction.get("schedule_type"),
                                    #interaction_id=oInteraction.id).save()

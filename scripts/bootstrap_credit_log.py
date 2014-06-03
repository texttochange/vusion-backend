import sys
import yaml
import pymongo
sys.path.insert(0, './')

from vusion.persist import (WorkerConfigManager, HistoryManager,
                            ProgramCreditLogManager, ShortcodeManager,
                            UnmatchableReplyManager, GarbageCreditLogManager)

from vusion.utils import time_to_vusion_format_date
from vusion.component import DialogueWorkerPropertyHelper, PrintLogger

stream = open("./etc/ttc_multiworker.yaml")
config = yaml.load(stream)
vusion_database_name = config.get('vusion_database_name', 'vusion')
mongodb_host = config.get('mongodb_host', 'localhost')
mongodb_port = config.get('mongodb_port', 27017)
c = pymongo.Connection(mongodb_host, mongodb_port)
vusion_db = c[vusion_database_name]

logger = PrintLogger()

col_shortcode = ShortcodeManager(vusion_db, 'shortcodes', logger=logger)
#col_shortcode.set_log_helper(logger)
##Interate on all the programs to calculate the CreditLogs
col_worker_config = WorkerConfigManager(vusion_db, 'workers', logger=logger)
#col_worker_config.set_log_helper(logger)
for worker_config in col_worker_config.get_worker_configs():
    program_database_name = worker_config['config']['database_name']
    logger.log("Start program %s" % program_database_name)
    program_db = c[program_database_name]
    col_history = HistoryManager(program_db, 'history', logger=logger)
    col_program_setting = program_db['program_settings']
    properties = DialogueWorkerPropertyHelper(col_program_setting, col_shortcode)
    properties.load()
    if properties['shortcode'] is None:
        logger.log("Skip program %s as no shortcode defined" % program_database_name);
        continue
    col_credit_log = ProgramCreditLogManager(vusion_db, 'credit_logs', program_database_name, logger=logger)
    col_credit_log.set_property_helper(properties)
    ##get last day in this history
    current_date = col_history.get_older_date()
    while current_date is not None:
        logger.log("... do %s" % time_to_vusion_format_date(current_date))
        counters = col_history.count_day_credits(current_date)
        col_credit_log.set_counters(counters, date=current_date)
        current_date = col_history.get_older_date(current_date)

## Garbage worker
col_credit_log = GarbageCreditLogManager(vusion_db, 'credit_logs', logger=logger)
col_unmatchable_reply = UnmatchableReplyManager(vusion_db, 'unmatchable_reply', logger=logger)
for code in col_shortcode.get_shortcodes():
    code_ref = code.get_vusion_reference()
    logger.log("Start garbage unmatchable %s" % code_ref)
    #logger.log("On shortcode %s" % code_ref)
    current_date = col_unmatchable_reply.get_older_date(code=code_ref)
    while current_date is not None:
        logger.log("... do %s" % time_to_vusion_format_date(current_date))        
        counters = col_unmatchable_reply.count_day_credits(current_date, code=code_ref)
        col_credit_log.set_counters(counters, date=current_date, code=code_ref)
        current_date = col_unmatchable_reply.get_older_date(current_date, code=code_ref)
from daemon import Daemon
from pdftoolkit import PDFToolkit, PDFIntrospectException
from logging import handlers

import json
import os
import errno
import sys
import logging
import time
import ConfigParser
import datetime
import uuid
import shutil
import re
import urllib

from kombu import BrokerConnection
from Queue import Empty

import subprocess
import eventlet
from eventlet.green import urllib2

eventlet.monkey_patch()

# General config
agentConfig = {}
agentConfig['logging'] = logging.INFO
agentConfig['version'] = '0.1'

rawConfig = {}

# Config handling
try:
    path = os.path.realpath(__file__)
    path = os.path.dirname(path)

    config = ConfigParser.ConfigParser()

    if os.path.exists('/etc/pdfconverter-agent/config.cfg'):
        configPath = '/etc/pdfconverter-agent/config.cfg'     
    else:
        configPath = path + '/config.cfg'

    if os.access(configPath, os.R_OK) == False:
        print 'Unable to read the config file at ' + configPath
        print 'Agent will now quit'
        sys.exit(1)

    config.read(configPath)

    # Core config
    agentConfig['redis_host'] = config.get('Main', 'redis_host')
    agentConfig['redis_port'] = config.get('Main', 'redis_port')
    agentConfig['redis_db'] = config.get('Main', 'redis_db')
    agentConfig['queue_name'] = config.get('Main', 'queue_name')
 
    # Tmp path
    if os.path.exists('/var/log/pdfconverter-agent/'):
        agentConfig['tmpDirectory'] = '/var/log/pdfconverter-agent/'
    else:
        agentConfig['tmpDirectory'] = '/tmp/' # default which may be overriden in the config later

    agentConfig['pidfileDirectory'] = agentConfig['tmpDirectory']

    # Media path 
    media_path = config.get('Main', 'media_folder')
    if not os.path.exists(media_path):
        media_path = agentConfig['tmpDirectory']
    agentConfig['media_path'] = media_path


    if config.has_option('Main', 'logging_level'):
        # Maps log levels from the configuration file to Python log levels
        loggingLevelMapping = {
            'debug'    : logging.DEBUG,
            'info'     : logging.INFO,
            'error'    : logging.ERROR,
            'warn'     : logging.WARN,
            'warning'  : logging.WARNING,
            'critical' : logging.CRITICAL,
            'fatal'    : logging.FATAL,
        }

        customLogging = config.get('Main', 'logging_level')

        try:
            agentConfig['logging'] = loggingLevelMapping[customLogging.lower()]

        except KeyError, ex:
            agentConfig['logging'] = logging.INFO


except ConfigParser.NoSectionError, e:
    print 'Config file not found or incorrectly formatted'
    print 'Agent will now quit'
    sys.exit(1)

except ConfigParser.ParsingError, e:
    print 'Config file not found or incorrectly formatted'
    print 'Agent will now quit'
    sys.exit(1)

except ConfigParser.NoOptionError, e:
    print 'There are some items missing from your config file, but nothing fatal'

class agent(Daemon):
    """
    Agent
    """
    def run(self):  
        # Setup connection
        mainLogger.debug('Connecting to Redis on %s %s %s' % (
            agentConfig['redis_host'], agentConfig['redis_port'], agentConfig['redis_db'])
        )
        connection = BrokerConnection(
                        hostname=agentConfig['redis_host'],
                        transport="redis",
                        virtual_host=agentConfig['redis_db'],
                        port=int(agentConfig['redis_port'])
        )
        connection.connect()
        consumer = Consumer(connection)

        while True:
            try:
               consumer.consume()
            except Empty:
               mainLogger.debug('No tasks, going to sleep')
               # sleep is patched and triggers context switching
               # for eventlet
               time.sleep(1)
                
        mainLogger.debug('Waiting')
        mainLogger.debug('Done & exit')
   
 
class Consumer(object):

    def __init__(self, connection, queue_name=agentConfig['queue_name'],
            serializer="pickle", compression=None):
        self.queue = connection.SimpleQueue(queue_name)
        self.serializer = serializer
        self.compression = compression

        # Create an eventlet pool of size 10
        self.pool = eventlet.GreenPool(10)
        self.queue = connection.SimpleQueue(agentConfig['queue_name'])


    def consume(self):
        """
        Consume message
        Spawn a green thread
        """
        message = self.queue.get(block=True, timeout=1)
        mainLogger.debug("Got a task !")
        self.pool.spawn_n(self.process, message, agentConfig)
      
    def process(self, msg, cfg):
        proc = TaskProcessor(msg, cfg)
        proc.run()
   
 
    def close(self):
        self.queue.close()

class TaskProcessor(object):
    """
    Task processor
    """
    def __init__(self, msg, cfg):
        """
         Message has:
        -  file_content (binary)
        -  file_name
        -  sender
        -  upload_to (where to upload)
        """
        self.msg = msg
        self.task = msg.payload
        self.file_content = self.task['file_content']
        self.file_name = self.task['file_name']
        self.sender = self.task['sender']
        self.upload_to = self.task['upload_to']
        self.callback = self.task['callback']

        # Path were the pdf received from file_content is written
        self.base_path = cfg['media_path']

        self.callback_payload = {
            "success": False,
            "imgs_path": [],
            "nbr_pages": -1,
            "msg": "",
        }

    def _create_today_folder_on(self, bpath):
        """
        Create a folder with todays date as name on path P
        """
        today = datetime.date.today()
        todaystr = today.isoformat()
        path = "%s%s" % (bpath, todaystr)
        if not os.path.exists(path):
            try:
                os.mkdir(path)
            except OSError as exc:
                if exc.errno == errno.EEXIST:
                    pass
                else: raise
        return path

    @property
    def unique_file_name(self):
        """
        Build Unique File name 
        """
        new_file_name = "%s_%s_%s".strip() % (
            uuid.uuid4(), 
            self.sender, 
            self.file_name
        )
        return re.sub(r'\s', '', new_file_name)


    def _save_payload_file(self, path):
        """
        Write payload file to disk
        """
        self.pdf_source_path = '%s/%s' % (path, self.unique_file_name)
        pdf_file = open(self.pdf_source_path, "w", 0)
        pdf_file.writelines(self.file_content)
        pdf_file.close()

    
    def _convert_pdf_to_img(self, ext='jpg'):
        # Call convert to build large imgs
        command = "convert -colorspace RGB -quality 70 -density 120 %s %s" % (
            self.pdf_source_path, 
            self.pdf_source_path + "_large_.%s" % ext
        )
        output, error = subprocess.Popen(
                            command.split(' '), stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE).communicate()
        return output, error

    def run(self):
        # Create today's folder if needed
        path = self._create_today_folder_on(bpath=self.base_path)
        # Write file from task to disk
        self._save_payload_file(path)

        # Call convert on pdf
        output, error = self._convert_pdf_to_img()
        if error:
            self.handle_error("[Error] converting pdf to img ", error)
            return 

        # Count pages
        try:
            nbr_pages = PDFToolkit.count_pages(self.pdf_source_path)
            self.callback_payload['nbr_pages'] = nbr_pages
        except PDFIntrospectException, e:
            self.handle_error("[Error] cant' count pages ", e)
            return

        # Move created imgs to upload_to dir
        source_imgs_path = ["%s_large_-%s.jpg" % (self.pdf_source_path, i) for i in range(0, int(nbr_pages))]
        try:
            upload_path = self._create_today_folder_on(bpath=self.upload_to)
            destination_paths = []
            for source_img_path in source_imgs_path:
                # build destionation file name
                destination_img_path = "%s/%s" % (upload_path, source_img_path.split('/')[-1])
                shutil.move(source_img_path, destination_img_path)
                # append new path minus upload_to
                destination_paths.append(destination_img_path.replace(self.upload_to,""))
            self.callback_payload['imgs_path'] = destination_paths
        except OSError, e:
            self.handle_error("[Error] moving new img files %s", e)
            return
 
        # Delete source pdf
        try:
            os.remove(self.pdf_source_path)
        except OSError, e:
            self.handle_error("[Error] deleting source pdf %s", e)
            return
        
        self.do_success_callback(self.callback_payload)

        mainLogger.debug("Convertion done")
        self.msg.ack() 
        
    def handle_error(self, msg):
        mainLogger.info(msg)
        self.do_error_callback(msg=msg, data=self.callback_payload)

    def do_error_callback(self, msg, data):
        self.callback_payload["msg"] = msg
        self.do_call_back(False, self.callback_payload)

    def do_success_callback(self, data):
        self.do_call_back(True, self.callback_payload)

    def do_call_back(self, success, data):
        self.callback_payload["success"] = success
        data = urllib.urlencode(self.callback_payload)
        req = urllib2.Request(self.callback, data)
        urllib2.urlopen(req)


# Control of daemon     
if __name__ == '__main__':  

    # Logging
    logFile = os.path.join(agentConfig['tmpDirectory'], 'pdfconverter-agent.log')

    if os.access(agentConfig['tmpDirectory'], os.W_OK) == False:
        print 'Unable to write the log file at ' + logFile
        print 'Agent will now quit'
        sys.exit(1)

    handler =  handlers.RotatingFileHandler(logFile, maxBytes=10485760, backupCount=5) # 10MB files
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    handler.setFormatter(formatter)

    mainLogger = logging.getLogger('main')
    mainLogger.setLevel(agentConfig['logging']) 
    mainLogger.addHandler(handler)  

    mainLogger.info('--')
    mainLogger.info('pdfconverter-agent %s started', agentConfig['version'])
    mainLogger.info('--')

 

    argLen = len(sys.argv)

    if argLen == 3 or argLen == 4: # needs to accept case when --clean is passed
        if sys.argv[2] == 'init':
            # This path added for newer Linux packages which run under
            # a separate sd-agent user account.
            if os.path.exists('/var/run/pdfconverter-agent/'):
                pidFile = '/var/run/pdfconverter-agent/pdfconverter-agent.pid'
            else:
                pidFile = '/var/run/pdfconverter-agent.pid'

    else:
        pidFile = os.path.join(agentConfig['pidfileDirectory'], 'pdfconverter-agent.pid')

    if os.access(agentConfig['pidfileDirectory'], os.W_OK) == False:
        print 'Unable to write the PID file at ' + pidFile
        print 'Agent will now quit'
        sys.exit(1)

    mainLogger.info('PID: %s', pidFile)

    if argLen == 4 and sys.argv[3] == '--clean':
        mainLogger.info('--clean')
        try:
            os.remove(pidFile)
        except OSError:
            # Did not find pid file
            pass

    # Daemon instance from agent class
    daemon = agent(pidFile)

    # Control options
    if argLen == 2 or argLen == 3 or argLen == 4:
        if 'start' == sys.argv[1]:
            mainLogger.info('Action: start')
            daemon.start()

        elif 'stop' == sys.argv[1]:
            mainLogger.info('Action: stop')
            daemon.stop()

        elif 'restart' == sys.argv[1]:
            mainLogger.info('Action: restart')
            daemon.restart()

        elif 'foreground' == sys.argv[1]:
            mainLogger.info('Action: foreground')
            daemon.run()

        elif 'status' == sys.argv[1]:
            mainLogger.info('Action: status')

            try:
                pf = file(pidFile,'r')
                pid = int(pf.read().strip())
                pf.close()
            except IOError:
                pid = None
            except SystemExit:
                pid = None

            if pid:
                print 'pdfconverter-agent is running as pid %s.' % pid
            else:
                print 'pdfconverter-agent is not running.'

        else:
            print 'Unknown command'
            sys.exit(1)

        sys.exit(0)

    else:
        print 'usage: %s start|stop|restart|status' % sys.argv[0]
        sys.exit(1)

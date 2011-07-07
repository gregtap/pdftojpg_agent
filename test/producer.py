from socket import gethostname
from time import time

from kombu import BrokerConnection

files = ['bulletin.pdf', 'med_4p_120k.pdf', 'small_45k.pdf', 'math_11p.pdf']
#files = ['bulletin.pdf',]

connection = BrokerConnection(
                hostname='rh2.dev.novagile.fr',
                transport="redis",
                virtual_host=0,
                port=6379)

print "Connection Producer to Redis"
connection.connect()

queue = connection.SimpleQueue("pdf_to_jpg")

for f in files:
    # open as binary
    my_file = open(f, "rb")
    my_file.seek(0)
    my_file_bcontent = my_file.read()
    my_file.close()

    # Push !
    queue.put({"file_content": my_file_bcontent,
                "file_name": f,
                "hostname":  gethostname(),
                "timestamp": time()},
                serializer="pickle",
                compression=None)

    my_file.close()
    print "Pushed on queue pdf_to_jpg"

connection.close()
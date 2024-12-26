import queue
import time
import logging
import multiprocessing
from data.server import Server as DataLayerServer
from network.server import Server as NetworkLayerServer
from consensus.server import Server as ConsensusServer
from incentive.server import Server as IncentiveServer
from service.server import Server as ServiceLayer


def start_data_layer():
    server = DataLayerServer()
    server.start()
    
def start_network_layer():
    server = NetworkLayerServer()
    server.start()
    
def start_consesus_layer():
    server = ConsensusServer()
    server.start()
    
def start_incentive_server():
    server = IncentiveServer()
    server.start()

if __name__ == "__main__":
    print("Starting data-layer")
    process_data = multiprocessing.Process(target=start_data_layer)
    process_data.start()
    
    time.sleep(2)
    
    print("Starting network-layer")
    process_network = multiprocessing.Process(target=start_network_layer)
    process_network.start()
    
    time.sleep(2)
    
    print("Starting consensus-layer")
    process_consensus = multiprocessing.Process(target=start_consesus_layer)
    process_consensus.start()

    time.sleep(2)
    
    print("Starting incentive-layer")
    process_incetive = multiprocessing.Process(target=start_incentive_server)
    process_incetive.start()
    
    time.sleep(2)
    
    print("Starting service-layer")
    service_layer = ServiceLayer()
    while True:
        try:
            command = input("-> ")
            service_layer.run(command=command.split(" "))
        except Exception as e:
            print(e)
        
import sys
import pickle
import zmq
import json

# Connecting to ZMQ server
context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://127.0.0.1:5555")
client_transaction_id = 0

def main():
    
    transaction_id = -1
    
    while True:        
        
        start_transaction_input = input("Do you want to start a transaction? (yes/no): ").lower()
        if start_transaction_input == 'no':
            break
        if start_transaction_input == 'yes':
            data = {
                "type": "start"
            }
            json_data = json.dumps(data)
            socket.send_string(json_data)
            
            response = socket.recv_string()
            received_data = json.loads(response)
            print(received_data)
            client_transaction_id = received_data['unique_client_id']
            transaction_id = received_data['transaction_id']
        
        while True:   
            operation = input("Do you want to Read/Write/Commit ? ").lower()         
        
            if operation == 'read':
                data = {
                    "type": "read",
                    "transaction_id": transaction_id,
                    "client_transaction_id": client_transaction_id
                }                       
                json_data = json.dumps(data)
                socket.send_string(json_data)
                
                response = socket.recv_string()
                received_data = json.loads(response)
                print(received_data)

            elif operation == 'write':
                value = input("Enter the value to write: ")
                data = {
                    "type": "write",
                    "value": str(value),
                    "transaction_id": transaction_id,
                    "unique_client_id": client_transaction_id
                }
                print(data)
                json_data = json.dumps(data)
                socket.send_string(json_data)
                
                response = socket.recv_string()
                received_response = json.loads(response)
                print("this is the response")
                print(received_response)
                transaction_id = received_response['transaction_id']
                print(received_response['value'])
                    
            elif operation == 'commit':
                data = {
                    "type": "commit",
                    "transaction_id": transaction_id,
                    "unique_client_id": client_transaction_id
                }
                print(data)
                json_data = json.dumps(data)
                socket.send_string(json_data)
                
                received_response = json.loads(response)
                print("this is the response")
                
                response = socket.recv_string()
                received_response = json.loads(response)
                print(received_response) 
                break
    

if __name__ == "__main__":
    main()

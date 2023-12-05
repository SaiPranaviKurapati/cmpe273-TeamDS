from datetime import datetime
from Datastore import DataStore
from Transaction import Transaction
import sys
import pickle
import zmq
import json

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://127.0.0.1:5555")
transactions = {}
data_store = DataStore()
unique_client_id = 0 
transaction_id = 0
global_transaction_Id = 0

def print_saved_mvcc(filename="mvcc_instance.pkl"):
    with open(filename, 'rb') as file:
        data = pickle.load(file)
    print("Transactions:")
    print(data.get('transactions'))
    print("Data Store Versions:")
    print(data.get('data_store_versions'))
       
def start_transaction(transaction_id, unique_client_id):
    timestamp = datetime.now()
    transaction = Transaction(transaction_id, timestamp, unique_client_id)
    transactions[(transaction_id, unique_client_id)] = transaction
    print(transactions)
    unique_client_id +=1
    return timestamp

def save_mvcc(filename):
    data_to_save = {
        'transactions': transactions,
        'data_store_versions': data_store.versions
    }
    with open(filename, 'wb') as file:
        pickle.dump(data_to_save, file)
  
def commit_transaction(transaction_id, unique_client_id):
    if(global_transaction_Id <= transaction_id):
        print ("global transaction id is less than local trnsaction Id, safe to commit ")
        print (global_transaction_Id)
        print(transaction_id)
    else :
        print (global_transaction_Id)
        print(transaction_id) 
        print ("Conflict!!! global transaction id is Greater than local trnsaction Id. Not saving Data ")
        return 0
    
    transaction = transactions[(transaction_id, unique_client_id)]
    if transaction:        
        try:
            for data_item, version in transaction.write_set.items():
                if data_item in data_store.versions:
                    data_store.versions[data_item].append(version)
                else:
                    data_store.versions[data_item] = [version]
            print("Commit is successful")
        except Exception as e:
            print(f"Error committing transaction: {e}")
        finally:
            del transactions[(transaction_id, unique_client_id)]            
    else:
        print(f"Transaction {transaction_id} not found.")
        
    save_mvcc("mvcc_instance.pkl")
    return 1

def rollback_transaction(transaction_id):
    transaction = transactions.get(transaction_id)
    if transaction:
        for data_item, _ in transaction.write_set.items():
            if data_item in data_store.versions:
                del data_store.versions[data_item][-1]
        del transactions[transaction_id]
        print("Rollback successful")
    else:
        print(f"Transaction {transaction_id} not found.")
        
def load_mvcc(filename="mvcc_instance.pkl"):
    try:
        with open(filename, 'rb') as file:
            data = pickle.load(file)
            print("loaded pickle file", data)        
        transactions = data.get('transactions', {})
        data_store.versions = data.get('data_store_versions', {})  
    except FileNotFoundError:
        return None
    except Exception as e:
        print(f"Error loading MVCC from {filename}: {e}")
        sys.exit(1)

def save_mvcc(filename="mvcc_instance.pkl"):
    data_to_save = {
        'transactions': transactions,
        'data_store_versions': data_store.versions
    }
    with open(filename, 'wb') as file:
        pickle.dump(data_to_save, file)

def print_saved_mvcc(filename="mvcc_instance.pkl"):
    try:
        with open(filename, 'rb') as file:
            data = pickle.load(file)

        print("pickle Transactions:")
        print(data.get('transactions'))

        print("pickle Data Store Versions:")
        print(data.get('data_store_versions'))

    except FileNotFoundError:
        print(f"File {filename} not found.")
    except Exception as e:
        print(f"Error loading data from {filename}: {e}")
                
if __name__ == '__main__':
    mvcc_filename = "mvcc_instance.pkl"

    save_mvcc(mvcc_filename)

    print("Initial MVCC state:")
    print_saved_mvcc(mvcc_filename)

    load_mvcc(mvcc_filename)
    start_transaction(transaction_id, unique_client_id)
    print(f"Transaction 1 started")
    key = (transaction_id, unique_client_id)
    data_store.write("balance", 100, transactions.get(key), 1 , 1)
    print("Write by Transaction 1")
    print("Version from Transaction 1", data_store.versions)
    print("Transaction IDs", transactions)    
    save_mvcc(filename="mvcc_instance.pkl") 
    commit_transaction(transaction_id, unique_client_id)
    
        
while True:
    message = socket.recv_string()
    print(message)
    received_data = json.loads(message)
    if received_data['type'] == "start":
        unique_client_id +=1
        data = {
                "transaction_id": transaction_id,
                "unique_client_id": unique_client_id
            }
        json_data = json.dumps(data)
        start_transaction(transaction_id, unique_client_id)
        socket.send_string(json_data)
        
    if received_data['type'] == "read":
        value = data_store.read_from_pickle("balance")
        data = {
                "value": value,
            }
        print("unique_client_id", unique_client_id)
        print("transaction_id", transaction_id)
        print("global_transaction_id", global_transaction_Id)
        json_data = json.dumps(data)
        print("versions",data_store.versions)
        socket.send_string(json_data)
        
    elif received_data['type'] == "write":
        print(received_data)
        client_transactioon = transactions[(received_data['transaction_id'], received_data['unique_client_id'])] 
        new_write_transaction = data_store.write("balance", int(received_data['value']), client_transactioon, received_data ['transaction_id'], received_data['unique_client_id'] )
        print_saved_mvcc("mvcc_instance.pkl")
        print(transactions)
        write_response_data = {
                "value": received_data['value'],
                "transaction_id": received_data ['transaction_id'],
                "unique_client_id": received_data['unique_client_id']
            }
        print("unique_client_id", unique_client_id)
        print("transaction_id", transaction_id)
        print("global_transaction_id", global_transaction_Id)
        write_json_data = json.dumps(write_response_data)
        socket.send_string(write_json_data)        
        
    elif received_data['type'] == "commit":
        print(transactions)
        client_transactioon = transactions[(received_data['transaction_id'], received_data['unique_client_id'])] 
        
        val = commit_transaction(int(received_data['transaction_id']), int(received_data['unique_client_id']) )
        if val == 1:
            transaction_id +=1
            print("Global transaction id before incrementing", global_transaction_Id)
            global_transaction_Id +=1
            print("unique_client_id", unique_client_id)
            print("transaction_id", transaction_id)
            print("global_transaction_id", global_transaction_Id)
            print("Global transaction id after incrementing", global_transaction_Id)
            data = {
                    "value": "success"
                }
            commit_json_data = json.dumps(data)
            socket.send_string(commit_json_data)
        if val == 0:
            data = {
                    "value": "failure Due to Conflict"
                }
            commit_json_data = json.dumps(data)
            socket.send_string(commit_json_data)


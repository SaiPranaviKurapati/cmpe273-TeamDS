import pickle
import sys
class DataStore:
    
    def __init__(self):
        self.versions = {}

    def read(self, data_item,transaction):
        print(",transaction timestamp in read,",transaction.timestamp)
        if data_item in self.versions:
            relevant_versions = []

            if transaction and transaction.timestamp is not None:
                relevant_versions =[v for v in self.versions[data_item] if v['write_ts'] is not None and v['write_ts'] <= transaction.timestamp]
            else:
                relevant_versions = self.versions[data_item]

            if relevant_versions:
                latest_version = max(relevant_versions, key=lambda v: v['write_ts'])
                transaction.read_set.add(data_item)
                return latest_version['value']
        return None


    def write(self, data_item, value, transaction, localId, globalId):
        relevant_versions = self.versions.get(data_item, [])

        if transaction and transaction.timestamp is not None and relevant_versions and max(relevant_versions, key=lambda v: v['write_ts'])['read_ts'] > transaction.timestamp:
            print(f"Transaction {transaction.transaction_id} aborted due to conflict.")
            return None

        existing_versions = self.versions.get(data_item, [])
        last_write_version = max(existing_versions, key=lambda v: v['write_ts'], default={'write_ts': None})

        read_ts = last_write_version['write_ts'] if last_write_version['write_ts'] else transaction.timestamp if transaction else None
        write_ts = transaction.timestamp if transaction else None

        new_version = {
            'value': value,
            'read_ts': read_ts,
            'write_ts': write_ts,
            'localId': localId,
            'globalId': globalId
        }

        if transaction:
            if not hasattr(transaction, 'write_set'):
                transaction.write_set = {}
            transaction.write_set[data_item] = new_version

        if data_item not in self.versions:
            self.versions[data_item] = [new_version]
        else:
            self.versions[data_item].append(new_version)
        print(",transaction ,",transaction)
        return new_version  
    
    def read_latest_by_write_timestamp(self, data_item):
        versions = self.versions.get(data_item, [])
        if versions:
            latest_version = max(versions, key=lambda v: v['write_ts'])
            return latest_version['value']
        else:
            return None
        
    def read_from_pickle(self, data_item):
        try:
            with open("mvcc_instance.pkl", 'rb') as file:
                data = pickle.load(file)
                print("loaded pickle file", data)        
            transactions = data.get('transactions', {})
            data_store = DataStore() 
            data_store.versions = data.get('data_store_versions', {})  
        except FileNotFoundError:
            return None
        except Exception as e:
            print(f"Error loading MVCC from")
            sys.exit(1)
            
        versions = data_store.versions.get(data_item, [])
        if versions:
            latest_version = max(versions, key=lambda v: v['write_ts'])
            return latest_version['value']
        else:
            return None
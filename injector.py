import json
import logging
import os
import sys
import time
from state import SingletonState

logger = logging.getLogger()

BATCH_SIZE = 20
MAX_FIND_SIZE = 500


class DataManager:
    def __init__(self, client, mapping):
        self.client = client
        self.mapping = mapping
        self.creation_batch_buffer = None
        self.update_batch_buffer = None
        self.nb_items = None
        self.total_objects_injected = None

        self.find_batch_dict = None

    

    def process(self, data, error_file_path, begin_index=0):
        logger.info("Number of elements in data: {}".format(len(data)))
        self.creation_batch_buffer = ''
        self.update_batch_buffer = ''
        self.nb_items = 0
        self.total_objects_injected = 0
        for item in data:
            try:
                self.nb_items += 1
                logger.info("item : " + str(item))
                self.process_through_data(data[item])
                if self.nb_items == BATCH_SIZE:
                        self.send_data_to_create()
                        self.send_data_to_update()
                        self.total_objects_injected += self.nb_items
                        self.nb_items = 0
            except:
                with open(os.path.join(error_file_path), "w") as f:
                    f.write(str(self.total_objects_injected + begin_index))
                sys.exit(0)

        if self.creation_batch_buffer != '':
            try:
                self.send_data_to_create()
                self.send_data_to_update()
                self.total_objects_injected += self.nb_items
                self.nb_items = 0
            except:
                with open(os.path.join(error_file_path), "w") as f:
                    f.write(str(self.total_objects_injected + begin_index))
                sys.exit(0)

    def process_through_data(self, data):

        items = data['_items']

        if items:
            logger.info("node")
            for item in items:
                self.process_through_data(items[item])

            self.process_projection(data)
        else:
            # Leaf
            logger.info("leaf")
            self.process_projection(data)

    def process_projection(self, projection):
        logger.info("Processing projection ... ")
        response = self.client.get_projection_by_ori(projection["_id"])
        result = json.loads(response.content.decode('utf8'))
        if result['total_items'] == 0:
            self.creation_batch_buffer = self.creation_batch_buffer + projection["_data"]
        else:
            result = result['items'][0]
            rdf_uuid = '<' + result['_ori'] + '>\t<http://orange-labs.fr/fog/ont/iot.owl#uuid>\t\"' + result['_uuid'] + '\"^^xsd:string .\n'
            self.update_batch_buffer = self.update_batch_buffer + rdf_uuid + projection["_data"]


    # Don't use this function for the moment
    def clean_namespace(self):
        uuids = []
        while True:
            result = self.client.get_projections_by_namespace(MAX_FIND_SIZE, 0)
            result = json.loads(result.content.decode('utf8'))
            for item in result.get('items', []):
                print('delete : ' + item.get('_ori'))
                uuids.append(item.get('_uuid'))
                if len(uuids) == MAX_FIND_SIZE:
                    response = self.client.delete_projection_batch(uuids)
                    print(response.status_code)
                    uuids = []
            if len(result.get('items')) < MAX_FIND_SIZE:
                break
        if len(uuids) > 0:
            self.client.delete_projection_batch(uuids)

    def send_data_to_create(self):
        # The buffer is empty
        if not self.creation_batch_buffer:
            return
        print('Create query send')
        self.creation_batch_buffer = "@prefix xsd:     <http://www.w3.org/2001/XMLSchema#> .\n\n" + self.creation_batch_buffer
        create_result = self.client.create_projection_batch(self.creation_batch_buffer)
        if create_result.status_code < 400:
            print('Create query success')
            logger.info("Insertion successfully done")
        else:
            logger.error("Insertion failed ! : status: {}  - {}, sended_batch: {}".format(create_result.status_code,
                                                                        create_result.content, self.creation_batch_buffer))
            #If a timeout exception is raised, sleep to allow the server to process the request
            if create_result.status_code == 504:
                time.sleep(30)
        self.creation_batch_buffer = ''

    def send_data_to_update(self):
        # The buffer is empty
        if not self.update_batch_buffer:
            return
        print('Update query send')
        self.update_batch_buffer = "@prefix xsd:     <http://www.w3.org/2001/XMLSchema#> .\n\n" + self.update_batch_buffer
        update_result = self.client.update_replace_projection_batch(self.update_batch_buffer)
        if update_result.status_code < 400:
            print('Update query success')
            logger.info("Insertion successfully done")
        else:
            print("Insertion failed ! : status: {}  - {}".format(update_result.status_code,
                                                                        update_result.content))
            logger.error("Insertion failed ! : status: {}  - {}".format(update_result.status_code,
                                                                        update_result.content))
            # If a timeout exception is raised, sleep to allow the server to process the request
            if update_result.status_code == 504:
                time.sleep(30)

        self.update_batch_buffer = ''

    def process_batch(self, data, error_file_path, begin_index=0):

        state = SingletonState.instance()

        logger.info("Number of elements in data: {}".format(len(data)))

        data_values = list(data.values())
        batch_total_loop = float(len(data_values)) / BATCH_SIZE
        nb_loop = 0
        self.total_objects_injected = 0
        self.update_batch_buffer = ""
        self.creation_batch_buffer = ""

        while nb_loop < batch_total_loop and state.get_state() != 'STOP' and state.get_state() != 'PAUSE':
            self.find_batch_dict = {}

            # About Object in data_values, those are most likely root object with childrens, meaning the number of object to inject > BATCH_SIZE
            for projection in data_values[:BATCH_SIZE]:
                self.process_through_data_batch(projection)

            self.process_projection_batch()
            self.send_data_to_create()
            self.send_data_to_update()
            # Slicing off data_values we just processed into thingin (be it through update or create)
            data_values = data_values[(BATCH_SIZE - len(data_values)):]
            nb_loop += 1
            self.total_objects_injected = nb_loop * BATCH_SIZE

        if state.get_state() == 'PAUSE':
            with open(os.path.join(error_file_path), "w") as f:
                f.write(str(self.total_objects_injected + begin_index))

    def process_through_data_batch(self, data):

        items = data['_items']

        if items:
            logger.info("node")
            for item in items:
                self.process_through_data_batch(items[item])
            self.find_batch_dict[data['_id']] = {'_data': data['_data'], '_id': data['_id']}
        else:
            # Leaf
            logger.info("leaf")
            self.find_batch_dict[data['_id']] = {'_data': data['_data'], '_id': data['_id']}

    def process_projection_batch(self):

        logger.info("Processing projections ... ")

        oris = [ori for ori in self.find_batch_dict]
        nb_finds = float(len(oris) / MAX_FIND_SIZE)
        loop = 0
        while loop < nb_finds:

            response = self.client.get_projections_by_ori(oris[:MAX_FIND_SIZE], MAX_FIND_SIZE)

            if response.status_code == 504:
                raise Exception('Request timed out from Thing\'in api, try again later')
            result = json.loads(response.content.decode('utf8'))

            if result['total_items'] == 0:
                pass
            else:
                result = result['items']
                for item in result:
                    self.find_batch_dict[item['_ori']]['_uuid'] = item['_uuid']
            loop += 1
            oris = oris[(MAX_FIND_SIZE - len(oris)):]

        for ori in self.find_batch_dict:
            if self.find_batch_dict[ori].get('_uuid') is None:
                self.creation_batch_buffer = self.creation_batch_buffer + self.find_batch_dict[ori]["_data"]
            else:
                projection = self.find_batch_dict[ori]
                rdf_uuid = '<' + projection['_id'] + '>\t<http://orange-labs.fr/fog/ont/iot.owl#uuid>\t\"' + projection[
                    '_uuid'] + '\"^^xsd:string .\n'
                self.update_batch_buffer = self.update_batch_buffer + rdf_uuid + projection["_data"]
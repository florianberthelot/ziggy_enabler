from datetime import datetime
from dateutil import parser
import json
import logging


logger = logging.getLogger()

def default_custom_function(id):
    return id

class JsonToRDFConverter:

    def __init__(self, mapping, default_custom_function=None, separator='.'):
        self.mapping = mapping
        self.switchDict = {
            "boolean": self.boolean,
            "integer": self.integer,
            "float": self.floatType,
            "string": self.string,
            "double": self.double,
            "date": self.date
        }

        self.separator = separator
        # Big map of <String, Dict>, for each individual is stored with the ori as its key, and the data as the value.
        self.map_items = dict()
        self.custom_function = default_custom_function

    def parse(self, data):

        try:
            skeleton = self.mapping["skeleton"]
        except:
            raise Exception('Mapping file malformed, need skeleton object to describe how to process data')

        # mark every items of the map with a keep_alive to False
        # upon receiving the new payload and thus updating the internal cached data,
        # if the mark remains to False,
        # it means the item did not existed anymore into the new payload and has to be deleted

        self.mark_to_delete(self.map_items)
        self.loop_through_data(self.map_items, skeleton, data)

        # Loop through self.map_items and trigger all the mark
        self.purge(self.map_items)

        return self.map_items

    def loop_through_data(self, map_items, skeleton, json_data):

        if json_data is None:
            return {}

        # A skeleton is either composed by a list or objects, there are no other alternatives for now
        if type(skeleton) is list:
            # Retrieve the skeleton to apply for the elements inside the list
            next_skeleton = skeleton[0]

            # For each element in the JsonArray of json_data
            for element in json_data:
                # Recursive call to the item of the list
                self.loop_through_data(map_items, next_skeleton, element)
        else:

            # Retrieve appropriate mapping
            mapping_name = skeleton["_mapping_id"]
            mapping_data = self.mapping[mapping_name]

            # Build individual ORI
            try:
                individual_ori = mapping_data["_id"].get("static", "")

                param = mapping_data["_id"]["param"]

            except Exception as e:
                raise Exception("Missing \"_id\" field in {}'s skeleton.\nThe given skeleton is {}.".format(mapping_name, json.dumps(mapping_data, indent = 4)))

            # If there is separator character in mapping[_id][param], it will detect it and try to split to get the param path of the real id value.
            id_value = self.reach_value(param, json_data)
            individual_ori = individual_ori + str(id_value)

            # If the individual has already been identified in map_items
            if individual_ori in map_items:
                # Retrieve the dictionary storing the data of the individual
                item = map_items[individual_ori]
                # Retrieve a pointer to the children (in terms of JSON encapsulation) of this individual
                items = item["_items"]

            else:

                # Prepare a new dictionary to store the data of the individual
                item = dict()
                # Prepare a new dictionary to store the children (in terms of JSON encapsulation) of this individual
                items = dict()

                # Set the individual_ori
                item["_id"] = individual_ori

            is_recursive = skeleton.get("_recursive", False)
            # Check if the current individual may have a recursive form, this can be needed for lists.
            if is_recursive:
                # Check if the recursive field has been properly declared
                recursive_field = skeleton.get("_recursive_field", None)
                if recursive_field is not None:
                    # Check if the recursive field in is the individual data
                    if recursive_field in json_data:
                        # Recursive loop, actually this is a very bad idea because a list with enough element will
                        # make the stack trace crash.
                        # TODO: Rewrite the loop using a while.
                        self.loop_through_data(items, skeleton, json_data[recursive_field])
                else:
                    logger.warning(
                        "Mapping {} has been declared as recursive but could not find"
                        " the _recursive_field declaration.".format(mapping_name))

            # For each key of the skeleton
            for key in skeleton:
                # Check if the key is not a metadata field
                if key != "_mapping_id" and key != "_recursive" and key != "_recursive_field":
                    # Recursive call to go through the skeleton childrens
                    self.loop_through_data(items, skeleton[key], json_data.get(key))

            # Force keep alive of the individual to true, this will ensure it will not be purged at the end of the parsing
            item["keep_alive"] = True
            # Process the turtle data of the individual
            item["_data"] = self.process_turtle(mapping_data, individual_ori, json_data)
            # Set the children (in terms of JSON encapsulation) of this individual
            item["_items"] = items

            map_items[individual_ori] = item

        return map_items

    def mark_to_delete(self, map_items):
        for key_item in map_items:
            map_items[key_item]["keep_alive"] = False
            items = map_items[key_item]['_items']

            if items:
                self.mark_to_delete(items)

    def purge(self, map_items):
        for key_item in list(map_items):
            if not map_items[key_item]["keep_alive"]:
                map_items.pop(key_item)
            else:
                items = map_items[key_item]['_items']
                if items:
                    self.purge(items)

    def process_turtle(self, mapping, individual_ori, individual_data):

        ttl = ""

        # Metadata
        class_metadata = mapping["_class"]

        # Handle individual declaration
        # Check if the class attribution of the individual is depends of a field or not.
        if class_metadata["field_dependent"]:

            class_field = class_metadata["field"]
            field_value = individual_data[class_field]
            field_value_to_owl_class_map = class_metadata["map"]

            if field_value in field_value_to_owl_class_map:
                ttl += self.declare_new_individual(individual_ori, field_value_to_owl_class_map[field_value])
            else:
                raise BaseException(
                    "Could not find appropriate class for following individual : {}"
                    " with class field {} and value {}".format(individual_ori, class_field, str(field_value)))
        else:
            # Force the class
            ttl += self.declare_new_individual(individual_ori, class_metadata["value"])

        # Handle data and object properties
        ttl += self.process_turtle_data_object_properties(individual_ori, mapping, individual_data)
        ttl += self.close_individual()

        # return "@prefix xsd:     <http://www.w3.org/2001/XMLSchema#> .\n\n" + ttl
        return ttl

    def process_turtle_data_object_properties(self, individual_ori, individual_mapping, individual_data, prefix=""):

        ttl = ""
        object_properties_metadata = individual_mapping.get("_object_properties", [])

        # Handle data properties
        # For each key in the individual_data dictionary

        # _location provide any users to create geographical coordinates
        if individual_mapping.get('_location') is not None:
            location_property = individual_mapping.get('_location')
            if type(location_property) is dict:
                longitude_field = location_property['longitude']
                latitude_field = location_property['latitude']
                ttl += self.declare_location_property(individual_ori, individual_data, longitude_field, latitude_field)

        if individual_mapping.get('_hidden_values') is not None:
            for hide_value, data_property_metadata in individual_mapping['_hidden_values'].items():
                property_value = self.reach_value(hide_value, individual_data)
                ttl += self.declare_data_property(individual_ori, data_property_metadata, property_value)


        for property_key in individual_data:

            # Retrieve its value
            property_value = individual_data[property_key]

            # Check if the prefix is empty, prefix will be set in case of data with multiple depth level
            if prefix == "":
                key_prefixed = str(property_key)
            else:

                # Force the key to use the prefix to have something similar to "prefix.key"
                key_prefixed = str(prefix + "." + property_key)

            # Check if the key starts with the "_" symbol, in that case just ignore it, this is reserved.
            if not property_key.startswith("_"):
                # Prepare a is_data_property flag
                is_data_property = True

                # Handling Object Properties
                # For each declared object_property into the object_properties_metadata dictionary
                for object_property_metadata in object_properties_metadata:

                    # Retrieve the field to use for the current object_property
                    object_property_field = object_property_metadata["field"]
                    # Retrieve the ori of the object_property
                    object_property_ori = object_property_metadata["object_property_ori"]
                    generate_id = object_property_metadata["generate_id"]

                    # Check if the key_prefixed match with the object_property field
                    if key_prefixed == object_property_field:
                        # Current property is an object property
                        is_data_property = False

                        # Check if we must generate an id for the individual targeted by the object_property
                        if generate_id == 'true':
                            # Id must be generated

                            target_mapping = self.mapping[object_property_metadata["_mapping_id"]]
                            target_mapping_id_metadata = target_mapping["_id"]

                            # Check if the property_value holds a list
                            if type(property_value) is list:

                                # For each element of the list, here elements must be dictionary holding enough data to
                                # generate the targeted individual ori
                                for property_sub_value in property_value:
                                    # Ignore object_property if value is None
                                    if property_sub_value is not None:
                                        # Check if the property_sub_value is a dictionary
                                        self.check_object_property_value_is_dict(property_sub_value, individual_ori,
                                                                                 object_property_ori)

                                        targeted_individual_ori = target_mapping_id_metadata["static"] + str(
                                            property_sub_value[target_mapping_id_metadata["param"]])
                                        ttl += self.declare_object_property(individual_ori, object_property_ori,
                                                                            targeted_individual_ori)
                            else:
                                # Ignore object_property if value is None
                                if property_value is not None:
                                    # here property_value must be dictionary holding enough data to generate
                                    # the targeted individual ori
                                    # Check if the property_sub_value is a dictionary
                                    self.check_object_property_value_is_dict(property_value, individual_ori,
                                                                             object_property_ori)

                                    targeted_individual_ori = target_mapping_id_metadata["static"] + str(
                                            property_value[target_mapping_id_metadata["param"]])
                                    ttl += self.declare_object_property(individual_ori, object_property_ori,
                                                                        targeted_individual_ori)

                        elif generate_id == 'false':
                            # Id must not be generated
                            # Instead it must taken from the map <field_value, individual_ori> embedded into the object
                            # property metadata
                            object_property_individuals_map = object_property_metadata["map"]

                            # Check if the property_value holds a list
                            if type(property_value) is list:
                                # For each element of the list, here elements must be simple string value
                                for property_sub_value in property_value:
                                    # Check if the property_sub_value is a string
                                    self.check_object_property_value_is_str(property_sub_value, individual_ori,
                                                                            object_property_ori)

                                    # Check if the property_sub_value is contained in the object property individual map
                                    self.check_object_property_value_is_in_map(property_sub_value,
                                                                               object_property_individuals_map,
                                                                               individual_ori, object_property_ori)

                                    targeted_individual_ori = object_property_individuals_map[property_sub_value]
                                    ttl += self.declare_object_property(individual_ori, object_property_ori,
                                                                        targeted_individual_ori)
                            else:
                                # Check if the property_sub_value is a string
                                self.check_object_property_value_is_str(property_value, individual_ori,
                                                                        object_property_ori)

                                # Check if the property_sub_value is contained in the object property individual map
                                self.check_object_property_value_is_in_map(property_value,
                                                                           object_property_individuals_map,
                                                                           individual_ori, object_property_ori)

                                targeted_individual_ori = object_property_individuals_map[property_value]
                                ttl += self.declare_object_property(individual_ori, object_property_ori,
                                                                    targeted_individual_ori)
                        else :
                            # Custom generated iri
                            # Ignore object_property if value is None
                            if property_value is None:
                                raise Exception('Encountered error for mapping with id : '  + str(self.mapping["_mapping_id"]) + ', property_value is None for the object ' + individual_ori + ' while creating custom ori')

                            # here property_value must be dictionary holding enough data to generate
                            # the targeted individual ori
                            # Check if the property_sub_value is a dictionary

                            self.check_object_property_value_is_str(property_value, individual_ori,
                                                                     object_property_ori)
                            if self.custom_function is not None:
                                targeted_individual_ori = self.custom_function(property_value, object_property_ori)
                            else:
                                targeted_individual_ori = property_value

                            ttl += self.declare_object_property(individual_ori, object_property_ori,
                                                                targeted_individual_ori)



                # Handling Data Properties

                if is_data_property:
                    # Check if the property_key is present in the individual mapping
                    # This test is probably useless, the test with the key_prefixed should be enough by itself
                    if property_key in individual_mapping:
                        data_property_metadata = individual_mapping[property_key]
                        ttl += self.declare_data_property(individual_ori, data_property_metadata,
                                                          property_value)
                    # Check the key_prefixed version
                    elif key_prefixed in individual_mapping:
                        data_property_metadata = individual_mapping[property_key]
                        ttl += self.declare_data_property(individual_ori, data_property_metadata,
                                                          property_value)
                    else:
                        # logger.warning(
                        #     "Could not find related metadata for the property {}. It will be ignored for the"
                        #     " individual generation.".format(property_key))
                        pass
            else:
                logger.warning(
                    "Detected key {} while processing data and object properties for individual with ori {}."
                    " field starting with a\"_\" symbol are ignored.".format(property_key, individual_ori))

        return ttl

    def check_object_property_value_is_dict(self, property, individual_ori, object_property_ori):
        # Generate the ori for the targeted individual, property_value must be a dictionary
        # holding enough data to generate the targeted individual ori
        if not type(property) is dict :
            property_value_type = str(type(property))
            raise BaseException(
                "Could not generate the targeted individual ori for the individual with ori {}"
                " on object property {}, property_value was of type {}, expected either a dict"
                " or a list.".format(individual_ori, object_property_ori, property_value_type))

    def check_object_property_value_is_str(self, property_value, individual_ori, object_property_ori):
        # Generate the ori for the targeted individual, property_value must be a dictionary
        # holding enough data to generate the targeted individual ori
        if not type(property_value) is str:
            property_value_type = str(type(property_value))
            raise BaseException(
                "Could not generate the targeted individual ori for the individual with ori {}"
                " on object property {}, property_value was of type {}, expected either a str"
                ".".format(individual_ori, object_property_ori, property_value_type))

    def check_object_property_value_is_in_map(self, property_value, individuals_map, individual_ori,
                                              object_property_ori):
        if property_value not in individuals_map:
            raise BaseException(
                "Could not generate the targeted individual ori for the individual with ori"
                " {}  on object property {}, property_value was : {}, it could not be"
                " found into the object property individuals map {}"
                ".".format(individual_ori, object_property_ori, property_value,
                           str(individuals_map)))

    def declare_new_individual(self, individual_ori, owl_class):
        return "<" + individual_ori + ">\ta\t<" + owl_class + "> .\n"

    def declare_object_property(self, individual_ori, object_property_ori, value):
        return "<" + individual_ori + ">\t<" + object_property_ori + ">\t<" + str(value) + "> .\n"

    def declare_data_property(self, individual_ori, data_property_metadata, value):

        data_property_ori = data_property_metadata["datatype_property_ori"]
        data_property_datatype = data_property_metadata["type"]

        ttl = "<" + individual_ori + ">\t<" + data_property_ori + ">\t"
        ttl += self.switchDict[data_property_datatype](value)
        ttl += " .\n"
        return ttl

    def declare_location_property(self, individual_ori, individual_data, longitude_field, latitude_field):

        latitude = self.reach_value(latitude_field, individual_data)
        longitude = self.reach_value(longitude_field, individual_data)

        property_value = "{\"type\": \"Point\", \"coordinates\": [" + str(longitude) + ", " + str(latitude) + "]}\"^^xsd:string"

        ttl = "<" + individual_ori + ">\t<http://www.opengis.net/gml/pos>\t"
        ttl += property_value
        ttl += " .\n"
        return ttl

    def reach_value(self, value_path, individual_data):
        value_path = value_path.split(self.separator)
        property_value = individual_data
        for param in value_path:

            if isinstance(property_value, list):
                try:
                    index = int(param)
                except ValueError:
                    raise Exception("You try to access list value with non integer value.\n Path to the value {}.".format(value_path))
                property_value = property_value[index]
            else:
                property_value = property_value[param]

        return property_value

    def close_individual(self):
        return "\n"

    def boolean(self, value):
        return "\"" + str(value) + "\"^^xsd:boolean"

    def integer(self, value):
        return "\"" + str(value) + "\"^^xsd:integer"

    def floatType(self, value):
        return "\"" + str(value) + "\"^^xsd:float"

    def string(self, value):
        return "\"" + str(value) + "\"^^xsd:string"

    def double(self, value):
        return "\"" + str(value) + "\"^^xsd:double"

    def date(self, value):
        return "\"" + str(datetime.strftime(parser.parse(str(value)), '%Y-%m-%dT%H:%M:%S.000Z')) + "\"^^xsd:date"


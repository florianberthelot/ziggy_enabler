import os
import requests
import json
import logging

logger = logging.getLogger()


class ZiggyHTTPClient:
    # Strange behavior using localhost address, translation seems to shortcut the parameters
    # Forgetting the "/" at the end of the URL leads to redirection which are kind of problematic, leave it there.
    PROXIES = dict(http=os.environ.get('http_proxy', ''), https=os.environ.get('https_proxy', ''))

    MODEL_ENTRY = "models/"
    PROJECTION_ENTRY = "projections/"
    BATCH_PROJECTION_ENTRY = "batch/projections/"
    PROJECTION_FIND_ENTRY = "projections/find/"

    PROJECTION_UPDATE_REPLACE_ENTRY = "update/replace/"
    PROJECTION_UPDATE_UNSET_ENTRY = "update/unset/"
    PROJECTION_UPDATE_SET_ENTRY = "update/set/"

    # ADMIN_NAMESPACE

    def __init__(self, namespace, endpoint):
        self.namespace = namespace
        self.session = requests.Session()

        self.endpoint = str(endpoint)

        if not self.endpoint.endswith("/"):
            self.endpoint += "/"

        self.model_url = self.endpoint + self.MODEL_ENTRY
        self.projection_url = self.endpoint + self.PROJECTION_ENTRY
        self.batch_projection_url = self.endpoint + self.BATCH_PROJECTION_ENTRY
        self.projection_find_url = self.endpoint + self.PROJECTION_FIND_ENTRY

        logger.info("ZiggyHTTPClient will run with following proxies : {}".format(self.PROXIES))

    def get_projection_by_ori(self, ori, hide_default_namespace = "true"):

        headers = {"namespace": self.namespace,
                   "Content-Type": "application/json",
                   "Accept": "application/json",
                   "Hide-Default-Namespace": hide_default_namespace}
        payload = {"query": {"$ori": str(ori)}}
        payload = json.dumps(payload)

        logger.info("POST - url : {}, headers : {}".format(self.projection_find_url, headers))
        logger.debug("POST - url : {}, data : {}, headers : {}".format(self.projection_find_url, payload, headers))

        return self.session.post(self.projection_find_url, data=payload, proxies=self.PROXIES, headers=headers)

    def get_projections_by_ori(self, oris, size, hide_default_namespace = "true"):

        headers = {"namespace": self.namespace,
                   "Content-Type": "application/json",
                   "Accept": "application/json",
                   "Hide-Default-Namespace": hide_default_namespace}
        payload = {"query": {"$ori": { "$in" : oris}}}
        payload = json.dumps(payload)

        logger.info("POST - url : {}, headers : {}".format(self.projection_find_url, headers))
        logger.debug("POST - url : {}, data : {}, headers : {}".format(self.projection_find_url, payload, headers))

        return self.session.post(self.projection_find_url + "?size={}".format(size), data=payload, proxies=self.PROXIES, headers=headers)

    def get_projection_by_uuid(self, uuid):

        headers = {"Namespace": self.namespace,
                   "Content-Type": "text/turtle",
                   "read-mode": "strict"}
        url = self.projection_url + uuid

        logger.info("GET - url : {}, headers : {}".format(url, headers))

        return self.session.get(url, proxies=self.PROXIES, headers=headers)

    def create_projection(self, data):

        headers = {"namespace": self.namespace, "Content-Type": "text/turtle"}
        data = str(data).encode('utf-8')

        logger.info("POST - url : {}, headers : {}".format(self.projection_url, headers))
        logger.debug("POST - url : {}, data : {}, headers : {}".format(self.projection_url, data, headers))

        return self.session.post(self.projection_url, data=data, proxies=self.PROXIES, headers=headers,
                                 allow_redirects=False)

    def create_projection_batch(self, data):

        headers = {"namespace": self.namespace, "Content-Type": "text/turtle"}
        data = str(data).encode('utf-8')

        logger.info(
            "POST - url : {}, headers : {}".format(self.batch_projection_url, headers))
        logger.debug(
            "POST - url : {}, data : {}, headers : {}".format(self.batch_projection_url, data, headers))

        return self.session.post(self.batch_projection_url, data=data, proxies=self.PROXIES, headers=headers,
                                 allow_redirects=False)

    def delete_projection(self, uuid):

        headers = {"namespace": self.namespace}
        url = self.projection_url + uuid

        logger.info("DELETE - url : {}, headers : {}".format(url, headers))

        return self.session.delete(url, proxies=self.PROXIES, headers=headers, allow_redirects=False)

    def delete_projection_batch(self, uuids):
        headers = {"namespace": self.namespace, "Content-Type": "application/json", "Accept": "application/json"}
        logger.info("DELETE - url : {}, headers : {}".format(self.batch_projection_url, headers))

        return self.session.delete(self.batch_projection_url, json=uuids, proxies=self.PROXIES, headers=headers,
                                   allow_redirects=False)

    def update_replace_projection(self, uuid, data):

        headers = {"namespace": self.namespace, "Content-Type": "text/turtle"}
        data = str(data).encode('utf-8')
        url = self.projection_url + self.PROJECTION_ENTRY + self.PROJECTION_UPDATE_REPLACE_ENTRY + uuid

        logger.info("PUT - url : {}, headers : {}".format(url, headers))
        logger.debug("PUT - url : {}, data : {}, headers : {}".format(url, data, headers))

        return self.session.put(url, data=data, proxies=self.PROXIES, headers=headers,
                                allow_redirects=False)

    def update_replace_projection_batch(self, data):

        headers = {"namespace": self.namespace, "Content-Type": "text/turtle"}
        data = str(data).encode('utf-8')
        url = self.batch_projection_url + self.PROJECTION_UPDATE_REPLACE_ENTRY

        logger.info("PUT - url : {}, headers : {}".format(url, headers))
        logger.debug("PUT - url : {}, data : {}, headers : {}".format(url, data, headers))

        return self.session.put(url, data=data, proxies=self.PROXIES, headers=headers,
                                allow_redirects=False)

    def update_set_projection(self, uuid, data):

        headers = {"namespace": self.namespace, "Content-Type": "text/turtle"}
        data = str(data).encode('utf-8')
        url = self.endpoint + self.PROJECTION_ENTRY + self.PROJECTION_UPDATE_SET_ENTRY + uuid

        logger.info("PUT - url : {}, headers : {}".format(url, headers))
        logger.debug("PUT - url : {}, data : {}, headers : {}".format(url, data, headers))

        return self.session.put(url, data=data, proxies=self.PROXIES, headers=headers,
                                allow_redirects=False)

    def update_set_projection_batch(self, data):

        headers = {"namespace": self.namespace, "Content-Type": "text/turtle"}
        data = str(data).encode('utf-8')
        url = self.batch_projection_url + self.PROJECTION_UPDATE_SET_ENTRY

        logger.info("PUT - url : {}, headers : {}".format(url, headers))
        logger.debug("PUT - url : {}, data : {}, headers : {}".format(url, data, headers))

        return self.session.put(url, data=data, proxies=self.PROXIES, headers=headers,
                                allow_redirects=False)

    def update_unset_projection(self, uuid, data):

        headers = {"namespace": self.namespace, "Content-Type": "text/turtle"}
        data = str(data).encode('utf-8')
        url = self.projection_url + self.PROJECTION_ENTRY + self.PROJECTION_UPDATE_UNSET_ENTRY + uuid

        logger.info("PUT - url : {}, headers : {}".format(url, headers))
        logger.debug("PUT - url : {}, data : {}, headers : {}".format(url, data, headers))

        return self.session.put(url, data=data, proxies=self.PROXIES, headers=headers,
                                allow_redirects=False)

    def update_unset_projection_batch(self, data):

        headers = {"namespace": self.namespace, "Content-Type": "text/turtle"}
        data = str(data).encode('utf-8')
        url = self.batch_projection_url + self.PROJECTION_UPDATE_UNSET_ENTRY

        logger.info("PUT - url : {}, headers : {}".format(url, headers))
        logger.debug("PUT - url : {}, data : {}, headers : {}".format(url, data, headers))

        return self.session.put(url, data=data, proxies=self.PROXIES, headers=headers,
                                allow_redirects=False)

    def get_projections_by_namespace(self, size = 1000, index = 0, hide_default_namespace = "true"):
        headers = {"namespace": self.namespace, "Content-Type": "application/json", "Accept": "application/json", "Hide-Default-Namespace": hide_default_namespace}
        data = '''{
                 "query": {}
               }'''
        url = self.projection_find_url + "?size={}&index={}".format(size, index)
        return self.session.post(url, data=data, proxies=self.PROXIES, headers=headers,
                                 allow_redirects=False)


    def get_projections_by_classes(self, classes, size = 1000, index = 0, hide_default_namespace = "true"):
        headers = {"namespace": self.namespace, "Content-Type": "application/json", "Accept": "application/json", "Hide-Default-Namespace": hide_default_namespace}
        data = {"query": {"$class": { "$in" : classes}}}
        url = self.projection_find_url + "?size={}&index={}".format(size, index)
        return self.session.post(url, json=data, proxies=self.PROXIES, headers=headers,
                                 allow_redirects=False)
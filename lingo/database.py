import re
import httplib
import urllib
import json
from urlparse import urlparse
import types
import threading
from datetime import datetime

import pymongo
import bson

from errors import *
import lingo

class Database(object):
    instances = {}

    @classmethod
    def from_host(self, host, dbname):
        if re.match(ur'^mongodb://', host):
            return MongoDB(pymongo.MongoClient(host), dbname)
        elif re.match(ur'^(couchdb|http)://', host):
            host = re.sub(ur'^couchdb', 'http', host)
            return CouchDB(host, dbname)
        else:
            raise DatabaseError("Invalid protocol, could not determine the type of database")

    def __init__(self, name = None):
        name_ = name or 'default'
        cls = self.__class__.__name__
        if cls not in self.instances:
            self.instances[cls] = {}

        if name_ in self.instances[cls]:
            raise DatabaseError("A '%s' instance with the name '%s' already exists" % (cls, name_))

        self.instances[cls][name_] = self

    @classmethod
    def get_instance(self, cls, name = None):
        cls_ = None
        name_ = None
        if name is None and '/' in cls:
            cls_, name_ = cls.split('/')
        else:
            cls_ = cls
            name_ = name or 'default'
        if cls_ not in self.instances:
            raise DatabaseError("No instances of '%s' exist" % (cls_,))
        if name_ not in self.instances[cls_]:
            raise DatabaseError("The '%s' instance '%s' does not exist" % (cls_, name_))
        return self.instances[cls_][name_]

class DatabasePartial(object):
    def __init__(self, model_or_instance, db_instance):
        self.model_or_instance = model_or_instance
        self.db_instance = db_instance

    def __getattr__(self, attrname):
        attr = getattr(self.db_instance, attrname)
        if isinstance(attr, types.MethodType):
            def _wrap(*args, **kwargs):
                return attr(self.model_or_instance, *args, **kwargs)
            return _wrap
        else:
            return attr

class MongoDBCustomCursor(object):
    def __init__(self, wrapped, cls):
        self.__dict__['wrapped']=wrapped
        self.__dict__['cls']=cls

    def __getattr__(self, k):
        return getattr(self.__dict__['wrapped'], k)

    def __setattr__(self, k, v):
        return setattr(self.__dict__['wrapped'], k, v)

    def __getitem__(self, i):
        cls=self.__dict__['cls']
        data=self.__dict__['wrapped'][i]
        return cls(**data)

class MongoDB(Database):
    def __init__(self, server, dbname, name = None):
        super(MongoDB, self).__init__(name)
        self.server = server
        self.dbname = dbname
        self.db = server[dbname]

    def _getCollection(self, model):
        return self.db[model._clsattr("__Collection__") or model.__name__]

    def find(self, model, spec, **kwargs):
        csr=self._getCollection(model).find(spec, **kwargs)
        return MongoDBCustomCursor(csr, model)

    def one(self, model, *args, **kwargs):
        csr=self.find(model, *args, **kwargs)
        if csr.count()!=1:
            raise ValidationError("Invalid result count for one(): expected exactly one, got %d"%(csr.count(),))
        return csr[0]

    def get(self, model, idstr):
        if not isinstance(idstr, bson.ObjectId):
            idstr=bson.ObjectId(idstr)
        return self.one(model, {"_id": idstr})

    def save(self, model_instance, **kwargs):
        if model_instance.__class__._clsattr("__Embedded__"):
            raise ModelError("Model %s is embedded and cannot be saved"%(model_instance.__class__.__name__,))
        if not model_instance.touch():
            raise ModelError("touch() failed")
        if model_instance._id:
            self._getCollection(model_instance.__class__).update({"_id": model_instance._id}, model_instance._asdict(skip=["_id"]), upsert=True, safe=True, multi=False, **kwargs)
        else:
            model_instance._id=self._getCollection(model_instance.__class__).insert(model_instance._asdict(skip=["_id"]), safe=True, **kwargs)
        return model_instance._id

class CouchDB(Database):
    def __init__(self, host, dbname, sync_views = True, name = None):
        super(CouchDB, self).__init__(name)
        res = urlparse(host)
        if res.scheme != 'http':
            raise NotImplementedError("Only the HTTP scheme is supported")
        elif (res.path and res.path != '/') or res.params or res.query or res.fragment:
            raise DatabaseError("Extra information was passed in the URL, which is not supported")
        elif res.username or res.password:
            raise NotImplementedError("Authentication is not supported")
        self.host = res.hostname
        self.port = res.port or 5984
        self.dbname = dbname

        self.default_headers = {'Connection': 'keep-alive'}
        self.threadlocal = threading.local()

        self._get_connection(True, sync_views, True)

    def _get_connection(self, test_conn = False, sync_views = False, reconnect = False):
        if reconnect or not hasattr(self.threadlocal, 'conn'):
            if hasattr(self.threadlocal, 'conn'):
                self.threadlocal.conn.close()
            self.threadlocal.conn = httplib.HTTPConnection(self.host, self.port)
            self.threadlocal.conn.connect()

        if test_conn:
            server_info = self._request('GET', '/').parsed_body
            assert 'couchdb' in server_info
            assert server_info['couchdb'] == 'Welcome'

        if sync_views:
            self.sync_views()

        return self.threadlocal.conn

    def _request(self, method, url, query = {}, body = None, headers = {}):
        real_headers = {}
        real_headers.update(self.default_headers)
        real_headers.update(headers)
        max_tries = 3
        for try_num in range(0, max_tries):
            try:
                self._get_connection().request(method, url + '?' + urllib.urlencode(query), body, real_headers)
                res = self._get_connection().getresponse()
            except (httplib.CannotSendRequest, httplib.BadStatusLine) as e:
                self._get_connection(reconnect = True)
                continue

            if res.status < 200 or res.status >= 400:
                ex = DatabaseError("%d %s" % (res.status, res.reason))
                ex.body = res.read()
                ex.parsed_body = json.loads(ex.body)
                ex.response = res
                raise ex
            else:
                res.body = res.read()
                res.parsed_body = json.loads(res.body)
                return res

        raise DatabaseError("Connection to the database failed after %d tries" % (max_tries,))

    def _request_db(self, method, url, query = {}, body = None, headers = {}):
        return self._request(method, '/' + self.dbname + url, query, body, headers)

    def create_db(self, dbname):
        return self._request('PUT', '/' + dbname)

    def delete_db(self, dbname):
        return self._request('DELETE', '/' + dbname)

    def _get_uuids(self, count = 1):
        return self._request('GET', '/_uuids', dict(count = count)).parsed_body['uuids']

    def save(self, model_instance):
        if model_instance.__class__._clsattr("__Embedded__"):
            raise ModelError("Model %s is embedded and cannot be saved"%(model_instance.__class__.__name__,))
        if not model_instance.touch():
            raise ModelError("touch() failed")
        skip = ['_id']
        _id = model_instance._id
        headers = {'Content-type': 'application/json'}
        typename = model_instance.__class__.get_type_name()
        if not _id:
            skip.append('_rev')
            res = self._request_db(
                'POST',
                '/',
                {},
                json.dumps(model_instance._asdict(skip = skip, extra = {'type': typename})),
                headers
            ).parsed_body
        else:
            res = self._request_db(
                'PUT',
                '/' + _id,
                {},
                json.dumps(model_instance._asdict(skip = skip, extra = {'type': typename})),
                headers
            ).parsed_body
        model_instance._id = res['id']
        model_instance._rev = res['rev']
        return model_instance._id

    def get(self, model, _id):
        try:
            data = self._request_db('GET', '/' + _id).parsed_body
            if model is None:
                return data
            else:
                return model(**data)
        except DatabaseError as e:
            if e.response.status == 404:
                raise NotFoundError("Not found: %s == %s" % (model.__class__.__name__ if model else '[None]', _id))
            else:
                raise e

    def find(self, model, view, keys = None):
        # TODO: limit, offset
        method = 'GET'
        body = None
        headers = {}
        if keys:
            body = json.dumps({'keys': keys if isinstance(keys, list) else [keys]})
            method = 'POST'
            headers = {'Content-type': 'application/json'}
        res = self._request_db(method, '/_design/' + model.get_type_name() + '/_view/' + view, {'include_docs': 'true'}, body, headers).parsed_body
        # res looks like: {offset: 0, total_rows: 100, rows: [{doc: {document data}, id: foobar, key: returnedkey, value: emittedvalue}, ...]}
        return [model(**row['doc']) for row in res['rows']]

    def sync_views(self):
        docs = {}
        for model in lingo.Model.__subclasses__():
            views = model._clsattr('__Views__')
            if views:
                id_ = '_design/' + model.get_type_name()
                if id_ not in docs:
                    docs[id_] = {'views': {}}
                docs[id_]['views'].update(views)

        # In order to update we need a _rev so let's get that
        for id_ in docs.keys():
            try:
                realdoc = self.get(None, id_)
                docs[id_]['_rev'] = realdoc['_rev']
            except NotFoundError:
                pass

        # Put the results
        for id_, doc in docs.items():
            if doc and len(doc):
                res = self._request_db(
                    'PUT',
                    '/' + id_,
                    {},
                    json.dumps(doc),
                    {'Content-type': 'application/json'}
                ).parsed_body

import re
import httplib
import urllib
import json
from urlparse import urlparse

import pymongo
import bson

from errors import *

class Base(object):
    @classmethod
    def from_host(self, host, dbname):
        if re.match(ur'^mongodb://', host):
            return MongoDB(pymongo.MongoClient(host), dbname)
        elif re.match(ur'^(couchdb|http)://', host):
            host = re.sub(ur'^couchdb', 'http', host)
            return CouchDB(host, dbname)
        else:
            raise DatabaseError("Invalid protocol, could not determine the type of database")

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

class MongoDB(Base):
    def __init__(self, server, dbname):
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
        if model_instance._id:
            self._getCollection(model_instance.__class__).update({"_id": model_instance._id}, model_instance._asdict(skip=["_id"]), upsert=True, safe=True, multi=False, **kwargs)
        else:
            model_instance._id=self._getCollection(model_instance.__class__).insert(model_instance._asdict(skip=["_id"]), safe=True, **kwargs)
        return model_instance._id

class CouchDB(Base):
    def __init__(self, host, dbname):
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
        self.conn = httplib.HTTPConnection(self.host, self.port)
        self.conn.connect()

        server_info = self._request('GET', '/').parsed_body
        assert 'couchdb' in server_info
        assert server_info['couchdb'] == 'Welcome'

    def _request(self, method, url, query = {}, body = None, headers = {}):
        real_headers = {}
        real_headers.update(self.default_headers)
        real_headers.update(headers)
        self.conn.request(method, url + '?' + urllib.urlencode(query), body, real_headers)
        res = self.conn.getresponse()
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
        skip = ['_id']
        _id = model_instance._id
        headers = {'Content-type': 'application/json'}
        typename = model_instance.__class__._clsattr("__Type__") or model_instance.__class__._clsattr("__Collection__") or model_instance.__class__.__name__
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
            return model(**data)
        except DatabaseError as e:
            if e.response.status == 404:
                raise NotFoundError("Not found: %s == %s" % (model.__class__.__name__, _id))
            else:
                raise e

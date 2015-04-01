import re
import httplib
import urllib
import json
from urlparse import urlparse
import types
import threading
from datetime import datetime
import base64

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

    def preprocess(self, model_instance, data):
        return True

    def attach(self, model_instance, name, file_obj = None, data = None, content_type = None):
        raise DatabaseError("Attachments are not supported by " + self.__class__.__name__)

    def attachments(self, model_instance):
        raise DatabaseError("Attachments are not supported by " + self.__class__.__name__)

    def get_attachment(self, model_instance, name):
        raise DatabaseError("Attachments are not supported by " + self.__class__.__name__)

    def delete_attachment(self, model_instance, name):
        raise DatabaseError("Attachments are not supported by " + self.__class__.__name__)
        

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

class CouchDBViewResult(object):
    def __init__(self, db, model, view, keys = None, limit = None, page = 0, use_startkey = False):
        if use_startkey:
            raise NotImplementedError("Only pagination by limit/offset is supported")

        self.db = db
        self.model = model
        self.view = view
        self.keys = keys
        self.limitnum = limit
        self.pagenum = page

        self._total = None
        self._data = None

    def page(self, pagenum):
        self.pagenum = pagenum
        self._data = None
        return self

    def limit(self, limit):
        self.limitnum = limit
        self._data = None
        return self

    def pages(self):
        if self._total is None:
            self.fetch()
        if self.limitnum is None:
            return 1 if self._total else 0
        return int(self._total / self.limitnum) + (1 if self._total % float(self.limitnum) > 0 else 0)

    def fetch(self):
        method = 'GET'
        body = None
        headers = {}
        if self.keys:
            body = json.dumps({'keys': self.keys if isinstance(self.keys, list) else [self.keys]})
            method = 'POST'
            headers = {'Content-type': 'application/json'}
        query = {'include_docs': 'true'}
        if self.limitnum is not None:
            query.update({
                'limit': self.limitnum,
                'skip': self.pagenum * self.limitnum
            })
        res = self.db._request_db(method, '/_design/' + self.model.get_type_name() + '/_view/' + self.view, query, body, headers).parsed_body
        # res looks like: {offset: 0, total_rows: 100, rows: [{doc: {document data}, id: foobar, key: returnedkey, value: emittedvalue}, ...]}
        self._total = res['total_rows']
        self._data = [self.model(**row['doc']) for row in res['rows']]

        return self

    def _get_data(self):
        if self._data is None:
            self.fetch()
        return self._data

    def __len__(self):
        return len(self._get_data())

    def __getitem__(self, key):
        return self._get_data().__getitem__(key)

    def __iter__(self):
        return self._get_data().__iter__()

    def __contains__(self, item):
        return self._get_data().__contains__(item)

class CouchDB(Database):
    def __init__(self, host, dbname, sync_views = True, name = None):
        super(CouchDB, self).__init__(name)
        res = urlparse(host)
        if res.scheme != 'http':
            raise NotImplementedError("Only the HTTP scheme is supported")
        elif (res.path and res.path != '/') or res.params or res.query or res.fragment:
            raise DatabaseError("Extra information was passed in the URL, which is not supported")
        self.username = res.username or None
        self.password = res.password or None
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

    def _request(self, method, url, query = {}, body = None, headers = {}, parse_body = True):
        real_headers = {}
        real_headers.update(self.default_headers)
        real_headers.update(headers)
        if self.username or self.password:
            real_headers.update({'Authorization': 'Basic %s' % (base64.b64encode('%s:%s' % (self.username or '', self.password or '')))})
        max_tries = 3
        for try_num in range(0, max_tries):
            try:
                qs = ''
                if query is not None:
                    qs = '?' + urllib.urlencode(query)
                self._get_connection().request(method, url + qs, body, real_headers)
                res = self._get_connection().getresponse()
            except (httplib.CannotSendRequest, httplib.BadStatusLine) as e:
                self._get_connection(reconnect = True)
                continue

            if res.status < 200 or res.status >= 400:
                ex = DatabaseError("%d %s" % (res.status, res.reason))
                ex.body = res.read()
                ex.parsed_body = json.loads(ex.body) if parse_body else None
                ex.response = res
                raise ex
            else:
                res.body = res.read()
                res.parsed_body = json.loads(res.body) if parse_body else None
                return res

        raise DatabaseError("Connection to the database failed after %d tries" % (max_tries,))

    def _request_db(self, method, url, query = {}, body = None, headers = {}, parse_body = True):
        return self._request(method, '/' + self.dbname + url, query, body, headers, parse_body)

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
        attachment_stubs = {'_attachments': {k: v._asdict() for k, v in model_instance._attachments.items() if not v._new}}
        if not _id:
            skip.append('_rev')
            data = model_instance._asdict(skip = skip, extra = {'type': typename})
            data.update(attachment_stubs)
            res = self._request_db(
                'POST',
                '/',
                {},
                json.dumps(data),
                headers
            ).parsed_body
        else:
            data = model_instance._asdict(skip = skip, extra = {'type': typename})
            data.update(attachment_stubs)
            res = self._request_db(
                'PUT',
                '/' + _id,
                {},
                json.dumps(data),
                headers
            ).parsed_body
        model_instance._id = res['id']
        model_instance._rev = res['rev']
        deleted_attachments = []
        for attachment in model_instance._attachments.values():
            if attachment._new:
                res = self._request_db('PUT', '/' + model_instance._id + '/' + attachment.name, {'rev': model_instance._rev}, attachment.data)
                attachment._new = False
            elif attachment._deleted:
                res = self._request_db('DELETE', '/' + model_instance._id + '/' + attachment.name, {'rev': model_instance._rev})
                deleted_attachments.append(attachment.name)
            else:
                continue
            model_instance._rev = res.parsed_body['rev']
        for name in deleted_attachments:
            del model_instance._attachments[name]
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
        return CouchDBViewResult(self, model, view, keys)

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

    def create_admin(self, username, password):
        return self._request('PUT', '/_config/admins/' + username, None, '"%s"' % (password,))

    def create_user(self, username, password, roles = []):
        return self._request('POST', '/_users', None, json.dumps(dict(
            id = 'org.couchdb.user:' + username,
            name = username,
            roles = roles,
            password = password
        )))

    def delete_admin(self, username):
        return self._request('DELETE', '/_config/admins/' + username)

    def delete_user(self, username):
        return self._request('DELETE', '/_users/org.couchdb.user:' + username)

    def delete_document(self, docid, revid):
        query = {'rev': revid} if revid else None
        return self._request_db('DELETE', '/' + docid, query)

    def delete(self, model_instance):
        if model_instance.__class__._clsattr("__Embedded__"):
            raise ModelError("Model %s is embedded and cannot be saved"%(model_instance.__class__.__name__,))
        if model_instance._id:
            return self.delete_document(model_instance._id, model_instance._rev)

    def preprocess(self, model_instance, data):
        # Bypass strict mode
        model_instance.__dict__['_attachments'] = {}
        if '_attachments' in data:
            for k, v in data['_attachments'].items():
                model_instance._attachments[k] = lingo.Attachment(model_instance, k, _new = False, **v)
            del data['_attachments']
        return True

    def attach(self, model_instance, name, file_obj = None, data = None, content_type = None):
        model_instance._attachments[name] = lingo.Attachment.create(model_instance, name, file_obj, data, content_type)

    def attachments(self, model_instance):
        return model_instance._attachments

    def get_attachment(self, model_instance, name):
        # Intentionally raise KeyError if the attachment doesn't exist
        a = model_instance._attachments[name]
        if a.stub or not a.data:
            return self._request_db('GET', '/' + model_instance._id + '/' + a.name, parse_body = False).body
        return a.body

    def delete_attachment(self, model_instance, name):
        if name in model_instance._attachments:
            if not model_instance._attachments[name]._new:
                model_instance._attachments[name]._deleted = True
            else:
                del model_instance._attachments[name]
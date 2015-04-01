import functools
import inspect
from datetime import datetime
import mimetypes
import base64
import logging

import bson

from dateutil.parser import parse
import pytz

from errors import *
import database

log = logging.getLogger(__name__)

class combomethod(object):
	def __init__(self, method):
		self.method = method

	def __get__(self, obj=None, objtype=None):
		@functools.wraps(self.method)
		def _wrapper(*args, **kwargs):
			if obj is not None:
				return self.method(obj, *args, **kwargs)
			else:
				return self.method(objtype, *args, **kwargs)
		return _wrapper

class Field(object):
	def __init__(self, ftype=None, fsubtype=None, validation=None, default=None, doc=None):
		self.ftype=ftype
		self.fsubtype=fsubtype
		self.validation=validation
		self.default=default
		self.doc=doc

	@classmethod
	def _scalar_to_python(self, ftype, value):
		# ftype and value must not be None
		if inspect.isclass(ftype) and isinstance(value, ftype):
			return value
		elif isinstance(ftype, Field):
			return ftype._to_python(value)
		elif issubclass(ftype, Model):
			return ftype(**value)
		elif issubclass(ftype, datetime):
			out = parse(str(value))
			if out.tzinfo is None or out.tzinfo.utcoffset(out) is None:
				out = out.replace(tzinfo = pytz.timezone('UTC'))
			return out
		else:
			return ftype(value)

	def _to_python(self, value_):
		value = value_
		if self.ftype is not None and value is not None:
			if issubclass(self.ftype, list):
				value = value if isinstance(value, list) else [value]
				value = [self._scalar_to_python(self.fsubtype, v) for v in value]
			elif issubclass(self.ftype, dict):
				value = value if isinstance(value, dict) else {str(value): value}
				value = {k:self._scalar_to_python(self.fsubtype, v) for k,v in value.items()}
			else:
				value = self._scalar_to_python(self.ftype, value)
		return value

	@classmethod
	def _scalar_to_json(self, ftype, value):
		# ftype and value must not be None
		if isinstance(ftype, Field):
			return ftype._to_json(value)
		elif issubclass(ftype, Model):
			if ftype._clsattr('__Embedded__'):
				return value._to_json()
			else:
				return str(value._id)
		elif issubclass(ftype, datetime):
			if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
				value = value.replace(tzinfo = pytz.timezone('UTC'))
			return value.isoformat()
		else:
			return value

	def _to_json(self, value_):
		value = value_
		if self.ftype is not None and value is not None:
			if issubclass(self.ftype, list):
				value = [self._scalar_to_json(self.fsubtype, v) for v in value]
			elif issubclass(self.ftype, dict):
				value = {k:self._scalar_to_json(self.fsubtype, v) for k,v in value.items()}
			else:
				value = self._scalar_to_json(self.ftype, value)
		return value

	def validate(self, value_):
		value = self._to_python(value_)
		if self.validation:
			self.validation(value) #Should raise ValidationError on failure
		return value

class Attachment(object):
	def __init__(self, doc, name, **kwargs):
		self.doc = doc
		self.name = name
		self.content_type = None
		self.data = None
		self.length = None
		self.stub = None

		self._new = True
		self._deleted = False
		for k, v in kwargs.items():
			setattr(self, k, v)

	@classmethod
	def create(self, doc, name, file_obj = None, data = None, content_type = None):
		if not (file_obj or data):
			raise ModelError("Either a file-like object or a string of data is required")
		return self(doc, name, content_type = content_type or mimetypes.guess_type(name)[0] or 'application/octet-stream', data = data or file_obj.read(), stub = False)

	def read(self):
		if self.data:
			return self.data
		return self.doc.database().get_attachment(self.name)

	def _asdict(self, with_data = False):
		out = dict(
			content_type = self.content_type,
		)
		if with_data and not self.stub and self.data:
			out['data'] = base64.b64encode(self.data)
		else:
			out['stub'] = True
		return out

class Model(object):
	class __DefaultPrototype__:
		__Strict__=False	#If true, assignments to attributes not defined in __Prototype__ are an error
		__Embedded__=False	#If true, this type of model only exists within other models

	@classmethod
	def _clsattr(self, attrName):
		if hasattr(self, "__Prototype__"):
			if hasattr(self.__Prototype__, attrName):
				return getattr(self.__Prototype__, attrName)
			elif hasattr(self.__DefaultPrototype__, attrName):
				return getattr(self.__DefaultPrototype__, attrName)
			else:
				return None
		else:
			raise ModelError("Model %s lacks a prototype"%(self.__name__,))

	@combomethod
	def database(self):
		if self is not Model:
			if hasattr(self, '_database'):
				return database.DatabasePartial(self, self._database)
		cls = self if inspect.isclass(self) and issubclass(self, Model) else self.__class__
		db_string = cls._clsattr('__Database__')
		if db_string is None:
			raise ModelError("The prototype of class '%s' does not define __Database__" % (cls.__name__,))
		return database.DatabasePartial(self, database.Database.get_instance(db_string))

	@classmethod
	def _fields(self):
		if hasattr(self, "__Prototype__"):
			return {k:v for k,v in self.__Prototype__.__dict__.items() if isinstance(v, Field)}
		else:
			raise ModelError("Model %s lacks a prototype"%(self.__name__,))

	@classmethod
	def get_type_name(self):
		return self._clsattr("__Type__") or self._clsattr("__Collection__") or self.__name__

	def __init__(self, **kwargs):
		self.__data__={}
		try:
			self.database().preprocess(kwargs)
		except Exception as e:
			log.error("Can't preprocess: %s: %s" % (e.__class__.__name__, str(e)))
		for k,v in self.__class__._fields().items():
			if k in kwargs:
				setattr(self, k, kwargs[k])
			else:
				setattr(self, k, v.default() if callable(v.default) else v.default)

	def touch(self):
		"""\
		Used to implement updated timestamps.  What happens here is the responsibility of the subclass.
		Return not True to prevent saving
		"""
		return True

	def __setattr__(self, k, v):
		if isinstance(self._clsattr(k), Field):
			try:
				v=self._clsattr(k).validate(v)
			except ValidationError as e:
				raise ValidationError("%s.%s: %s"%(self.__class__.__name__, k, str(e)))
			self.__dict__['__data__'][k]=v
		elif (k in self.__dict__) or (not self._clsattr("__Strict__")):
			self.__dict__[k]=v
		else:
			raise ValidationError("%s: No such attribute %s"%(self.__class__.__name__, k))

	def __getattr__(self, k):
		# Special methods that interact with the database
		if k in ['save', 'attach', 'attachments', 'get_attachment', 'delete_attachment']:
			def wrapped_special_method(*args, **kwargs):
				return getattr(self.database(), k)(*args, **kwargs)
			return wrapped_special_method

		f=self.__class__._clsattr(k)
		if isinstance(f, Field):
			out=self.__dict__['__data__'][k]
			if issubclass(f.ftype, Model) and not isinstance(out, f.ftype) and out is not None:
				#Requested field should be a model instance, but it is not - load it
				if f.ftype._clsattr("__Embedded__"):
					out=f.ftype(**out)
				else:
					#A load from the database is required.
					pass #TODO: load from the database
				setattr(self, k, out)
			return out
		else:
			return self.__dict__[k]

	def _to_json(self, skip=None, extra = {}):
		out={}
		for k,v in self.__dict__['__data__'].items():
			if skip and k in skip:
				continue
			out[k] = self._clsattr(k)._to_json(v)
		out.update(extra)
		return out

	def _asdict(self, skip=None, extra = {}):
		return self._to_json(skip, extra)

import bson

from errors import *
from database import MongoDBCustomCursor

class Field(object):
	def __init__(self, ftype=None, fsubtype=None, validation=None, default=None, doc=None, cast=True):
		self.ftype=ftype
		self.fsubtype=fsubtype
		self.validation=validation
		self.default=default
		self.doc=doc
		self.cast=cast

	def _validate(self, ftype, value):
		if ftype is not None:
			if not isinstance(value, ftype) and value is not None:
				if self.cast:
					try:
						if issubclass(ftype, Model):
							value=ftype(**value)
						else:
							value=ftype(value)
					except TypeError as e:
						raise ValidationError("%s(%s): %s"%(str(ftype), str(value), str(e)))
				else:
					raise ValidationError("Expected %s, got %s"%(ftype.__name__, value.__class__.__name__))
		return value

	def validate(self, value):
		if issubclass(self.ftype, list):
			self._validate(self.ftype, value)
			value=[self._validate(self.fsubtype, v) for v in value]
			if self.validation:
				for v in value:
					self.validation(v) #Should raise ValidationError on failure
		else:
			value=self._validate(self.ftype, value)
			if self.validation:
				self.validation(value)
		return value

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

	@classmethod
	def _fields(self):
		if hasattr(self, "__Prototype__"):
			return {k:v for k,v in self.__Prototype__.__dict__.items() if isinstance(v, Field)}
		else:
			raise ModelError("Model %s lacks a prototype"%(self.__name__,))

	def __init__(self, **kwargs):
		self.__data__={}
		for k,v in self.__class__._fields().items():
			if k in kwargs:
				setattr(self, k, kwargs[k])
			else:
				setattr(self, k, v.default() if callable(v.default) else v.default)

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

	def _asdict(self, skip=None):
		out={}
		for k,v in self.__dict__['__data__'].items():
			if skip and k in skip:
				continue
			if isinstance(v, Model):
				if v.__class__._clsattr("__Embedded__"):
					v=v._asdict()
				else:
					v=str(v._id)
			out[k]=v
		return out

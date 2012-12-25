import unittest, pymongo, bson
import lingo

class SampleEmbeddedModel(lingo.Model):
	class __Prototype__:
		__Embedded__=True
		strField=lingo.Field(unicode, default=u"")
		intField=lingo.Field(int, default=0)

class SampleModel(lingo.Model):
	class __Prototype__:
		_id=lingo.Field(bson.ObjectId)
		strField=lingo.Field(unicode, default=u"")
		embedField=lingo.Field(SampleEmbeddedModel, default=SampleEmbeddedModel)

SampleModel.__Prototype__.linkField=lingo.Field(SampleModel, default=None, cast=False)

class TestLingo(unittest.TestCase):
	def setUp(self):
		self.conn=pymongo.Connection("localhost")
		self.db=self.conn["lingo-test"]

	def test_FieldValidationWithCasting(self):
		f=lingo.Field(int, default=0)
		
		self.assertEquals(f.validate(3), 3)
		self.assertEquals(f.validate(250), 250)
		self.assertEquals(f.validate(-23874), -23874)
		self.assertEquals(f.validate("12"), 12)
		self.assertEquals(f.validate(True), 1)

	def test_FieldValidationNoCasting(self):
		f=lingo.Field(int, default=0, cast=False)

		self.assertEquals(f.validate(3), 3)
		self.assertEquals(f.validate(250), 250)
		self.assertEquals(f.validate(-23874), -23874)
		
		with self.assertRaises(lingo.ValidationError):
			f.validate("12")

	def _numInRange(self, minimum, maximum):
		def _internal(num):
			if num<minimum:
				raise lingo.ValidationError("Number must be greater than %d"%(minimum-1,))
			if num>maximum:
				raise lingo.ValidationError("Number must be less than %d"%(maximum+1,))
		return _internal

	def test_FieldExtendedValidation(self):
		f=lingo.Field(int, default=0, validation=self._numInRange(0, 10))

		self.assertEquals(f.validate(0), 0)
		self.assertEquals(f.validate(7), 7)
		self.assertEquals(f.validate(10), 10)

		with self.assertRaises(lingo.ValidationError):
			f.validate(-3)

		with self.assertRaises(lingo.ValidationError):
			f.validate(11)

	def test_ModelInstantiation_Empty(self):
		i=SampleModel()
		self.assertEquals(i.strField, u"")
		self.assertEquals(i.embedField.strField, u"")
		self.assertEquals(i.embedField.intField, 0)

	def test_ModelInstantiation_KWArgs(self):
		i=SampleModel(strField="bar")
		self.assertEquals(i.strField, "bar")

	def test_Setattr(self):
		i=SampleModel()
		i.embedField.intField="12"
		self.assertEquals(i.embedField.intField, 12)

	def test_getDefaultCollection(self):
		self.assertEquals(SampleModel._getCollection(self.db), self.db["SampleModel"])

	def test_getExplicitCollection(self):
		SampleModel.__Prototype__.__Collection__="foobar"
		self.assertEquals(SampleModel._getCollection(self.db), self.db["foobar"])
		del(SampleModel.__Prototype__.__Collection__)

	def test_CannotSaveEmbeddedModels(self):
		i=SampleEmbeddedModel()
		with self.assertRaises(lingo.ModelError):
			i.save(self.db)

	def test_NewObjectId(self):
		i=SampleModel()
		i.save(self.db)
		self.assertIsInstance(i._id, bson.ObjectId)
		self.assertGreater(len(str(i._id)), 0)

if __name__=="__main__":
	unittest.main()
import unittest, pymongo, bson
from lingo import lingo, database

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

class TestMongoDB(unittest.TestCase):
	def setUp(self):
		database.Database.instances = {}
		self.conn=pymongo.Connection("localhost")
		self.db=self.conn["lingo-test"]
		self.db.SampleModel.remove()
		self.mdb = database.MongoDB(self.conn, 'lingo-test')

	def test_getDefaultCollection(self):
		self.assertEquals(self.mdb._getCollection(SampleModel), self.db["SampleModel"])

	def test_getExplicitCollection(self):
		SampleModel.__Prototype__.__Collection__="foobar"
		self.assertEquals(self.mdb._getCollection(SampleModel), self.db["foobar"])
		del(SampleModel.__Prototype__.__Collection__)

	def test_CannotSaveEmbeddedModels(self):
		i=SampleEmbeddedModel()
		with self.assertRaises(lingo.ModelError):
			self.mdb.save(i)

	def test_NewObjectId(self):
		i=SampleModel()
		self.mdb.save(i)
		self.assertIsInstance(i._id, bson.ObjectId)
		self.assertGreater(len(str(i._id)), 0)

	def test_SaveNew(self):
		i=SampleModel()
		i.strField="foobar"
		self.assertIsNone(i._id)
		self.mdb.save(i)
		self.assertIsNotNone(i._id)
		self.tempid=str(i._id)

	def test_FindExisting(self):
		i=SampleModel(strField="foobar")
		self.mdb.save(i)
		tempid=str(i._id)
		
		del(i)
		i=self.mdb.find(SampleModel, {"_id": bson.ObjectId(tempid)})[0]
		self.assertIsNotNone(i._id)
		self.assertEquals(tempid, str(i._id))
		self.assertEquals(i.strField, u"foobar")

	def test_FindOneExisting(self):
		i=SampleModel(strField="foobar")
		self.mdb.save(i)
		tempid=str(i._id)
		
		del(i)
		i=self.mdb.one(SampleModel, {"_id": bson.ObjectId(tempid)})
		self.assertIsNotNone(i._id)
		self.assertEquals(tempid, str(i._id))
		self.assertEquals(i.strField, u"foobar")

	def test_FindOneMissingExisting(self):
		i=SampleModel(strField="foobar")
		self.mdb.save(i)
		tempid=str(i._id)
		
		del(i)
		with self.assertRaises(lingo.ValidationError):
			i=self.mdb.one(SampleModel, {"_id": bson.ObjectId()})

	def test_GetExistingWithBSONID(self):
		i=SampleModel(strField="foobar")
		self.mdb.save(i)
		tempid=str(i._id)
		
		del(i)
		i=self.mdb.get(SampleModel, bson.ObjectId(tempid))
		self.assertIsNotNone(i._id)
		self.assertEquals(tempid, str(i._id))
		self.assertEquals(i.strField, u"foobar")

	def test_GetExistingWithString(self):
		i=SampleModel(strField="foobar")
		self.mdb.save(i)
		tempid=str(i._id)
		
		del(i)
		i=self.mdb.get(SampleModel, tempid)
		self.assertIsNotNone(i._id)
		self.assertEquals(tempid, str(i._id))
		self.assertEquals(i.strField, u"foobar")

	def test_SaveExisting(self):
		i=SampleModel(strField="foobar")
		self.mdb.save(i)
		tempid=str(i._id)
		del(i)
		i=self.mdb.find(SampleModel, {"_id": bson.ObjectId(tempid)})[0]
		self.mdb.save(i)
		self.assertEquals(tempid, str(i._id))

if __name__=="__main__":
	unittest.main()
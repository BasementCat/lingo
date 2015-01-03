import time
import logging
logging.basicConfig()

import unittest#, pymongo, bson
from lingo import lingo, database

class SampleEmbeddedModel(lingo.Model):
	class __Prototype__:
		__Embedded__=True
		strField=lingo.Field(unicode, default=u"")
		intField=lingo.Field(int, default=0)

class SampleModel(lingo.Model):
	class __Prototype__:
		__Database__ = 'CouchDB'
		_id=lingo.Field(unicode)
		_rev=lingo.Field(unicode)
		strField=lingo.Field(unicode, default=u"")
		embedField=lingo.Field(SampleEmbeddedModel, default=SampleEmbeddedModel)

		__Views__ = {
			'getByStrField': {
				'map': """\
					function(doc) {
						if (doc.type == "SampleModel") {
							emit(doc.strField, doc.id);
						}
					}
				"""
			}
		}

SampleModel.__Prototype__.linkField=lingo.Field(SampleModel, default=None, cast=False)

class TestCouchDB(unittest.TestCase):
	def setUp(self):
		database.Database.instances = {}
		self.db = database.CouchDB('http://localhost', 'lingo-test', sync_views = False)
		try:
			self.db.delete_db('lingo-test')
		except:
			pass
		self.db.create_db('lingo-test')
		self.db.sync_views()

	def test_CannotSaveEmbeddedModels(self):
		i=SampleEmbeddedModel()
		with self.assertRaises(lingo.ModelError):
			i.database().save()

	def test_NewObjectId(self):
		i=SampleModel()
		i.database().save()
		self.assertGreater(len(i._id), 0)
		self.assertGreater(len(i._rev), 0)

	def test_SaveNew(self):
		i=SampleModel()
		i.strField="foobar"
		self.assertIsNone(i._id)
		i.database().save()
		self.assertIsNotNone(i._id)
		self.assertIsNotNone(i._rev)
		self.tempid=i._id

	def test_FindAll(self):
		for v in ["foo", "foo", "bar", "baz"]:
			i=SampleModel(strField=v)
			i.database().save()

		res = SampleModel.database().find('getByStrField')
		self.assertEquals(4, len(res))

	def test_FindOneKey(self):
		for v in ["foo", "foo", "bar", "baz"]:
			i=SampleModel(strField=v)
			i.database().save()

		res = SampleModel.database().find('getByStrField', 'foo')
		self.assertEquals(2, len(res))
		for obj in res:
			self.assertEquals('foo', obj.strField)

		res = SampleModel.database().find('getByStrField', 'bar')
		self.assertEquals(1, len(res))
		for obj in res:
			self.assertEquals('bar', obj.strField)

		res = SampleModel.database().find('getByStrField', 'baz')
		self.assertEquals(1, len(res))
		for obj in res:
			self.assertEquals('baz', obj.strField)

	def test_FindMultipleKeys(self):
		for v in ["foo", "foo", "bar", "baz"]:
			i=SampleModel(strField=v)
			i.database().save()

		res = SampleModel.database().find('getByStrField', ['foo', 'bar'])
		self.assertEquals(3, len(res))
		for obj in res:
			self.assertTrue(obj.strField in ['foo', 'bar'])

	def test_FindMissing(self):
		res = SampleModel.database().find('getByStrField', 'notarealkey')
		self.assertEquals(0, len(res))

	def test_FindMissingView(self):
		with self.assertRaises(lingo.DatabaseError):
			res = SampleModel.database().find('notarealview')
		
	def test_GetMissing(self):
		with self.assertRaises(lingo.NotFoundError):
			i = SampleModel.database().get("thisiddoesnotexistever")
			self.assertIsNone(i)

	def test_GetExistingWithString(self):
		i=SampleModel(strField="foobar")
		i.database().save()
		tempid=i._id
		
		del(i)
		i=SampleModel.database().get(tempid)
		self.assertIsNotNone(i._id)
		self.assertIsNotNone(i._rev)
		self.assertEquals(tempid, i._id)
		self.assertEquals(i.strField, u"foobar")

	def test_SaveExisting(self):
		i=SampleModel(strField="foobar")
		i.database().save()
		tempid=i._id
		del(i)
		i=SampleModel.database().get(tempid)
		self.assertEquals(tempid, i._id)

	def test_MassOperation(self):
		objs = []
		t_new = 0.0
		t_save = 0.0
		t_get = 0.0
		t_start = time.time()
		for i in range(0, 100):
			v = SampleModel(strField = "Test %d" % (i,))
			ts = time.time()
			self.db.save(v)
			t_new += time.time() - ts
			objs.append(v)

		for obj in objs:
			obj.embedField.strField = "asdf"
			ts = time.time()
			self.db.save(obj)
			t_save += time.time() - ts

		for obj in objs:
			ts = time.time()
			temp = SampleModel.database().get(obj._id)
			t_get += time.time() - ts

		t_duration = time.time() - t_start
		import sys
		sys.stderr.write("\nTIMINGS: %fs total, %fs in %d ops, %fs lost. new: %fs (%fms per), save: %fs (%fms per), get: %fs (%fms per)\n" %(
			t_duration,
			t_new + t_save + t_get,
			len(objs),
			t_duration - (t_new + t_save + t_get),
			t_new,
			(t_new / float(len(objs))) * 1000,
			t_save,
			(t_save / float(len(objs))) * 1000,
			t_get,
			(t_get / float(len(objs))) * 1000
			))

if __name__=="__main__":
	unittest.main()
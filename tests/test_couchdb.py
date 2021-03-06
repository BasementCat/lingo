import time
import logging
from StringIO import StringIO
logging.basicConfig()

import unittest#, pymongo, bson
from lingo import lingo, database, errors

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

class SampleModel2(lingo.Model):
	class __Prototype__:
		__Database__ = 'CouchDB'
		_id=lingo.Field(unicode)
		_rev=lingo.Field(unicode)
		strField=lingo.Field(unicode, default=u"")
		embedField=lingo.Field(SampleEmbeddedModel, default=SampleEmbeddedModel)

		__Views__ = {
			'getAll': {
				'map': """\
					function(doc) {
						emit(doc.id, doc.id);
					}
				"""
			}
		}

SampleModel.__Prototype__.linkField=lingo.Field(SampleModel, default=None)

class TouchableModel(lingo.Model):
	class __Prototype__:
		__Database__ = 'CouchDB'
		_id=lingo.Field(unicode)
		strField=lingo.Field(unicode, default=u"")
	
	def touch(self):
		self.strField = u'touched'
		return True

class TestCouchDB(unittest.TestCase):
	def setUp(self):
		try:
			db = database.CouchDB('http://adminuser:password@localhost', 'lingo-test', sync_views = False, name = 'setup_temp')
			db.delete_admin('adminuser')
		except database.DatabaseError:
			pass
		database.Database.instances = {}
		self.db = database.CouchDB('http://localhost', 'lingo-test', sync_views = False)
		try:
			self.db.delete_db('lingo-test')
		except:
			pass
		try:
			self.db.delete_db('lingo-test-auth')
		except:
			pass
		self.db.create_db('lingo-test')
		self.db.sync_views()

	def test_Authentication(self):
		self.db.create_admin('adminuser', 'password')
		try:
			self.db.create_db('lingo-test-auth')
			self.assertTrue(False, "Was able to create a database without being an admin")
		except database.DatabaseError:
			db = database.CouchDB('http://adminuser:password@localhost', 'lingo-test', sync_views = False, name = 'auth_test')
			try:
				db.create_db('lingo-test-auth')
				db.delete_db('lingo-test-auth')
			except:
				self.assertTrue(False, "Was not able to create a database as admin")

	def test_CannotSaveEmbeddedModels(self):
		i=SampleEmbeddedModel()
		with self.assertRaises(lingo.ModelError):
			i.database().save()

	def test_CannotDeleteEmbeddedModels(self):
		i=SampleEmbeddedModel()
		with self.assertRaises(lingo.ModelError):
			i.database().delete()

	def test_NewObjectId(self):
		i=SampleModel()
		i.database().save()
		self.assertGreater(len(i._id), 0)
		self.assertGreater(len(i._rev), 0)

	def test_Touch(self):
		i=TouchableModel()
		self.assertEquals(i.strField, u'')
		i.database().save()
		self.assertEquals(i.strField, u'touched')

	def test_delete(self):
		i=SampleModel()
		i.database().save()
		self.assertGreater(len(i._id), 0)
		self.assertGreater(len(i._rev), 0)
		i.database().delete()
		with self.assertRaises(errors.NotFoundError):
			i2 = SampleModel.database().get(i._id)

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

	def test_ModelMap(self):
		SampleModel(strField='SampleModel').database().save()
		SampleModel2(strField='SampleModel2').database().save()
		res = SampleModel.database().find('getByStrField')
		for obj in res:
			self.assertEquals(obj.__class__.__name__, obj.strField)

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
			# Use len() to force the data to be retrieved
			res = len(SampleModel.database().find('notarealview'))
		
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

	def test_Pagination(self):
		fields = []
		for i in range(0, 100):
			v = SampleModel(strField = "Test %d" % (i,))
			self.db.save(v)
			fields.append(v.strField)

		res = SampleModel.database().find('getByStrField')
		res.limit(19)
		self.assertEquals(res.pages(), 6)
		for pagenum in range(0, 6):
			res.page(pagenum)
			if pagenum == 5:
				self.assertEquals(len(res), 5)
			else:
				self.assertEquals(len(res), 19)

			for obj in res:
				self.assertTrue(obj.strField in fields)

	def test_SaveAndGetAttachment_String(self):
		i=SampleModel(strField="foobar")
		i.attach('test.txt', data = "hello world")
		i.database().save()
		tempid=i._id
		
		del(i)
		i=SampleModel.database().get(tempid)
		self.assertEquals("hello world", i.get_attachment('test.txt'))

	def test_ReadAttachment(self):
		i=SampleModel(strField="foobar")
		i.attach('test.txt', data = "hello world")
		i.database().save()
		tempid=i._id
		
		del(i)
		i=SampleModel.database().get(tempid)
		self.assertEquals("hello world", i.attachments()['test.txt'].read())

	def test_SaveAndGetAttachment_Resave(self):
		i=SampleModel(strField="foobar")
		i.attach('test.txt', data = "hello world")
		i.database().save()
		tempid=i._id
		
		del(i)
		i=SampleModel.database().get(tempid)
		self.assertEquals("hello world", i.get_attachment('test.txt'))
		i.strField = "barbaz"
		i.database().save()

		del(i)
		i=SampleModel.database().get(tempid)
		self.assertEquals("hello world", i.get_attachment('test.txt'))

	def test_SaveAndGetAttachment_File(self):
		i=SampleModel(strField="foobar")
		i.attach('test.txt', file_obj = StringIO("hello world"))
		i.database().save()
		tempid=i._id
		
		del(i)
		i=SampleModel.database().get(tempid)
		self.assertEquals("hello world", i.get_attachment('test.txt'))

	def test_DeleteAttachment(self):
		i=SampleModel(strField="foobar")
		i.attach('test.txt', data = "hello world")
		i.database().save()
		tempid=i._id
		
		del(i)
		i=SampleModel.database().get(tempid)
		self.assertEquals("hello world", i.get_attachment('test.txt'))
		i.strField = "barbaz"
		i.database().save()

		del(i)
		i=SampleModel.database().get(tempid)
		i.delete_attachment('test.txt')
		i.database().save()

		del(i)
		i=SampleModel.database().get(tempid)
		with self.assertRaises(KeyError):
			i.get_attachment('test.txt')

	def test_SaveAndGetAttachment_String_BinaryData(self):
		i=SampleModel(strField="foobar")
		i.attach('test.png', data = "\x89PNG\r\nhello world")
		i.database().save()
		tempid=i._id
		
		del(i)
		i=SampleModel.database().get(tempid)
		self.assertEquals("\x89PNG\r\nhello world", i.get_attachment('test.png'))

if __name__=="__main__":
	unittest.main()
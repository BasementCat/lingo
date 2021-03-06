import unittest, pymongo, bson, pytz
from datetime import datetime
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

SampleModel.__Prototype__.linkField=lingo.Field(SampleModel, default=None)

class SampleNestedModel(lingo.Model):
    class __Prototype__:
        nestedField = lingo.Field(dict, lingo.Field(list, SampleEmbeddedModel))

class TestFieldsModels(unittest.TestCase):
    def test_FieldValidation(self):
        f=lingo.Field(int, default=0)
        
        self.assertEquals(f.validate(3), 3)
        self.assertEquals(f.validate(250), 250)
        self.assertEquals(f.validate(-23874), -23874)
        self.assertEquals(f.validate("12"), 12)
        self.assertEquals(f.validate(True), 1)

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

    def test_NestedToPython(self):
        i = SampleNestedModel(
            nestedField = dict(
                foo = [
                    dict(
                        strField = 'foo',
                        intField = 3
                    )
                ],
                bar = [
                    dict(
                        strField = 'bar',
                        intField = 5
                    )
                ]
            )
        )

        self.assertEquals('foo', i.nestedField['foo'][0].strField)
        self.assertEquals(3, i.nestedField['foo'][0].intField)
        self.assertEquals('bar', i.nestedField['bar'][0].strField)
        self.assertEquals(5, i.nestedField['bar'][0].intField)

    def test_NestedToJson(self):
        i = SampleNestedModel(
            nestedField = dict(
                foo = [
                    dict(
                        strField = 'foo',
                        intField = 3
                    )
                ],
                bar = [
                    dict(
                        strField = 'bar',
                        intField = 5
                    )
                ]
            )
        )
        i = i._to_json()

        self.assertEquals('foo', i['nestedField']['foo'][0]['strField'])
        self.assertEquals(3, i['nestedField']['foo'][0]['intField'])
        self.assertEquals('bar', i['nestedField']['bar'][0]['strField'])
        self.assertEquals(5, i['nestedField']['bar'][0]['intField'])

    def test_NaiveDatetimeFieldToJson(self):
        f = lingo.Field(datetime)
        dt = datetime(2015, 1, 4, 5, 47, 45, 578293)
        self.assertEquals('2015-01-04T05:47:45.578293+00:00', f._to_json(dt))

    def test_AwareDatetimeFieldToJson(self):
        f = lingo.Field(datetime)
        dt = datetime(2015, 1, 4, 5, 47, 45, 578293).replace(tzinfo = pytz.timezone("UTC"))
        self.assertEquals('2015-01-04T05:47:45.578293+00:00', f._to_json(dt))

    def test_DatetimeStringToPython_Format1(self):
        f = lingo.Field(datetime)
        dt = datetime(2015, 1, 4, 5, 47, 45, 578293).replace(tzinfo = pytz.timezone("UTC"))
        self.assertEquals(dt, f._to_python('2015-01-04T05:47:45.578293+00:00'))
        self.assertEquals('UTC', dt.tzname())

    def test_DatetimeStringToPython_Format2(self):
        f = lingo.Field(datetime)
        dt = datetime(2015, 1, 4, 5, 47, 45, 578293).replace(tzinfo = pytz.timezone("UTC"))
        self.assertEquals(dt, f._to_python('2015-01-04T05:47:45.578293Z'))
        self.assertEquals('UTC', dt.tzname())

    def test_DatetimeStringToPython_Format3(self):
        f = lingo.Field(datetime)
        dt = datetime(2015, 1, 4, 5, 47, 45).replace(tzinfo = pytz.timezone("UTC"))
        self.assertEquals(dt, f._to_python('2015-01-04T05:47:45+00:00'))
        self.assertEquals('UTC', dt.tzname())

    def test_DatetimeStringToPython_Format4(self):
        f = lingo.Field(datetime)
        dt = datetime(2015, 1, 4, 5, 47, 45).replace(tzinfo = pytz.timezone("UTC"))
        self.assertEquals(dt, f._to_python('2015-01-04T05:47:45Z'))
        self.assertEquals('UTC', dt.tzname())

    def test_DatetimeStringToPython_Format5(self):
        f = lingo.Field(datetime)
        dt = datetime(2015, 1, 4, 5, 47, 45, 578293).replace(tzinfo = pytz.timezone("UTC"))
        self.assertEquals(dt, f._to_python('2015-01-04T05:47:45.578293'))
        self.assertEquals('UTC', dt.tzname())

    def test_DatetimeStringToPython_Format6(self):
        f = lingo.Field(datetime)
        dt = datetime(2015, 1, 4, 5, 47, 45).replace(tzinfo = pytz.timezone("UTC"))
        self.assertEquals(dt, f._to_python('2015-01-04T05:47:45'))
        self.assertEquals('UTC', dt.tzname())

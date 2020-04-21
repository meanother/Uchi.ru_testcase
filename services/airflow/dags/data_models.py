from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.models import Model
from infi.clickhouse_orm.fields import *
from infi.clickhouse_orm.engines import MergeTree, TinyLog


class Events_buf(Model):
    ts = UInt64Field()  #
    userId = StringField()
    sessionId = UInt16Field()  #
    page = StringField()
    auth = StringField()
    # method = StringField()
    method = NullableField(StringField())
    status = NullableField(UInt16Field())
    # status = UInt16Field()
    level = StringField()
    itemInSession = NullableField(UInt16Field())
    # itemInSession = UInt16Field()
    # location = StringField()
    location = NullableField(StringField())
    userAgent = NullableField(StringField())
    # userAgent = StringField()
    lastName = NullableField(StringField())
    # lastName = StringField()
    firstName = NullableField(StringField())
    # firstName = StringField()
    registration = UInt64Field()
    gender = NullableField(StringField())
    # gender = StringField()
    artist = NullableField(StringField())
    # artist = StringField()
    song = NullableField(StringField())
    # song = StringField()
    # length = NullableField(Float32Field())
    length = Float32Field()

    engine = TinyLog()


class Events(Model):
    ts = DateTimeField()
    userId = StringField()
    sessionId = UInt16Field()
    page = FixedStringField(30)
    auth = FixedStringField(20)
    method = FixedStringField(10)
    status = UInt16Field()
    level = FixedStringField(10)
    itemSession = UInt16Field()
    location = NullableField(StringField())
    userAgent = NullableField(StringField())
    lastname = NullableField(FixedStringField(300))
    firstname = NullableField(FixedStringField(300))
    registration = UInt64Field()
    gender = NullableField(FixedStringField(2))
    artist = NullableField(FixedStringField(500))
    song = NullableField(StringField())
    length = Float32Field()

    engine = MergeTree(
        partition_key=('level',),
        order_by=('ts', 'userId',)
    )


database = Database('eventdata')

database.create_table(Events_buf)
database.create_table(Events)

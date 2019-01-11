
import asyncio, logging

import aiomysql
#期待通过ORM实现如下代码
#class User(Model):
#    定义类的属性到列的映射：
#    id = IntegerField('id')
#    name = StringField('username')
#    email = StringField('email')
#    password = StringField('password')
#
# 创建一个实例：
#u = User(id=12345, name='Michael', email='test@orm.org', password='my-pwd')
# 保存到数据库：
#u.save()
# 其中，父类Model和属性类型StringField、IntegerField是由ORM框架提供的，
# 剩下的魔术方法比如save()全部由metaclass自动完成。虽然metaclass的编写会比较复杂，
# 但ORM的使用者用起来却异常简单。
# 现在，我们就按上面的接口来实现该ORM

def log(sql, args=()):
    logging.info('SQL: %s' % sql)

#创建连接池
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool # 全局变量用于保存连接池
    #A coroutine that creates a pool of connections to MySQL database.
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop # 传递消息循环对象loop用于异步执行
    )

#Select函数，返回结果集
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            #执行sql语句，同时替换占位符
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
        logging.info('rows returned: %s' % len(rs))
        return rs

#Insert, Update, Delete函数，返回结果数
async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise ## raise不带参数，则把此处的错误往上抛
        return affected

# 根据输入的参数生成占位符列表
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    # 以','为分隔符，将列表合成字符串
    return ', '.join(L)

#Field类，它负责保存数据库表的字段名和字段类型
class Field(object):

    def __init__(self, name, column_type, primary_key, default):
    # 表的字段包含名字、类型、是否为表的主键和默认值
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        #结果类似<StringField,varchar(100):None>
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)

#metaclass允许你创建类或者修改类
#按照默认习惯，metaclass的类名总是以Metaclass结尾，以便清楚地表示这是一个metaclass
#在ModelMetaclass中，一共做了几件事情：
#1、排除掉对Model类的修改；
#2、在当前类（比如User）中查找定义的类的所有属性，如果找到一个Field属性，
#   就把它保存到一个__mappings__的dict中，同时从类属性中删除该Field属性，否则，容易造成运行时错误（实例的属性会遮盖类的同名属性）；
#3、把表名保存到__table__中，这里简化为表名默认为类名。
class ModelMetaclass(type):
    #__new__ 是在__init__之前被调用的特殊方法
    #__new__是用来创建对象并返回之的方法
    #cls表示元类——ModelMetaclass
    #name表示创建的类——User
    #bases表示创建的类的基类——Model
    #attrs表示创建的类的特性，包括方法和属性——详见Model.py
    def __new__(cls, name, bases, attrs):
        #如果是model直接返回
        #如果不是则对特性进行修改
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        tableName = attrs.get('__table__', None) or name #如果没有表，表名就用Model
        logging.info('found model: %s (table: %s)' % (name, tableName))
        mappings = dict()
        fields = []
        primaryKey = None
        #在当前类（比如User）中查找定义的类的所有属性
        #如name = StringField(ddl='varchar(50)')
        #k=name,v=StringField(ddl='varchar(50)')
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    #除主键外的其他属性
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
        #为什么要删除，User是元类的实例，所以User的类属性会覆盖 元类属性，因此要删掉
        #为什么要删除，已经用mappings字典获取所有的属性以及属性类型了，这样下面只需要用一个属性__mappings__就可以获得所有属性和数据列的对应关系了
            attrs.pop(k)
        # 保存除主键外的属性名为``（运算出字符串）列表形式
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        #attrs是类的特性集合
        #下面创建各种特性
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName 
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

# 定义ORM所有映射的基类：Model
# Model类的任意子类可以映射一个数据库表
# Model类可以看作是对所有数据库表操作的基本定义的映射
# 基于字典查询形式
# Model从dict继承，拥有字典的所有功能，同时实现特殊方法__getattr__和__setattr__，能够实现属性操作
# 例如对象实例user['id']即可轻松通过UserModel去数据库获取到id
# 实现数据库操作的所有方法，定义为class方法，所有继承自Model都具有数据库操作方法
# 在Model类中，就可以定义各种操作数据库的方法，比如save()，delete()，find()，update等等
# Model只是一个基类，如何将具体的子类如User的映射信息读取出来呢？答案就是通过metaclass：ModelMetaclass类
class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        # 获取某个具体的值，肯定存在的情况下使用该函数,否则会使用__getattr()__
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        # 这个方法当value为None的时候能够返回默认值
        value = getattr(self, key, None)
        if value is None:
            # self.__mapping__在metaclass中，用于保存不同实例属性在Model基类中的映射关系
            field = self.__mappings__[key]
            if field.default is not None: # 如果实例的域存在默认值，则使用默认值
                # field.default是callable的话则直接调用 
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value
    #添加类的方法，无需实例化即可调用
    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        sql = [cls.__select__]
        if where:
        # 这里不用协程，是因为不需要等待数据返回
            sql.append('where')
            sql.append(where) # 这里的where实际上是colName='xxx'这样的条件表达式
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None) # 从kw中查看是否有orderBy属性
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)   # mysql中可以使用limit关键字
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):  # 如果是int类型则增加占位符
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:  # limit可以取2个参数，表示一个范围
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs] # 返回结果，结果是list对象，里面的元素是dict类型的

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_'] # 有结果则rs这个list中第一个词典元素_num_这个key的value值

    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        # pk是dict对象
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        # arg是保存所有Model实例属性和主键的list,使用getValueOrDefault方法的好处是保存默认值
        # 将自己的fields保存进去
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    async def update(self):
        # 这里使用getValue说明只能更新那些已经存在的值，因此不能使用getValueOrDefault方法
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)

import orm
import asyncio
from models import User, Blog, Comment

'''测试ORM是否成功'''

async def test(loop):
    await orm.create_pool(user='www-data', password='www-data', db='awesome')

    u = User(name='Test', email='test@example.com', passwd='1234567890', image='about:blank')

    await u.save()

if __name__ == 'main':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test(loop))
    loop.run_forever()
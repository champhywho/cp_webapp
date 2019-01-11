import logging;logging.basicConfig(level=logging.INFO)

import asyncio,os,json,time
from datetime import datetime

from aiohttp import web
'''所有注释在代码的上一行'''
def index(request):
    #增加content_type返回html
    return web.Response(body=b'<h1>awesome</h1>',content_type='text/html')

async def init(loop):
    #将eventloop设置为服务器调用的函数
    app=web.Application(loop=loop)
    #增加协程
    app.router.add_route('GET','/',index)
    #创建服务器，handler负责连接、接受请求、处理响应、关闭连接
    srv=await loop.create_server(app.make_handler(),'127.0.0.1',9000)
    logging.info('server started at http://127.0.0.1:9000...')
    return srv

loop=asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()
import asyncio
import os
import psycopg
import traceback
import json
from aiohttp import web
import sys
from custom import connect_db
from datetime import datetime,timezone

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

active_users={}


# async def save_message(message,user_id,chat_id):
async def save_message(msg,chat_id,last_msg):
    try:
        print('inside save_message')
        conn=await connect_db()
        if conn:
            async with conn:
                async with conn.cursor() as crs:
                    await crs.execute('''
                                        update chats
                                            set message=message || %s::JSONB,last_msg=%s::JSONB
                                            where chat_id=%s
                                      ''',(json.dumps(msg),json.dumps(last_msg),chat_id))
        print('message saved')
    except Exception:
        traceback.print_exc()
async def store_users(client_id,websocket):
    try:
        if client_id not in active_users:
                active_users[client_id]=websocket
        elif active_users[client_id] != websocket:
            active_users[client_id]=websocket
        print(active_users)
    except Exception:
        traceback.print_exc()

async def extract_db_data(data):
    try:
        dt=datetime.now(timezone.utc).replace(microsecond=0)
        dt=dt.strftime('%Y-%m-%dT:%I:%M:%S%p')
        data['time']=dt
        chat_id=data['chatId']
        del data['chatId']
        message=[data]
        last_msg={
            'time':data['time'],
            'msg':data['msg']
        }
        print('about to save messages')
        await save_message(message,chat_id,last_msg)
    except psycopg.DatabaseError as e:
        traceback.print_exc()    

    # return user_id,chat_id,message
async def handler(request):
    ws=web.WebSocketResponse()
    try:
        user_id=request.query.get('user_id')
        await store_users(user_id,ws)
        await ws.prepare(request)
        async for message in ws:
            deserialized_mssg=json.loads(message.data)
            if 'recieverId' in deserialized_mssg:
                if deserialized_mssg['recieverId'] in active_users:
                    reciever_websocket=active_users[deserialized_mssg['recieverId']]
                    await reciever_websocket.send_str(json.dumps(deserialized_mssg['message']))
                    await extract_db_data(deserialized_mssg['message'])
    except Exception:
        traceback.print_exc()
    finally:
        return ws

async def health(request):
    return web.Response(text='OK')


async def main():
    try:
        port=int(os.environ.get('PORT'))
        app=web.Application()
        app.router.add_get("/",health)
        app.router.add_get("/chat",handler)
        runner=web.AppRunner(app)
        await runner.setup()
        site=web.TCPSite(runner,'0.0.0.0',port)
        await site.start()
        print(f'chat server is running on port :{port}',flush=True)

        await asyncio.Future()
    except Exception:
        traceback.print_exc()

    
asyncio.run(main())

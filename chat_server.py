import websockets
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
        conn=await connect_db()
        if conn:
            async with conn:
                async with conn.cursor() as crs:
                    await crs.execute('''
                                        update chats
                                            set message=message || %s::JSONB,last_msg=%s::JSONB
                                            where chat_id=%s
                                      ''',(json.dumps(msg),json.dumps(last_msg),chat_id))
    except Exception:
        traceback.print_exc()
async def store_users(deserialized_mssg,websocket):
    try:
        if deserialized_mssg['userId'] not in active_users:
                active_users[deserialized_mssg['userId']]=websocket
                await websocket.send(json.dumps(deserialized_mssg))
        elif active_users[deserialized_mssg['userId']] != websocket:
            active_users[deserialized_mssg['userId']]=websocket
            await websocket.send(json.dumps(deserialized_mssg))
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
        await save_message(message,chat_id,last_msg)
    except psycopg.DatabaseError as e:
        traceback.print_exc()    

    # return user_id,chat_id,message
async def handler(websocket):
    try:
        async for message in websocket:
            deserialized_mssg=json.loads(message)
            if 'userId' in deserialized_mssg:
                await store_users(deserialized_mssg,websocket)
            if 'recieverId' in deserialized_mssg:
                await extract_db_data(deserialized_mssg['message'])
                if deserialized_mssg['recieverId'] in active_users:
                    reciever_websocket=active_users[deserialized_mssg['recieverId']]
                    await reciever_websocket.send(json.dumps(deserialized_mssg['message']))
    except Exception:
        traceback.print_exc()

async def health(request):
    return web.Response(text='OK')


async def main():
    port=int(os.environ.get('PORT',1991))
    ws_server=await websockets.serve(handler,'0.0.0.0',port)
    app=web.Application()
    app.router.add_get("/",health)
    runner=web.AppRunner(app)
    await runner.setup()
    site=web.TCPSite(runner,'0.0.0.0',8080)
    await site.start()
    print(f'chat server is running on port :{port}')
    await asyncio.Future()

    
asyncio.run(main())
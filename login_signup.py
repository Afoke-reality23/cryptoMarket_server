import asyncio
import traceback
from urllib.parse import urlparse,parse_qs
import httpx
import json
from custom import connect_db
import uuid
from response import response
import psycopg
from custom import generate_username
from custom import generate_trans_id
from dotenv import load_dotenv
import os

load_dotenv()
def process_google_auth(data,sock,cursor):
    request_line=data.splitlines()[0]
    method,url,protocol=request_line.split(' ')
    parse_url=urlparse(url)
    queries=parse_qs(parse_url.query)
    client_id=os.getenv('client_id')
    client_secret=os.getenv('client_secret')
    return asyncio.run(main(queries['code'][0],client_id,client_secret,sock,cursor))


async def autho_user(code,client_id,client_secret,sock,crs):
    try:
        async with httpx.AsyncClient() as request:
            url='https://oauth2.googleapis.com/token'
            headers={
                'Content-Type':'application/x-www-form-urlencoded'
            }
            param={
                'code':code,
                'client_id':client_id,
                'client_secret':client_secret,
                'redirect_uri':'http://127.0.0.1:1998/auth/google/callback',
                'grant_type':'authorization_code'
            }
            if response.status_code == 200:
                data=response.json()
                token=data.get('access_token')
                data=await get_user_google_info(token,sock,crs)
                return data
    except asyncio.CancelledError as asyncError:
        traceback.print_exc(asyncError)
            
async def get_user_google_info(token,sock,crs):
    async with httpx.AsyncClient() as request:
        url='https://www.googleapis.com/oauth2/v2/userinfo'
        headers={
            'Authorization': f'Bearer {token}'
        }
        response=await request.get(url,headers=headers)
        data=response.json()
        del data['verified_email']
        # data['name=']=generate_username()
        # data['balance']=10000
        crs.execute('select email from users')
        response=crs.fetchall()
        emails=[]
        for x in response:
            emails.append(x[0])
        if data['email'] in emails:
            data=await login(data,sock)
        else:
            data=await signup(data,crs,sock)
        return data

async def main(code,client_id,client_secret,sock,crs):
    try:
        async with asyncio.TaskGroup() as task:
            tg=task.create_task(autho_user(code,client_id,client_secret,sock,crs))
            data=await tg
            return data
    except Exception as error:
        traceback.print_exc()



async def signup(data,crs,sock='',method=''):
    try:
        if len(data) ==1 and list(data.keys())[0] == list(data.keys())[0]:
            ## for manually changing of username
            await crs.execute('select username from users')
            usernames=await crs.fetchall()
            for x in usernames:
                if data['username'] in usernames:
                    msg={'response':'Ooops! That name already taken.Please choose another','status':'unavaliable'}
                    reply=json.dumps(msg)
                else:
                    msg={'response':'username avaliable','status':'available'}
                    reply=json.dumps(msg)
                data={'body':reply}
        else:
            column=list(data.keys())
            if column[0] == 'id':
                column[0]='google_id'
            cols=",".join(column)
            values=list(data.values())
            vals=['%s']*len(values)
            placeholders=",".join(vals)
            await crs.execute(f'insert into users({cols}) values({placeholders})',values)
            session_id=str(uuid.uuid4())
            await crs.execute('select users_id from users where email=%s',(data['email'],))
            user_id=await crs.fetchone()
            await crs.execute('select * from users')
            await crs.execute("insert into session(session_id,user_id) values(%s,%s)",(session_id,user_id[0]))
            await crs.execute('select username from users')
            signedup_username=await crs.fetchall()
            signup_username=await generate_username(crs)
            while (signup_username,) in signedup_username:
                signup_username=await generate_username(crs)
            transaction_id=generate_trans_id()
            await crs.execute('update users set username=%s,balance=%s,transaction_id=%s where users_id=%s',[signup_username,10000,transaction_id,user_id[0]])
            if method == 'POST':
                msg={'status':'successfull'}
                reply=json.dumps(msg)
                data={'body':reply,'session_id':session_id}
            else:
                msg=(
                    'HTTP/1.1 302 Found\r\n'
                    'Location: https://realcryptomarket.netlify.app/index.html\r\n'
                    # 'Access-Control-Allow-Origin:http://127.0.0.1:5500\r\n'
                    'Content-Length:0\r\n'
                    # 'Access-Control-Allow-Credentials:true\r\n'
                    f'Set-Cookie:session_id={session_id};HttpOnly;Path=/;SameSite=Strict\r\n'
                    '\r\n\r\n'
                )
                sock.write(msg.encode('utf-8'))
                await sock.drain()
        return data
        
    except psycopg.DatabaseError as e:
        print('Database error:',e)
        traceback.print_exc()  
    except Exception as error:
        print('Exception occureds:',repr(error))
        traceback.print_exc()

async def login(data,crs,sock,method):
    try:
        session_id=str(uuid.uuid4())
        await crs.execute('select users_id from users where email=%s',(data['email'],))
        user_id=await crs.fetchone()
        await crs.execute("insert into session(session_id,user_id) values(%s,%s)",(session_id,user_id[0]))
        if method == 'POST':
            msg={'status':'successfull'}
            reply=json.dumps(msg)
            data={'body':reply,'session_id':session_id}
            return data
        else:
            msg=(
                'HTTP/1.1 302 Found\r\n'
                'Location:https://realcryptomarket.netlify.app/index.html\r\n'
                'Acces-Control-Allow-Credential:true'
                'Content-Length:0\r\n'
                f'Set-Cookie:session_id={session_id};HttpOnly;Path=/;SameSite=Strict\r\n'
                '\r\n\r\n'
            )
            sock.send(msg.encode('utf-8'))
    except psycopg.DatabaseError as error:
        print(error)
    # return 
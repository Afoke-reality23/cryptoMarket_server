import socket
import json
from urllib.parse import urlsplit,parse_qs,urlparse
from datetime import datetime
from decimal import Decimal
import traceback
from custom import connect_db
from response import response
import asyncio
import psycopg
from login_signup import process_google_auth,signup,login
from custom import database_column_value_extractor
import threading
import uuid
import os
import sys

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def main():
    try:
        port = int(os.environ.get('PORT',1998))
        server=await asyncio.start_server(handle_connections,'',port)
        print(f'server is running on port:{port}')
        async with server:
            await server.serve_forever()
    except Exception as err:
        traceback.print_exc()



async def handle_connections(reader,writer):
    try:
        data=await recieve_full_data(reader,writer)
        if data is not None:
            method=data.split('\r\n')[0]
            parse_url=urlparse(method)
            query=parse_url.path
            path_method=query.split(' ')
            path=path_method[1]
            method=path_method[0]
            cookies={}
            headers=data.split('\r\n')
            for header in headers:
                if header.startswith('Cookie:'):
                    cookie_header=header.replace('Cookie:','')
                    cookie_list=cookie_header.split(';')
                    cookie_tuple=list(tuple(tup.split('=')) for tup in cookie_list)
                    cleaned_cookie=[(k.strip(),v.strip()) for k,v in cookie_tuple]
                    cookies.update(cleaned_cookie)
            isLoggedIn='loggedIn' if cookies.get('session_id') else 'not_logged_in'
            valid_session_id=cookies.get('session_id')
            OAUTH_REQUIRED_PATH=(
                '/frontend/oauth/login/password'
                '/frontend/oauth/create-account/password'
                '/oauth/status'
                '/logout'
                '/buy'
                '/profile'
                '/transaction'
                '/market-listing'
                '/buy-listed'
                '/chat'
            )
            conn=await connect_db()
            async with conn:
                async with conn.cursor() as crs:
                    if path == '/auth/google/callback':
                        print('process google called')
                        server_response=process_google_auth(data,writer,crs)
                    elif path in OAUTH_REQUIRED_PATH:
                        server_response=await process_request(path,data,writer,method,isLoggedIn,valid_session_id,crs)
                    else:
                        server_response=await process_request(path,data,writer,method,isLoggedIn,valid_session_id,crs)
                    body=server_response.get('body')
                    max_age=server_response.get('max_age')
                    session_id=server_response.get('session_id')
                    if session_id:
                        await response(writer,method,body,session_id,max_age)
                    else:
                        await response(writer,method,body)
    except (Exception,KeyboardInterrupt) as error:
        traceback.print_exc()


async def recieve_full_data(reader,writer):
    try:
        headers=await reader.readuntil(b'\r\n\r\n')
        method=urlsplit(headers).path.split(b' ')[0]
        line_headers=headers.split(b'\r\n')
        if method == b'OPTIONS':
            await response(writer,method)
            return
        content_length=0
        for line in line_headers:
            if line.lower().startswith(b'content-length:'):
                content_length=int(line.split(b':',1)[1].strip())
                break
        body=b''
        if content_length > 0:
            body=await reader.readexactly(content_length)
        request=headers + body
        full_request=request.decode('utf-8')
        return full_request
    except Exception:
        traceback.print_exc()
async def process_request(path,request,sock,method,status,cookie,crs):#process all http request
    try:
        headers,body=request.split('\r\n\r\n',1)
        if headers.startswith('GET'):
            if not headers:
                return
            params=urlsplit(headers)
            if params.query:
                data=parse_qs(params.query)
                get_data={}
                for k,v in data.copy().items():
                    get_data[k]=v[0].split(' ',1)[0]
                print(get_data)
            match path:
                case '/transaction':
                     assets=await get_users_transation(cookie,crs)
                case '/assets':
                    assets=await get_assets(crs)
                case '/search':
                    assets=await get_searched_assets(get_data,crs)
                case '/total':
                    assets=await get_total_values(crs)
                case '/profile':
                    assets=await get_user_profile(cookie,crs)
                case '/asset_details':
                    assets=await get_asset_details(get_data,crs)
                case '/chart':
                    assets=get_asset_chart(get_data,crs)
                case '/oauth/status':
                    assets=oauth_user(status,crs)
                case '/logout':
                    assets=await logout(cookie,crs)
                case '/market-listing':
                    assets=await get_listed_asset(crs)
                case '/chat':
                    assets=await fetch_chat(cookie,crs,get_data['chat_id'])
            return assets
        else:
            data=json.loads(body)
            match path:
                case '/frontend/oauth/create-account/password':
                    await crs.execute('select email from users')
                    response=await crs.fetchall()
                    emails=[x[0] for x in response]
                    if data['email'] in emails:
                        msg={'response':'Invalid Credential','status':'Registered'}
                        reply=json.dumps(msg)
                        status={'body':reply}
                    else:
                        status=await signup(data,crs,sock,method)
                case'/frontend/oauth/login/password':
                    await crs.execute('select email from users')
                    response=await crs.fetchall()
                    emails=[]
                    for x in response:
                        emails.append(x[0])
                    if data['email'] in emails:
                        await crs.execute("select password from users where email=%s",(data['email'],))
                        password=await crs.fetchone()
                        if password[0] == data['password']:
                            status=await login(data,crs,sock,method)
                        else:
                            msg={'response':'invalid Credential','status':'Invalid Credential'}
                            reply=json.dumps(msg)
                            status={'body':reply}
                            # return data
                    else:
                        msg={'response':'Invalid Credential','status':'Not Found'}
                        reply=json.dumps(msg)
                        status={'body':reply}
                        # return data
                case '/buy':
                    trans_type=path.replace('/','')
                    data['trans_type']=trans_type
                    status=await transaction(data,cookie,crs)
                    # return status
                case '/sell':
                    print('lame')
                    trans_type=path.replace('/','')
                    data['trans_type']=trans_type
                    status=await transaction(data,cookie,crs)
                    # return status
                case '/market-listing':
                    if data.get('status')=='Negotiating':
                        await crs.execute('select user_id from session where session_id=%s',(cookie,))
                        buyer_id=await crs.fetchone()
                        chat_id=str(uuid.uuid4())
                        await crs.execute('update market_list set state=%s,chat_id=%s,buyer_id=%s where seller_id=%s and asset_id=%s',(data['status'],chat_id,buyer_id[0],data['sellerId'],data['asset_id']))
                        await crs.execute('insert into chats(chat_id,seller_id,buyer_id) values(%s,%s,%s)',(chat_id,data['sellerId'],buyer_id[0]))
                        reply={'chat_id':chat_id}
                        data=json.dumps(reply)
                        status={'body':data}
                    else:
                        status=await save_listed_asset(data,cookie,crs)
                    # return asset
                case '/buy-listed':
                    if data.get('status') == 'Negotiating':
                        await crs.execute('update market_list set status=%s where seller_id=%s and asset_id=%s',(data['status'],data['sellerId'],data['asset_id']))
                        status={'body':{'status':'successful'}}
                    else:
                        await crs.execute('select seller_id,asset_id,quantity,set_price,processing_speed from market_list where seller_id=%s and asset_id=%s',[data['sellerId'],data['asset_id']])
                        data=await crs.fetchone()
                        await crs.execute('select user_id from session where session_id=%s',(cookie,))
                        buyer_id=await crs.fetchone()
                        buyer_id=float(buyer_id[0])
                        if buyer_id==int(data[0]):
                            raise ValueError('cant buy own asset')
                        await crs.execute('select transaction_id from users u join session s on s.user_id=u.users_id where session_id=%s',(cookie,))
                        wallet=await crs.fetchone()
                        data={'user_id':int(data[0]),'asset_id':data[1],'trans_quantity':float(data[2]),'trans_price':float(data[3]),'processing_speed':float(data[4]),'trans_type':'sell','reciever_wallet':wallet[0]}
                        status=await transaction(data,cookie,crs)
                        print('status >>>>>>>>.',status)
                        response=json.loads(status['body'])
                        if 'status'in response:
                            print('delete listed asset')
                            await crs.execute('delete from market_list where seller_id=%s and asset_id=%s',(float(data['user_id']),float(data['asset_id'])))

            return status
    except psycopg.DatabaseError as error:
        print(error)
    except Exception:
        traceback.print_exc()


async def fetch_chat(cookie,crs,chat_id):
    try:
        await crs.execute('select user_id from session where session_id=%s',(cookie,))
        user_id=await crs.fetchone()
        await crs.execute('select seller_id,buyer_id from chats where chat_id=%s',(chat_id,))
        ids=await crs.fetchone()
        if user_id[0] ==ids[0]:
            await crs.execute('select seller_msg,buyer_msg from chats where seller_id=%s and chat_id=%s',(user_id[0],chat_id))
        else:
            await crs.execute('select seller_msg,buyer_msg from chats where buyer_id=%s and chat_id=%s',(user_id[0],chat_id))
        messages=await crs.fetchone()
        reply=json.dumps(messages)
        data={'body':reply}
        return data
    except Exception:
        traceback.print_exc()


async def get_listed_asset(crs):
    try:
        await crs.execute("""
                    select
                        name,
                        symbol,
                        a.id,
                        image,
                        set_price,
                        m.quantity,
                        seller_id
                    from market_list m
                    join assets a on a.id=m.asset_id
                    where state=%s
                    """,('listed',))
        db_response=await crs.fetchall()
        listed_asset=[[float(y) if isinstance(y,Decimal) else y for y in x] for x in db_response]
        reply=json.dumps(listed_asset)
        data={'body':reply}
        return data
    except Exception:
        traceback.print_exc()


async def save_listed_asset(data,cookie,crs):
    try:
        await crs.execute('select user_id from session where session_id=%s',(cookie,))
        user_id=await crs.fetchone()
        data['seller_id']=user_id[0]
        db_data=database_column_value_extractor(data)
        await crs.execute(f'insert into market_list({db_data[0]}) values({db_data[1]})',db_data[2])
        confirmation={'status':'success'}
        reply=json.dumps(confirmation)
        data={'body':reply}
        return data
    except Exception:
        traceback.print_exc()

async def get_searched_assets(data,crs):
    try:
        searched_assets=data['searched_asset'].split(',')
        placeholder=",".join(['%s']* len(searched_assets))
        await crs.execute(f"select id,name,symbol,price,market_cap,percent_change_24h from assets where id in ({placeholder}) order by no",searched_assets)
        assets=await crs.fetchall()
        all_assets=[]
        for asset in assets:
            asset={
                'asset_id':asset[0],
                'asset_name':asset[1],
                'symbol':asset[2],
                'asset_price':float(asset[3]),
                'market_cap':float(asset[4]),
                'percent_change_24h':float(asset[5])
            }
            all_assets.append(asset)  
        db_assets=json.dumps(all_assets)
        reply={'body':db_assets}
        return reply
    except Exception:
        traceback.print_exc()
    except psycopg.DatabaseError as error:
        print('DatabaseErro:',error)


async def get_user_profile(cookie,crs):
        try:
            await crs.execute('select user_id from session where session_id=%s',(cookie,))
            user_id=await crs.fetchone()
            await crs.execute("select username,balance,transaction_id from users where users_id=%s",(user_id[0],))
            user_profile=await crs.fetchall()
            client_data={
                'username':user_profile[0][0],
                'balance':float(user_profile[0][1]),
                'transId':user_profile[0][2]
            }
            tot_query="select sum(total_value) as net_value from portfolio where user_id=%s"
            await crs.execute(tot_query,(user_id[0],))
            tot_value=await crs.fetchone()
            total_value=float(tot_value[0]) if tot_value[0] is not None else 0
            await crs.execute("""
                        select 
                        name,
                        symbol,
                        price,
                        image,
                        quantity,
                        avg_price,
                        total_value,
                        a.id
                        from portfolio p
                        join assets a on a.id=p.asset_id
                        where p.user_id=%s
                        """,(user_id[0],))
            db_response=await crs.fetchall()
            assets=[]
            for x in db_response:
                asset=[]
                for y in x:
                    if isinstance(y,Decimal):
                        asset.append(float(y))
                    else:
                        asset.append(y)
                assets.append(asset)
            msg={'user_balance':client_data,'total_value':total_value,'asset':assets}
            reply=json.dumps(msg)
            data={'body':reply}
            return data
        except Exception:
            traceback.print_exc()
        except psycopg.DatabaseError as error:
            print('DatabaseErro:',error)


def oauth_user(status,crs):
    msg={'isloggedIn':status}
    reply=json.dumps(msg)
    data={'body':reply}
    return data

async def logout(cookies,crs):
    await crs.execute("select * from session where session_id=%s",(cookies,))
    cookie_response=await crs.fetchone()
    if cookie_response:
        await crs.execute("delete from session where session_id =%s",(cookies,))
        max_age=0
        reply={
            'session_id':cookies,
            'max_age':max_age
        }
        return reply

async def get_asset_chart(data,crs):
    await crs.execute('select open,high,low,close,time from chart where asset_id=%s',(data['id'],))
    rows=await crs.fetchall()
    chart_value=[[float(v) if isinstance(v,Decimal) else v for v in row] for row in rows]
    charts=json.dumps(chart_value)
    data={'body':charts}
    return data



async def get_asset_details(data,crs):
    try:
        query=f"""
                select
                    name,
                    symbol,
                    price,
                    market_cap,
                    description,
                    summary,
                    snippet,
                    founder,
                    total_volume,
                    max_supply,
                    total_supply,
                    circulating_supply
                from assets a
                join assets_market m on m.id=a.id
                join assets_detail d on d.id=a.id
                where a.id=%s
                """
        await crs.execute(query,(data['asset_id'],))
        values=await crs.fetchone()
        cols=['asset_name','symbol','asset_price','market_cap','description','summary','snippet','founder','total_volume','supply_max','supply_total','supply_circulating']
        detail_dict={}
        if values:
            for i,value in enumerate(values):
                if isinstance(value,Decimal):
                    detail_dict.update([(cols[i],float(value))])
                else:
                    detail_dict.update([(cols[i],value)])
        else:
            detail_dict={'response':'asset have no details yet'}
        reply=json.dumps(detail_dict)
        data={'body':reply}
        return data
    except Exception:
        traceback.print_exc()
    except psycopg.DatabaseError as error:
        print('DatabaseErro:',error)
#GET queies
#100% done with assets endpoint
async def get_assets(crs):
    try:
        await crs.execute("select id,name,symbol,price,market_cap,percent_change_24h from assets order by no limit 200")
        assets=await crs.fetchall()
        all_assets=[]
        for asset in assets:
            asset={
                'asset_id':asset[0],
                'asset_name':asset[1],
                'symbol':asset[2],
                'asset_price':float(asset[3]),
                'market_cap':float(asset[4]),
                'percent_change_24h':float(asset[5])
            }
            all_assets.append(asset)  
        db_assets=json.dumps(all_assets)
        reply={'body':db_assets}
        return reply
    except Exception:
        traceback.print_exc()
    except psycopg.DatabaseError as error:
        print('DatabaseErro:',error)

async def get_total_values(crs):
    await crs.execute('select sum(market_cap) as total_market_cap,sum(percent_change_24h) as total_percent from assets')
    totals=await crs.fetchone()
    totals=[float(x) for x in totals ]
    reply=json.dumps(totals)
    data={'body':reply}
    return data



async def get_users_transation(cookie,crs):#Done with this for now REFACTOR later
    # will refactore this later to handle not just only user transaction but all transaction done in the past in the day
        try:
            query='select user_id from session where session_id=%s'
            await crs.execute(query,(cookie,))
            user_id=await crs.fetchone()
            await crs.execute("""
                        select
                            name,
                            symbol,
                            trans_quantity,
                            trans_price,
                            trans_type,
                            image,
                            trans_time
                        from transaction t
                        join assets a on a.id=t.asset_id
                        where t.user_id=%s
                        """,(user_id[0],))
            response=await crs.fetchall()
            transaction=[]
            if response:
                for x in response:
                    transact=[]
                    for y in x:
                        if isinstance(y,Decimal):
                            transact.append(float(y))
                        elif isinstance(y,datetime):
                            transact.append(y.strftime('%Y-%m-%d %H:%M:%S'))
                        else:
                            transact.append(y)
                    transaction.append(transact)
            reply=json.dumps(transaction)
            data={'body':reply}
            return data
        except (Exception,SyntaxError,ValueError,IndexError):
            traceback.print_exc()
        except psycopg.DatabaseError as error:
            print('DatabaseErro:',error)

async def buy_asset(buy_data,crs,balance):
    try:
        db_data=database_column_value_extractor(buy_data)
        insert=f"INSERT INTO transaction({db_data[0]}) VALUES({db_data[1]})"
        await crs.execute(insert,db_data[2])
        portfolio=f"""
                INSERT INTO portfolio(user_id,asset_id,avg_price,quantity,total_value)
                    select 
                        user_id,
                        asset_id,
                        (sum(trans_price * trans_quantity)/sum(trans_quantity)),
                        sum(trans_quantity),
                        sum(trans_price)
                    from transaction t
                    where user_id=%s and asset_id=%s
                    group by t.user_id,t.asset_id
                    ON CONFLICT(user_id,asset_id)
                    DO UPDATE SET
                        quantity=EXCLUDED.quantity,
                        avg_price=EXCLUDED.avg_price,
                        total_value=EXCLUDED.total_value
                """
        await crs.execute(portfolio,(buy_data['user_id'],buy_data['asset_id']))
        await crs.execute('update users set balance =%s where users_id=%s',(balance,buy_data['user_id']))
    except Exception:
        traceback.print_exc()
    

async def sell_asset(sell_data,crs,balance,quantity,cookie):
    sell_data['trans_quantity']=sell_data['trans_quantity']-(sell_data['processing_speed'] * 0.0001)
    sell_data['trans_price']=sell_data['trans_price'] - sell_data['processing_speed']
    if sell_data['processing_speed'] == 7:
        processing_speed='standard'
    elif sell_data['processing_speed'] == 5:
        processing_speed='slow'
    else:
        processing_speed='fast'
    profit_data={'amount':sell_data['processing_speed'],'processing_speed':processing_speed}
    del sell_data['processing_speed']
    seller_id=sell_data['user_id']
    prof_data=database_column_value_extractor(profit_data)
    await crs.execute(f'insert into profit({prof_data[0]}) values({prof_data[1]})',prof_data[2])
    if sell_data['reciever_wallet']:
        await crs.execute('select transaction_id from users')
        all_wallet_ids=await crs.fetchall()
        wallet_ids=[wallet_id[0] for wallet_id in all_wallet_ids]
        if sell_data['reciever_wallet'] not in wallet_ids:
            raise ValueError('invalid wallet address')
        buyer_data={'asset_id':sell_data['asset_id'],'trans_quantity':sell_data['trans_quantity'],'trans_price':sell_data['trans_price'],'trans_type':'buy'}
        status=await transaction(buyer_data,cookie,crs)
        response=json.loads(status['body'])
        if 'error' in  response:
            raise ValueError(f'Buyer transaction failed {response['error']}')
    sell_data=dict(buyer_data)
    sell_data['user_id']=seller_id
    sell_data['trans_type']='sell'
    db_data=database_column_value_extractor(sell_data)
    insert=f"INSERT INTO transaction({db_data[0]}) VALUES({db_data[1]})"
    await crs.execute(insert,db_data[2])
    quantity_balance=float(quantity[0])- sell_data['trans_quantity']
    await crs.execute(" update portfolio set quantity=%s where user_id=%s and asset_id=%s",[quantity_balance,sell_data['user_id'],sell_data['asset_id']])
    await crs.execute('update users set balance =%s where users_id=%s',(balance,sell_data['user_id']))


async def transaction(client_data,cookie,crs): # transaction function update the transaction and portfolio table
    try:
        await crs.execute('BEGIN')
        if not client_data.get('user_id'):
            query='select user_id from session where session_id=%s'
            await crs.execute(query,(cookie,))
            user_id=await crs.fetchone()
            client_data['user_id']=user_id[0]
        else:
            user_id=client_data.get('user_id')
        validate_trans_client_data(client_data,crs)
        await validate_trans_db_data(crs,client_data)
        await crs.execute("select balance from users where users_id=%s",(client_data['user_id'],))
        user_db_balance=await crs.fetchone()
        await crs.execute("select quantity from portfolio where user_id=%s and asset_id=%s",(client_data['user_id'],client_data['asset_id']))
        user_quantity_balance=await crs.fetchone()
        user_quantity_balance_check=user_quantity_balance[0] if user_quantity_balance else 0 
        user_balance_check=float(user_db_balance[0]) if user_db_balance else 0
        portfolio_balance=(
            user_balance_check + client_data['trans_price']
            if client_data['trans_type']=='sell'
            else max(user_balance_check - client_data['trans_price'],0)
            )
        if client_data['trans_type']=='buy':
            print('ayo')
            if client_data['trans_price'] > user_balance_check:
                raise ValueError('insufficient balance')
            await buy_asset(client_data,crs,portfolio_balance)
        else:
            if client_data['trans_quantity'] > float(user_quantity_balance_check):
                raise ValueError('insuffiencit asset')
            await sell_asset(client_data,crs,portfolio_balance,user_quantity_balance,cookie)
        delete_port='delete from portfolio where quantity=%s'
        await crs.execute(delete_port,(0,))
        await crs.execute('COMMIT')
        msg={'status':'successfull'}
        reply=json.dumps(msg)
        data={'body':reply}
        return data
    except ValueError as error:
        valError={"error":str(error)}
        reply=json.dumps(valError)
        data={'body':reply}
        return data
    except psycopg.DatabaseError as error:
        print("DatabaseError:",error)
        await crs.execute('ROLLBACK')
    except Exception:
        traceback.print_exc()


def validate_trans_client_data(val_client_data,crs):# validate input sent sent by client before inserting into transaction table
    try:
        required_data={"user_id","asset_id","trans_type","trans_quantity","trans_price"}
        missing_data_key=required_data - set(val_client_data.keys())
        missing_data_values={}
        for k,v in val_client_data.copy().items():
            if k==''.strip():
                missing_data_values.update({k:v})
                raise ValueError(f"missing values:f{missing_data_values}")
        if missing_data_key:
            raise ValueError(f"missing data:{','.join(missing_data_key)}")
        if int(val_client_data["trans_price"]) < 0: #not sure this is error is ever gonna be raised cause i already handle this in the frontend
            raise ValueError(f"Invalid Numbers:Negative numbers not allowed") 
        return ValueError
    except Exception:
        traceback.print_exc()



async def validate_trans_db_data(crs,val_db_data): # validate data from the transaction table in the db to decide weather to insert new data into the transaction table or not
    try:
        check_user_asset_existence="""
        select users_id,symbol 
        from users u,assets a
        where u.users_id=%s and a.id=%s
        """
        await crs.execute(check_user_asset_existence,(val_db_data['user_id'],val_db_data['asset_id']))
        check=await crs.fetchall()
        if not check:
            raise ValueError('user or selected assets does not exist')
        
        fetch_duplicate="""
        select user_id,asset_id,trans_type,trans_quantity,trans_price,trans_time
        from transaction 
        where user_id=%s and CAST(trans_time AS timestamp) > NOW()- INTERVAL '5 seconds'
        """
        await crs.execute(fetch_duplicate,(val_db_data['user_id'],))
        result=await crs.fetchall()
        if result:
            duplicates=result
        else:
            return
        for v in duplicates:
            duplist=list(v)
            last_time=duplist[5]
            del duplist[5]
            dupdata=duplist
            now=datetime.now()
            time_diff=(now-last_time).total_seconds()
        for k,v in val_db_data.items():
            if v in dupdata and time_diff < 5:
                raise ValueError('duplicate transaction wait 5s before trying again')
            else:
                return
    except Exception:
        traceback.print_exc()
            
    


        
# def update_assets():
# conn=connect_db()
#     assets=fetch_assets()
#     query=f"""
#     updat assets
#     set price=%s,percent_change_24h=%s
#     where symbol=%s
#     """
#     for asset in assets:
#         symbol=asset['symbol']
#         price=asset['price']
#         percent=asset['percent_change_24h']
#     crs.execute(query,(price,percent,symbol))

# threading.Timer(300,update_assets).start()

if __name__=='__main__':
    asyncio.run(main())
import socket
import json
from urllib.parse import urlparse,parse_qs
from datetime import datetime
from decimal import Decimal
import traceback
from custom import connect_db
from response import response
import asyncio
import psycopg2
from login_signup import process_google_auth,signup,login
from custom import database_column_value_extractor
import os


server = socket.socket()
port = int(os.environ.get('PORT',5000))
# port=1998
server_IP = ''  
server.bind((server_IP, port))
server.listen()

def handle_connections():
    try:
        conn=connect_db()
        if conn:
            crs=conn.cursor()
        while True:
            print('Server is listening for connections!!!')
            conn, addr = server.accept()
            # data=conn.recv(1024).decode()
            data=recieve_full_data(conn)
            if data is None or data.strip()== '':
                continue
            method=data.split('\r\n')[0]
            parse_url=urlparse(method)
            query=parse_url.path
            path_method=query.split(' ')
            path=path_method[1]
            method=path_method[0]
            if data.startswith('OPTIONS') or  data.startswith('HEAD'):
                response(conn,method)
                continue
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
                '/frontend/oauth/login/password/'
                '/frontend/oauth/create-account/password/'
                '/oauth/status'
                '/logout'
                '/buy'
                '/profile'
                '/transaction'
            )
            if path == '/auth/google/callback':
                print('process google called')
                server_response=process_google_auth(data,conn,crs)
            elif path in OAUTH_REQUIRED_PATH:    
                server_response=process_request(path,data,conn,method,isLoggedIn,valid_session_id,crs)
            else:
                server_response=process_request(path,data,conn,method,isLoggedIn,valid_session_id,crs)

            body=server_response.get('body')
            max_age=server_response.get('max_age')
            session_id=server_response.get('session_id')
            if session_id:
                response(conn,method,body,session_id,max_age)
            else:
                response(conn,method,body)
    except (Exception,KeyboardInterrupt) as error:
        traceback.print_exc()
    finally:
        conn.close()
def recieve_full_data(conn):
    try:
        request=b''
        while b'\r\n\r\n' not in request:
            chunk=conn.recv(1024)
            if not chunk:
                return None
            request+=chunk
        header_bytes,remaining_chunk=request.split(b'\r\n\r\n',1)
        headers=header_bytes.decode('utf-8')
        content_length=0
        for line in headers.split('\r\n'):
            if line.lower().startswith('content-length'):
                content_length=int(line.split(':',1)[1].strip())
                break
        body=remaining_chunk
        while len(body) < content_length:
            chunk=conn.recv(1024)
            if not chunk:
                print('empty chunk')
                break
            body+=chunk
        full_request=headers + '\r\n\r\n' + body.decode('utf-8')
        # print(full_request)
        return full_request
    except Exception:
        traceback.print_exc()
def process_request(path,request,sock,method,status,cookie,crs):#process all http request
    try:
        headers,body=request.split('\r\n\r\n',1)
        if headers.startswith('GET'):
            if not headers:
                return
            header=headers.splitlines()[0]
            url=header.split(' ')[1]
            parse_url=urlparse(url)
            table_name=parse_url.path.replace('/'," ").strip()
            query_param=parse_qs(parse_url.query)
            data={'table_name':table_name,'columns':{k:v[0] for k,v in query_param.items()}}
            match path:
                case '/transaction':
                     assets=get_users_transation(cookie,crs)
                case '/assets':
                    assets=get_assets(crs)
                case '/search':
                    assets=get_searched_assets(data['columns'],crs)
                case '/total':
                    assets=get_total_values(crs)
                case '/profile':
                    assets=get_user_profile(cookie,crs)
            
                case '/asset_details':
                    assets=get_asset_details(data['columns'],crs)
                case '/chart':
                    assets=get_asset_chart(data['columns'],crs)
                case '/oauth/status':
                    assets=oauth_user(status,crs)
                case '/logout':
                    assets=logout(cookie,crs)
            return assets
        else:
            data=json.loads(body)
            print('here is data json',data)
            match path:
                case '/frontend/oauth/create-account/password/':
                    print('very bginining')
                    crs.execute('select email from users')
                    response=crs.fetchall()
                    print(response)
                    emails=[x for x in response]
                    # for x in response:
                        # emails.append(x[0])
                    if data['email'] in emails:
                        msg={'response':'Invalid Credential','status':'Registered'}
                        reply=json.dumps(msg)
                        data={'body':reply}
                        return data
                    else:
                        # data['username']=generate_username()
                        # data['balance']=10000
                        print('about to call sign up')
                        assets=asyncio.run(signup(data,crs,sock,method))
                        print('print signup asset',assets)
                    return assets
                case'/frontend/oauth/login/password/':
                    crs.execute('select email from users')
                    response=crs.fetchall()
                    emails=[]
                    for x in response:
                        emails.append(x[0])
                    if data['email'] in emails:
                        crs.execute("select password from users where email=%s",(data['email'],))
                        password=crs.fetchone()[0]
                        if password == data['password']:
                            assets=login(data,sock,method,crs)
                            return assets
                        else:
                            msg={'response':'invalid Credential','status':'Invalid Credential'}
                            reply=json.dumps(msg)
                            data={'body':reply}
                            return data
                    else:
                        msg={'response':'Invalid Credential','status':'Not Found'}
                        reply=json.dumps(msg)
                        data={'body':reply}
                        return data
                case '/buy':
                    trans_type=path.replace('/','')
                    data['trans_type']=trans_type
                    assets=transaction(data,cookie,login)
                    return assets
                case '/sell':
                    print('lame')
                    trans_type=path.replace('/','')
                    data['trans_type']=trans_type
                    assets=transaction(data,cookie,login)
                    return assets
            
    except Exception as error:
        traceback.print_exc()

def get_searched_assets(data,crs):
    try:
        searched_assets=data['searched_asset'].split(',')
        placeholder=",".join(['%s']* len(searched_assets))
        crs.execute(f"select id,name,symbol,price,market_cap,percent_change_24h from assets where id in ({placeholder}) order by no",searched_assets)
        assets=crs.fetchall()
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
    except psycopg2.DatabaseError as error:
        print('DatabaseErro:',error)


def get_user_profile(cookie,crs):
        try:
            crs.execute('select user_id from session where session_id=%s',(cookie,))
            user_id=crs.fetchone()[0]
            crs.execute("select username,balance,transaction_id from users where users_id=%s",(user_id,))
            user_profile=crs.fetchall()[0]
            client_data={
                'username':user_profile[0],
                'balance':float(user_profile[1]),
                'transId':user_profile[2]
            }
            tot_query="select sum(total_value) as net_value from portfolio where user_id=%s"
            crs.execute(tot_query,(user_id,))
            tot_value=crs.fetchone()[0]
            total_value=float(tot_value) if tot_value else 0
            crs.execute("""
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
                        """,(user_id,))
            db_response=crs.fetchall()
            crs.execute('select * from portfolio')
            alls=crs.fetchall()
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
        except psycopg2.DatabaseError as error:
            print('DatabaseErro:',error)


def oauth_user(status,crs):
    msg={'isloggedIn':status}
    reply=json.dumps(msg)
    data={'body':reply}
    return data

def logout(cookies,crs):
    crs.execute("select * from session where session_id=%s",(cookies,))
    cookie_response=crs.fetchone()[0]
    if cookie_response:
        crs.execute("delete from session where session_id =%s",(cookies,))
        max_age=0
        reply={
            'session_id':cookies,
            'max_age':max_age
        }
        return reply

def get_asset_chart(data,crs):
    crs.execute('select open,high,low,close,time from chart where asset_id=%s',(data['id'],))
    rows=crs.fetchall()
    chart_value=[[float(v) if isinstance(v,Decimal) else v for v in row] for row in rows]
    charts=json.dumps(chart_value)
    data={'body':charts}
    return data



def get_asset_details(data,crs):
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
        crs.execute(query,(data['asset_id'],))
        values=crs.fetchone()
        cols=['asset_name','symbol','asset_price','market_cap','description','summary','snippet','founder','total_volume','supply_max','supply_total','supply_circulating']
        detail_dict={}
        for i,value in enumerate(values):
            if isinstance(value,Decimal):
                detail_dict.update([(cols[i],float(value))])
            else:
                detail_dict.update([(cols[i],value)])
        reply=json.dumps(detail_dict)
        data={'body':reply}
        return data
    except Exception:
        traceback.print_exc()
    except psycopg2.DatabaseError as error:
        print('DatabaseErro:',error)
#GET queies
#100% done with assets endpoint
def get_assets(crs):
    try:
        crs.execute("select id,name,symbol,price,market_cap,percent_change_24h from assets order by no limit 200")
        assets=crs.fetchall()
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
    except psycopg2.DatabaseError as error:
        print('DatabaseErro:',error)

def get_total_values(crs):
    crs.execute('select sum(market_cap) as total_market_cap,sum(percent_change_24h) as total_percent from assets')
    totals=crs.fetchone()
    totals=[float(x) for x in totals ]
    reply=json.dumps(totals)
    data={'body':reply}
    return data



def get_users_transation(cookie,crs):#Done with this for now REFACTOR later
    # will refactore this later to handle not just only user transaction but all transaction done in the past in the day
        try:
            query='select user_id from session where session_id=%s'
            crs.execute(query,(cookie,))
            user_id=crs.fetchone()[0]
            crs.execute("""
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
                        """,(user_id,))
            response=crs.fetchall()
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
        except psycopg2.DatabaseError as error:
            print('DatabaseErro:',error)

def buy_asset(buy_data,crs,balance):
    db_data=database_column_value_extractor(buy_data)
    columns=db_data['columns']
    placeholders=db_data['placeholder']
    insert=f"INSERT INTO transaction({columns}) VALUES({placeholders})"
    crs.execute(insert,db_data['values'])

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
    crs.execute(portfolio,(buy_data['user_id'],buy_data['asset_id']))
    crs.execute('update users set balance =%s where users_id=%s',(balance,buy_data['user_id']))
    

def sell_asset(sell_data,crs,balance,quantity):
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
    prof_data=database_column_value_extractor(profit_data)
    prof_columns=prof_data['columns']
    prof_placeholder=prof_data['placeholder']
    prof_values=prof_data['values']
    crs.execute(f'insert into profit({prof_columns}) values({prof_placeholder})',prof_values)
    crs.execute('select transaction_id from users')
    all_wallet_id=[wallet_id[0] for wallet_id in crs.fetchall()]
    if sell_data['reciever_wallet'] not in all_wallet_id:
        raise ValueError('invalid wallet address')
    crs.execute('select users_id,balance from users where transaction_id=%s',(sell_data['reciever_wallet'],))
    reciever_db_data=[value for value in crs.fetchone()]
    reciever_id,reciever_balance=reciever_db_data
    del sell_data['reciever_wallet']
    reciever_data=dict(sell_data)
    reciever_data['user_id']=float(reciever_id)
    reciever_data['trans_type']='buy'
    buy_asset(reciever_data,crs,float(reciever_balance))
    db_data=database_column_value_extractor(sell_data)
    columns=db_data['columns']
    placeholder=db_data['placeholder']
    values=db_data['values']
    insert=f"INSERT INTO transaction({columns}) VALUES({placeholder})"
    crs.execute(insert,values)
    quantity_balance=float(quantity[0])- sell_data['trans_quantity']
    crs.execute(" update portfolio set quantity=%s where user_id=%s and asset_id=%s",[quantity_balance,sell_data['user_id'],sell_data['asset_id']])
    crs.execute(f'update users set balance =%s where users_id=%s',(balance,sell_data['user_id']))


def transaction(client_data,cookie,crs): # transaction function update the transaction and portfolio table
    try:
        crs.execute('BEGIN')
        query='select user_id from session where session_id=%s'
        crs.execute(query,(cookie,))
        user_id=crs.fetchone()[0]
        client_data['user_id']=user_id
        validate_trans_client_data(client_data)
        validate_trans_db_data(crs,client_data)
        crs.execute("select balance from users where users_id=%s",(user_id,))
        user_db_balance=crs.fetchone()
        crs.execute("select quantity from portfolio where user_id=%s and asset_id=%s",[user_id,client_data['asset_id']])
        user_quantity_balance=crs.fetchone()
        user_quantity_balance_check=user_quantity_balance[0] if user_quantity_balance else 0 
        user_balance_check=user_db_balance[0] if user_db_balance else 0
        portfolio_balance=(
            user_balance_check + client_data['trans_price']
            if client_data['trans_type']=='sell'
            else max(user_balance_check - client_data['trans_price'],0)
            )
        if client_data['trans_type']=='buy':
            if client_data['trans_price'] > user_balance_check:
                raise ValueError('insufficient balance')
            buy_asset(client_data,crs,portfolio_balance)
        else:
            if client_data['trans_quantity'] > float(user_quantity_balance_check):
                raise ValueError('insuffiencit asset')
            sell_asset(client_data,crs,portfolio_balance,user_quantity_balance)
        delete_port='delete from portfolio where quantity=%s'
        crs.execute(delete_port,(0,))
        crs.execute('COMMIT')
        msg={'response':'asset puprof_dataessfull'}
        reply=json.dumps(msg)
        data={'body':reply}
        return data
    except ValueError as error:
        valError={"error":str(error)}
        reply=json.dumps(valError)
        data={'body':reply}
        return data
    except psycopg2.DatabaseError as error:
        print("DatabaseError:",error)
        crs.execute('ROLLBACK')


def validate_trans_client_data(val_client_data,crs):# validate input sent sent by client before inserting into transaction table
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



def validate_trans_db_data(crs,val_db_data): # validate data from the transaction table in the db to decide weather to insert new data into the transaction table or not
    check_user_asset_existence="""
    select users_id,symbol 
    from users u,assets a
    where u.users_id=%s and a.id=%s
    """
    crs.execute(check_user_asset_existence,(val_db_data['user_id'],val_db_data['asset_id']))
    check=crs.fetchall()
    if not check:
        raise ValueError('user or selected assets does not exist')
    
    fetch_duplicate="""
    select user_id,asset_id,trans_type,trans_quantity,trans_price,trans_time
    from transaction 
    where user_id=%s and CAST(trans_time AS timestamp) > NOW()- INTERVAL '5 seconds'
    """
    crs.execute(fetch_duplicate,(val_db_data['user_id'],))
    result=crs.fetchall()
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

handle_connections()
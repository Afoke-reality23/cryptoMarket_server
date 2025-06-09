import psycopg
from parserdb import config
import asyncio
import httpx
import traceback
from datetime import datetime
import random
from urllib.parse import urlsplit,parse_qs
import os
from response import response
from dotenv import load_dotenv
from pathlib import Path


def database_column_value_extractor(data):
    keys=",".join(list(data.keys()))
    values=list(data.values())
    placholder=','.join(['%s']*len(data.values()))
    return keys,placholder,values
   

async def connect_db(): # Connect to the data
    try:
        db_url=os.environ.get('DATABASE_URL')
        if db_url:
            if 'sslmode' not in db_url:
                if '?'in db_url:
                    db_url+='&sslmode=require'
                else:
                    db_url+='?sslmode=require'
            conn=await psycopg.AsyncConnection.connect(db_url)
        else:
            db_params=config()
            conn=await psycopg.AsyncConnection.connect(**db_params)
        await conn.set_autocommit(True)
        return conn
    except psycopg.DatabaseError as error:
        traceback.print_exc()
        return None
async def check_unread_message(user_id,crs,chat_id):
    await crs.execute('''
                        select
                            case
                                when seller_id=%s
                                    then buyer_read_status
                                else seller_read_status
                            end as other_user_reas_status,
                            unread_message
                        from chats
                        where chat_id=%s and (seller_id=%s or buyer_id=%s)
                        ''',(user_id,chat_id,user_id,user_id))
    status=await crs.fetchone()
    return status

def generate_trans_id():
    stri='bcdefghjklmnopqrstuvwxyzBCDEFGHLJKLMNOPQRSTUVWXYZ0123456789'
    result='0x'
    for x in range(34):
        result+=random.choice(stri)
    return result

async def generate_username(crs):
   
    first_list = [
    "cobra", "mustang", "whale", "crypto", "ignite", "block", "vaults", "forge", "shift", "core",
    "tokenridge", "pulse", "panther", "ledger", "raptor", "nimbuschain", "vault", "lynx", "hound", "origin",
    "hashfield", "cryptogrid", "mint", "bear", "apexflow", "quantflow", "hash", "stellar", "nodeflux", "meta",
    "primenet", "quantum", "vectorcore", "synerchain", "synergy", "ascend", "stride", "rally", "fox", "blockstream",
    "radianthub", "token", "scale", "originshift", "vector", "byte", "cougar", "eagle", "nova", "boar",
    "grid", "node", "hashzone", "cryptonova", "rhino", "orbit", "drift", "forgenet", "blocksy", "jackal",
    "titanfield", "stormbyte", "corevault", "titan", "wolf", "prime", "dragon", "stridelink", "boltchain", "chainbyte",
    "stake", "stag", "vault","way", "fox","vault", "nex","flow", "bit","grid", "skyc","hain", "vaultify", "trust","net", "zenvault",
    "chain", "graviton", "raider","byte", "spark", "hawk", "apex", "vulture", "nexus", "bound", "token","wave",
    "falcon", "rise", "ascension", "flux", "forge", "climb", "ecliptix", "strive","core", "spark","chain", "blocklift"
]
    second_list = [
    "neuronet", "trader", "venture", "glimmer", "token","pilot", "hash","lane", "cryptic", "dynex", "rally","hub", "metagrid","meta","grid"
    "lumen", "uplift", "falconer", "orbit","byte", "bridge", "byte","field", "spark","shift", "corelumen","core","lumen","block","trail", "phoenix",
    "array", "stellar", "mint","path","mint","path", "vault","port","vault","port" ,"platform", "tiger", "drift","vault", "hash","venture", "aspect", "altgrid",
    "wolf","dog", "wolf","dog","path","stream","path","stream", "byte","code","byte","code", "realm", "astral","byte", "crypto","sphere", "crane", "zenith", "node","burst", "vector","net",
    "otter", "cascade", "pathway", "jaguar", "strata", "nodefy", "pinnacle","byte", "eagle", "orbitalcore", "vaultify",
    "beacon", "cypher", "fox","shift", "chain","stone", "envision", "stream", "launch", "token","storm", "lion", "minter",
    "conflux", "uplift","chain", "flare", "orca", "crypto","burst", "momentum", "stellar", "pillar", "shif","tbyte", "radiant","core",
    "uplink", "shark", "vectra","chain", "gateway", "bull", "lucent", "stellar","hub", "panther", "radiant", "altcoin",
    "path","chain", "titanway", "origrid", "flarehub", "blocker", "uplinknet", "shiftify", "mamba", "buffalo", "lumen","flow",
    "eon", "echelon","chain", "block","nova", "sky","byte", "cheetah", "raven", "viper", "cobra", "badger", "neuron"
]
    firstname=random.choice(first_list)
    secondname=random.choice(second_list)
    username=firstname + secondname
    await crs.execute('select username from users')
    savedusername=await crs.fetchall()
    if (username,) in savedusername:
        num=random.randint(1,1000)
        new_username=username+str(num)
        return new_username
    else:
        return username

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


def meta_data(data):
    method=path=isLoggedIn=valid_session_id=cookies=OAUTH_REQUIRED_PATH=None
    
    try:
        if data is not None:
                cookies={}
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
                    '/texting'
                )
                main_header,body=data.split('\r\n\r\n',1)
                header=urlsplit(main_header).path.split(' ')
                method=header[0]
                path=header[1]
                headers=data.splitlines()
                for header in headers:
                    if header.startswith('Cookie:'):
                        cookie_header=header.replace('Cookie:','')
                        cookie_list=cookie_header.split(';')
                        cookie_tuple=list(tuple(tup.split('=')) for tup in cookie_list)
                        cleaned_cookie=[(k.strip(),v.strip()) for k,v in cookie_tuple]
                        cookies.update(cleaned_cookie)
                isLoggedIn='loggedIn' if cookies.get('session_id') else 'not_logged_in'
                valid_session_id=cookies.get('session_id')
                
        return data,path,method,valid_session_id,isLoggedIn,OAUTH_REQUIRED_PATH,cookies
    except Exception:
        traceback.print_exc()
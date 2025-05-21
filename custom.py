from parserdb import config
import psycopg2
import asyncio
import httpx
import traceback
from datetime import datetime
import random
import os
import urllib.parse as up


def database_column_value_extractor(data):
    keys=",".join(list(data.keys()))
    values=list(data.values())
    placholder=','.join(['%s']*len(data.values()))
    data={
        'columns':keys,
        'values':values,
        'placeholder':placholder
    }
    return data
   

def connect_db(): # Connect to the data
    try:
        db_url=os.environ.get('DATABASE_URL')
        if db_url:
            if 'sslmode' not in db_url:
                if '?'in db_url:
                    db_url+='&sslmode=require'
                else:
                    db_url+='?sslmode=require'
            conn=psycopg2.connect(db_url)
        else:
            db_params=config()
            conn=psycopg2.connect(**db_params)
        conn.autocommit=True
        return conn
    except psycopg2.DatabaseError as error:
        traceback.print_exc()


def generate_trans_id():
    print('hi')
    stri='bcdefghjklmnopqrstuvwxyzBCDEFGHLJKLMNOPQRSTUVWXYZ0123456789'
    result='0x'
    for x in range(34):
        result+=random.choice(stri)
    return result

def generate_username():
   
    first_list = [
    "cobra", "mustang", "whale", "crypto", "ignite", "block", "vaults", "forge", "shift", "core",
    "tokenridge", "pulse", "panther", "ledger", "raptor", "nimbuschain", "vault", "lynx", "hound", "origin",
    "hashfield", "cryptogrid", "mint", "bear", "apexflow", "quantflow", "hash", "stellar", "nodeflux", "meta",
    "primenet", "quantum", "vectorcore", "synerchain", "synergy", "ascend", "stride", "rally", "fox", "blockstream",
    "radianthub", "token", "scale", "originshift", "vector", "byte", "cougar", "eagle", "nova", "boar",
    "grid", "node", "hashzone", "cryptonova", "rhino", "orbit", "drift", "forgenet", "blocksy", "jackal",
    "titanfield", "stormbyte", "corevault", "titan", "wolf", "prime", "dragon", "stridelink", "boltchain", "chainbyte",
    "stake", "stag", "vaultway", "foxvault", "nexflow", "bitgrid", "skychain", "vaultify", "trustnet", "zenvault",
    "chain", "graviton", "raiderbyte", "spark", "hawk", "apex", "vulture", "nexus", "bound", "tokenwave",
    "falcon", "rise", "ascension", "flux", "forge", "climb", "ecliptix", "strivecore", "sparkchain", "blocklift"
]
    second_list = [
    "neuronet", "trader", "venture", "glimmer", "token","pilot", "hash","lane", "cryptic", "dynex", "rallyhub", "metagrid","meta","grid"
    "lumen", "uplift", "falconer", "orbit","byte", "bridge", "byte","field", "spark","shift", "corelumen","core","lumen","block","trail", "phoenix",
    "array", "stellar", "mint","path","mintpath", "vaultport","vault","port" ,"platform", "tiger", "drift","vault", "hash","venture", "aspect", "altgrid",
    "wolfdog", "wolf","dog","pathstream","path","stream", "bytecode","byte","code", "realm", "astralbyte", "cryptosphere", "crane", "zenith", "nodeburst", "vectornet",
    "otter", "cascade", "pathway", "jaguar", "strata", "nodefy", "pinnaclebyte", "eagle", "orbitalcore", "vaultify",
    "beacon", "cypher", "foxshift", "chainstone", "envision", "stream", "launch", "tokenstorm", "lion", "minter",
    "conflux", "upliftchain", "flare", "orca", "cryptoburst", "momentum", "stellar", "pillar", "shiftbyte", "radiantcore",
    "uplink", "shark", "vectrachain", "gateway", "bull", "lucent", "stellarhub", "panther", "radiant", "altcoin",
    "pathchain", "titanway", "origrid", "flarehub", "blocker", "uplinknet", "shiftify", "mamba", "buffalo", "lumenflow",
    "eon", "echelonchain", "blocknova", "skybyte", "cheetah", "raven", "viper", "cobra", "badger", "neuron"
]
    firstname=random.choice(first_list)
    secondname=random.choice(second_list)
    username=firstname + secondname
    # username='Gibberish Masoon'
    crs.execute('select username from users')
    savedusername=crs.fetchall()
    if (username,) in savedusername:
        num=random.randint(1,1000)
        new_username=username+str(num)
        return new_username
    else:
        return username

# generate_username()

# def history_apex():
#     crs.execute('select open,time from chart where asset_id=1')
#     data=crs.fetchall()
#     print(data)

# async def chart_update(crs,data):
#     try:
#         para=",".join(['%s']*len(data))
#         crs.execute(f"insert into chart(asset_id,time,high,low,open,close) values({para})",(data))
#         crs.connection.commit()
#     except psycopg2.DatabaseError as error:
#         print(error)
#         traceback.print_exc()


# async def fill_chart(tg):
#     try:
#         num=11
#         await asyncio.sleep(1)
#         while num < 1000:
#             print(num)
#             async with httpx.AsyncClient() as fetch:
#                 crs.execute(f'select symbol from assets where id={num}')
#                 symbol=crs.fetchone()[0]
#                 crs.execute(f'select asset_name from assets where id={num}')
#                 ass_name=crs.fetchone()[0]
#                 print(ass_name)
               
#                 fetch_chart=await fetch.get(f"https://min-api.cryptocompare.com/data/v2/histoday?fsym={symbol}&tsym=USD&limit=1825")
#                 if fetch_chart.status_code==200:
#                     charts=fetch_chart.json()
#                     chart_data=charts.get('Data').get('Data')
#                     for chart in chart_data:
#                         values=list(chart.values())[:5]
#                         values.insert(0,num)
#                         tg.create_task(chart_update(crs,values))
#                 else:
#                     values=[num,'null','null','null','null','null']
#                     tg.create_task(chart_update(crs,values))
#             num+=1

#     except asyncio.CancelledError as error:
#         traceback.print_exc()

# # asyncio.run(fill_chart())

# async def main():
#     try:
#         async with asyncio.TaskGroup() as tg:
#             tg.create_task(fill_chart(tg))
#     except Exception as error:
#         traceback.print_exc()
#         print(error)

# # asyncio.run(main())

# async def update_id():
#     await asyncio.sleep(3)
#     id_no=1
#     while id_no < 1000:
#         query=f"""
#             insert into assets(asset_id,asset_idname,asset_name,symbol,asset_type,asset_price,market_cap,percent_chng_24h,volumn_quote_24h,percent_chng_7d,volumn_quote_7d,percent_chng_30d,volumn_quote_30d,logo)
#             select asset_id,asset_idname,asset_name,symbol,asset_type,asset_price,market_cap,percent_chng_24h,volume_quote_24h,percent_chng_7d,volumne_quote_7d,percent_chng_30d,volume_quote_30d,logo_url from asset where new_id={id_no}
#             """
#         crs.execute(query)
#         crs.execute(f'select id,asset_name,symbol from assets where id={id_no}')
#         asset=crs.fetchone()
#         print(asset)
#         id_no+=1



# async def update_func(crs,table,assets):
#         query=f"""
#             update {table}
#             set
#             volume_change_24h=%s,
#             volume_change_7d=%s,
#             volume_change_30d=%s,
#             percent_change_7d=%s,
#             percent_change_30d=%s
#             where id=%s
#             """
#         crs.execute(query,assets)
# async def update_funcA(crs,table,assets):
#         query=f"update {table} set asset_type=%s where id=%s"
#         crs.execute(query,assets)

# async def insert_func(crs,table,infos):
#         cols=",".join(list(infos.keys()))
#         placeholders=",".join(['%s'] * len(list(infos.values())))
#         values=list(infos.values())
#         crs.execute(f'insert into {table}({cols}) values({placeholders})',values) 
    


# async def processing(tg):
#     try:
#         async with httpx.AsyncClient() as fetch:
#             crs.execute('select no,id,symbol from coin_gecko_assets where no > 5')
#             gecko_detail=crs.fetchall()
#             # print(gecko_detail)
#             for no,asstId,symbol in gecko_detail:
#                 await asyncio.sleep(1)
#                 request= await fetch.get(f'https://data-api.coindesk.com/asset/v1/metadata?asset={symbol}&asset_lookup_priority=SYMBOL&quote_asset=USD')
#                 if request.status_code==200:
#                     data=request.json()
#                     asset=data['Data']
#                     main_asset=[]
#                     if asstId != asset.get('URI')and symbol != asset.get('URI'):
#                         print(f'rejected asset no:{no} {asstId}')
#                         continue
#                     print(no,symbol)
#                     main=(
#                         asset.get('URI','NULL'),
#                         asset.get('ASSET_TYPE','NULL'),
#                         asset.get('LOGO_URL','NULL'),
#                     )
#                     leaders=asset.get('PROJECT_LEADERS') or []
#                     main_asset.append(main)
#                     detail={
#                     'id':asstId,
#                     'description': asset.get('ASSET_DESCRIPTION','null'),
#                     'snippet':asset.get('ASSET_DESCRIPTION_SNIPPET','null'),
#                     'summary':asset.get('ASSET_DESCRIPTION_SUMMARY','null'),
#                     'website':asset.get('WEBSITE_URL','null'),
#                     'whitepaper':asset.get('WHITEP_PAPER_URL','null'),
#                     'founder':leaders[0].get('FULL_NAME','NULL') if len(leaders) > 0 else 'null'
#                     }
#                     # sc=asset.get('SUPPLY_CIRCULATING',0)
#                     # market={
#                     # 'asset_symbol':asset.get('SYMBOL','NULL'),
#                     # 'supply_circulating':float(asset.get('SUPPLY_CIRCULATING',0)) if asset.get('SUPPLY_CIRCULATING',0) else 0,
#                     # 'supply_total':float(asset.get('SUPPLY_TOTAL',0)) if asset.get('SUPPLY_TOTAL',0) else  0,
#                     # 'supply_max':float(asset.get('SUPPLY_MAX',0)) if asset.get('SUPPLY_MAX',0) else 0,
#                     # 'supply_issued':float(asset.get('SUPPLY_ISSUED',0)) if asset.get('SUPPLY_ISSUED',0) else 0,
#                     # 'volumn_change_24h':float(asset.get('SPOT_MOVING_24_HOUR_QUOTE_VOLUME_USD',0)) if asset.get('SPOT_MOVING_24_HOUR_QUOTE_VOLUME_USD',0) else 0,
#                     # 'volumn_change_7d':float(asset.get('SPOT_MOVING_7_DAY_QUOTE_VOLUME_USD',0)),
#                     # 'volumn_change_30d':float(asset.get('SPOT_MOVING_30_DAY_QUOTE_VOLUME_USD',0)),
#                     # 'percent_change_7d':float(asset.get('SPOT_MOVING_7_DAY_CHANGE_PERCENTAGE_USD',0)),
#                     # 'percent_change_30d':float(asset.get('SPOT_MOVING_30_DAY_CHANGE_PERCENTAGE_USD',0))
#                     # }
#                     market=[
#                         float(asset.get('SPOT_MOVING_24_HOUR_QUOTE_VOLUME_USD',0)) if asset.get('SPOT_MOVING_24_HOUR_QUOTE_VOLUME_USD',0) else 0,float(asset.get('SPOT_MOVING_7_DAY_QUOTE_VOLUME_USD',0)),
#                         float(asset.get('SPOT_MOVING_30_DAY_QUOTE_VOLUME_USD',0)),
#                         float(asset.get('SPOT_MOVING_7_DAY_CHANGE_PERCENTAGE_USD',0)),
#                         float(asset.get('SPOT_MOVING_30_DAY_CHANGE_PERCENTAGE_USD',0)),
#                         asstId
#                     ]
#                     # markets.append(market)
#                     asset_type=[asset.get('ASSET_TYPE','null'),asstId]
#                 tg.create_task(update_func(crs,'gecko_market',market))
#                 tg.create_task(update_funcA(crs,'coin_gecko_assets',asset_type))
#                 tg.create_task(insert_func(crs,'gecko_details',detail))
#                 # tg.create_task(insert_func(crs,'asset_details',details))
#     except asyncio.CancelledError as error:
#         print(error)
#         traceback.print_exc()
#         raise asyncio.CancelledError
    

# async def main():
#     try:
#         async with asyncio.TaskGroup() as tg:
#             tg.create_task(processing(tg))
#     except Exception as error:
#         print(error)
#         traceback.print_exc()

# # asyncio.run(main())

# def update_db():
#     # crs.execute('update assets set symbol=%s where symbol=%s',('GMTT','GOMINING'))
#     crs.execute('update assets set asset_name=%s,symbol=%s where id=%s',('dydx','DYDX',539))
#     # crs.execute("select * from assets where symbol='USDE'")
#     # ass=crs.fetchall()
#     # print(ass)
#     # crs.execute('select id from assets where uri is null order by id')
#     # ast_ids=crs.fetchall()
#     # print(ast_ids)
#     # for ids in ast_ids:
#     #     print(ids[0])



# # def experi():
# #     crs.execute('select id from gecko_details')
# #     details_id=crs.fetchall()
# #     crs.execute('select id from coin_gecko_assets')
# #     asset_id=crs.fetchall()
# #     asset_set={x for x in asset_id}
# #     print('asset',len(asset_set))
# #     details_set={x for x in details_id}
# #     print('details',len(details_set))
# #     left_out_id=asset_set-details_set
# #     print('leftout',len(left_out_id))
# #     print(left_out_id)
#     # print(len(left_out_id),left_out_id)
#     # details_set=set()
#     # for x in de
#     # crs.execute('select id from coin_gecko_assets')
#     # asset_id=crs.fetchall()

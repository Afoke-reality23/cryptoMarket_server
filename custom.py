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
    # data={
    #     'columns':keys,
    #     'values':values,
    #     'placeholder':placholder
    # }
    return keys,placholder,values
   

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

def generate_username(crs):
   
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


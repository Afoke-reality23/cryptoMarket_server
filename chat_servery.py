import asyncio
import os
from custom import connect_db

async def handle_client(reader,writer):
    addr=writer.get_extra_info('peername')
    print(f'Connected:{addr}')
    while True:
        data=await reader.readuntil(b'\r\n\r\n')
        print(data)
async def main():
    port=int(os.environ.get('PORT',1999))
    server=await asyncio.start_server(handle_client,'',port)
    print(f'server is running on port: {port}')
    async with server:
        await server.serve_forever()

asyncio.run(main())
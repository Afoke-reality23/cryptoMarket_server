import socket
import traceback
def auth_header(status='200 OK'):
    headers = (
                f"HTTP/1.1 {status}\r\n"
                "Content-Type: application/json\r\n"
                "Access-Control-Allow-Origin:https://realcryptomarket.netlify.app\r\n"
                # "Access-Control-Allow-Origin:http://127.0.0.1:5500\r\n"
                "Access-Control-Allow-Methods: GET,POST,OPTIONS\r\n"
                "Access-Control-Allow-Headers: Content-Type\r\n"
                "Access-Control-Allow-Credentials:true\r\n"
                )
    return headers

async def response(writer,method='',data='',session_id='',max_age=''):# Response route
    try:
        if session_id:
            cookie_attr='HttpOnly;Path=/;SameSite=None;Secure'
            if max_age==0:
                cookie_attr+=f'; Max-Age={max_age}'
            set_cookie= f"Set-Cookie: session_id={session_id};{cookie_attr}\r\n"
            header=auth_header() + set_cookie + '\r\n\r\n'
        else:
            if method==b'OPTIONS' or b'HEAD':
                preflight_header=auth_header('204 No Content')
                header=preflight_header + '\r\n\r\n'
                # return header
            else:
                header=auth_header() + '\r\n\r\n'
        if data:
            rsp=header + data
        else:
            rsp=header
        writer.write(rsp.encode('utf-8'))
        await writer.drain()
       
    except ConnectionResetError:
        print('client closed the connection before full response was send')
    except Exception:
        traceback.print_exe()
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


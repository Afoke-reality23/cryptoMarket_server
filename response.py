import socket
from urllib.parse import urlparse,parse_qs

def auth_header():
    headers = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: application/json\r\n"
                "Access-Control-Allow-Origin:https://realcryptomarket.netlify.app\r\n"
                # "Access-Control-Allow-Origin:http://127.0.0.1:5500\r\n"
                "Access-Control-Allow-Methods: GET,POST,OPTIONS\r\n"
                "Access-Control-Allow-Headers: Content-Type\r\n"
                "Access-Control-Allow-Credentials:true\r\n"
                )
    return headers

def response(sock,method,data='',session_id='',max_age=''):# Response route
    if session_id:
        print('session id header sent')
        print(max_age)
        set_cookie= f"Set-Cookie: session_id={session_id};HttpOnly;Path=/;SameSite=Strict;Domain=127.0.0.1\r\n"
        del_cookie= f"Set-Cookie: session_id={session_id};HttpOnly;Path=/;SameSite=Strict; max-age={max_age}\r\n"
        cookie_header=auth_header()
        cookie=set_cookie if max_age else del_cookie
        header=cookie_header + cookie + '\r\n\r\n'
    else:
        if method=='OPTIONS':
            preflight_header=auth_header().replace('200 OK','204 No Content')
            header=preflight_header + '\r\n\r\n'
        else:
            header=auth_header() + '\r\n\r\n'
    # print(header)
    if data:
        rsp=header + data
    else:
        rsp=header
    # print(rsp)
    sock.send(rsp.encode('utf-8'))
    sock.shutdown(socket.SHUT_RDWR)

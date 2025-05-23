import socket
from urllib.parse import urlparse,parse_qs

def auth_header(status='200 OK'):
    headers = (
                f"HTTP/1.1 {status} \r\n"
                "Content-Type: application/json\r\n"
                "Access-Control-Allow-Origin: https://realcryptomarket.netlify.app\r\n"
                # "Access-Control-Allow-Origin:http://127.0.0.1:5500\r\n"
                # "Cache-Control:no-cache\r\n"
                "Access-Control-Allow-Methods: GET,POST,OPTIONS\r\n"
                "Access-Control-Allow-Headers: Content-Type\r\n"
                "Access-Control-Allow-Credentials:true\r\n"
                )
    return headers

def response(sock,method,data='',session_id='',max_age=None):# Response route
    print('inside response')
    if session_id:
        cookie_attr="Path=/;HttpOnly;SameSite=None;Secure"
        if max_age == None:
            cookie_attr+=f'; Max-Age={max_age}'
        set_cookie+=f'Set-Cookie:session_id={session_id};{cookie_attr}\r\n'
        header=auth_header() + set_cookie + '\r\n'
    else:
        if method=='OPTIONS':
            header=auth_header('204 No Content') + '\r\n'
        else:
            header=auth_header() + '\r\n'
    # print(header)
    if data:
        rsp=header + data
    else:
        rsp=header
    sock.send(rsp.encode('utf-8'))
    sock.shutdown(socket.SHUT_RDWR)

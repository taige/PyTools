from tsproxy.listener import *


class HttpsListener(HttpListener):

    def __init__(self, listen_addr, connector, loop=None, **kwargs):
        kwargs.setdefault('name', 'https')
        # ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        # ctx.load_cert_chain('/Users/taige/selfsigned.cert', '/Users/taige/selfsigned.key')
        super().__init__(listen_addr, connector, loop=loop, **kwargs)

    def do_https_forward(self, request, connection, peer_conn):
        connection.writer.write(httphelper.https_proxy_response(request.version))
        connection.to_ssl()
        yield from common.forward_forever(connection, peer_conn, is_responsed=True)




import asyncio
import collections
import logging
import time
from http.client import responses
from urllib.parse import urlparse
from urllib.parse import urlunparse

import aiohttp
from aiohttp import errors
from aiohttp import hdrs

from tsproxy import common

NO_CONTENT = 204
NOT_MODIFIED = 304

logger = logging.getLogger(__name__)

RequestMessage = collections.namedtuple(
    'RequestMessage',
    ['method', 'path', 'version', 'headers', 'raw_headers',
     'should_close', 'compression', 'request_line', 'url', 'body', 'error', 'request_time'])

RequestURL = collections.namedtuple(
    'RequestURL',
    ['full_url', 'full_path', 'scheme', 'netloc', 'hostname', 'port', 'path', 'query'])

ResponseMessage = collections.namedtuple(
    'ResponseMessage',
    ['version', 'code', 'reason', 'headers', 'raw_headers',
     'should_close', 'compression', 'chunked', 'content_length', 'response_line',
     'head_length', 'body', 'raw_data', 'error', 'response_time'])


class HttpParser(aiohttp.protocol.HttpParser):

    def __init__(self, max_line_size=10240, max_headers=32768,
                 max_field_size=10240):
        super().__init__(max_line_size, max_headers, max_field_size)

    def _parse_version(self, version):
        try:
            if version.startswith('HTTP/'):
                n1, n2 = version[5:].split('.', 1)
                obj_version = aiohttp.HttpVersion(int(n1), int(n2))
            else:
                raise errors.BadStatusLine(version)
        except:
            raise errors.BadStatusLine(version)
        if obj_version <= aiohttp.protocol.HttpVersion10:  # HTTP 1.0 must asks to not close
            close = True
        else:  # HTTP 1.1 must ask to close.
            close = False
        return close

    def parse_headers(self, lines, status=200, request_method='GET', default_close=True):
        headers, raw_headers, close, compression = super().parse_headers(lines)

        # are we using the chunked-style of transfer encoding?
        tr_enc = headers.get(hdrs.TRANSFER_ENCODING)
        if tr_enc and tr_enc.lower() == "chunked":
            chunked = True
        else:
            chunked = False

        # do we have a Content-Length?
        # NOTE: RFC 2616, S4.4, #3 says we ignore this if tr_enc is "chunked"
        content_length = headers.get(hdrs.CONTENT_LENGTH)

        # are we using the chunked-style of transfer encoding?
        if content_length and not chunked:
            try:
                length = int(content_length)
            except ValueError:
                length = None
            else:
                if length < 0:  # ignore nonsensical negative lengths
                    length = None
        else:
            length = None

        # does the body have a fixed length? (of zero)
        if length is None \
                and (status == NO_CONTENT or status == NOT_MODIFIED or 100 <= status < 200 or request_method == "HEAD"):
            length = 0

        # then the headers weren't set in the request
        return headers, raw_headers, default_close if close is None else close, compression, chunked, length


def bad_request(error=None, request_line=None, method=None, version=None, url=None, close=True, timeout=None, request_time=None):
    if timeout and not error:
        error = errors.BadHttpMessage('read request timeout(%d)' % timeout)
    return RequestMessage(
        method, None if not url else url.full_path, version, None, None,
        close, None, request_line, url, None, error, request_time if request_time else time.time())


# ['version', 'code', 'reason', 'headers', 'raw_headers',
#  'should_close', 'compression', 'chunked', 'content_length', 'response_line',
#  'head_length', 'var', 'body', 'raw_data', 'error'])
def bad_response(error, response_line=None, raw_data=b'', timeout=None):
    if timeout and not error:
        error = errors.BadHttpMessage('read response timeout(%d)' % timeout)
    return ResponseMessage(
        None, error.code, error.message, None, None,
        None, None, False, None, response_line,
        len(raw_data), None, raw_data, error, time.time())


class HttpRequestParser(HttpParser):

    def parse_request(self, reader, raw_data=b'', read_timeout=common.default_timeout):
        # read HTTP message (request line + headers)
        request_time = time.time()
        try:
            with common.Timeout(read_timeout):
                if raw_data and raw_data.find(b'\r\n') > 0:
                    pass
                else:
                    raw_data += yield from reader.readuntil(b'\r\n')

                request_line, _ = raw_data.split(b'\r\n', 1)
                request_line = request_line.decode('utf-8', 'surrogateescape')
                method, version, url, close = self._parse_requestline(request_line)

                while True:
                    end_index = raw_data.find(b'\r\n\r\n')
                    if raw_data and end_index > 0:
                        header_lines = raw_data[:end_index+4]
                        body = raw_data[end_index+4:]
                        break
                    else:
                        raw_data += yield from reader.readuntil(b'\r\n')

                _request = self._parse_request(header_lines, request_line, method, version, url, close, request_time)
        except EOFError:
            return None
        except aiohttp.HttpProcessingError as bad_req:
            return bad_request(bad_req, request_line, request_time=request_time)
        except asyncio.TimeoutError:
            return bad_request(errors.BadHttpMessage('read request timeout(%d)' % read_timeout), request_line, method, version, url, close, request_time=request_time)
        except asyncio.LimitOverrunError as exc:
            return bad_request(errors.LineTooLong('%s' % raw_data, exc.consumed), request_line, method, version, url, close, request_time=request_time)

        chunk_len = len(reader)
        if chunk_len > 0:
            body += yield from reader.read_bytes(size=chunk_len)
        if len(body) > 0:
            _request = _request._replace(body=body)

        return _request

    def _parse_requestline(self, line):
        # request line
        # line = request_line.decode('utf-8', 'surrogateescape')
        try:
            method, path, version = line.split(None, 2)
        except ValueError:
            raise errors.BadStatusLine(line) from None

        # method
        method = method.upper()
        if not aiohttp.protocol.METHRE.match(method):
            raise errors.BadStatusLine(method)

        # version
        close = self._parse_version(version)

        # path
        url = self.parse_path(path, method)

        return method, version, url, close

    def _parse_request(self, raw_data, line, method, version, url, default_close=True, request_time=None):
        lines = raw_data.split(b'\r\n')

        # read headers
        headers, raw_headers, close, compression, _, _ = self.parse_headers(lines, default_close=default_close)
        if close is None:  # then the headers weren't set in the request
            close = default_close

        return RequestMessage(
            method, url.full_path, version, headers, raw_headers,
            close, compression, line, url, b'', None, request_time if request_time else time.time())

    @staticmethod
    def parse_path(path, method):
        '''    ['full_url', 'full_path', 'scheme', 'netloc', 'hostname', 'port', 'path', 'query'])
        '''
        url = urlparse(path)
        result = {'scheme': url.scheme if url.scheme else 'http' if url.netloc else '',
                  'netloc': url.netloc,
                  'path': url.path,
                  'query': url.query}
        result['full_url'] = urlunparse((result['scheme'], result['netloc'], url.path, url.params, url.query, url.fragment))
        result['full_path'] = urlunparse(('', '', url.path, url.params, url.query, url.fragment))
        if method == 'CONNECT':
            hostname, port = url.path.split(':')
            result['hostname'] = hostname
            result['port'] = int(port) if port else 443
        else:
            result['hostname'] = url.hostname if url.hostname else None
            result['port'] = int(url.port) if url.port else 80

        return RequestURL(**result)


class HttpResponseParser(HttpParser):
    """Read response status line and headers.

    BadStatusLine could be raised in case of any errors in status line.
    Returns RawResponseMessage"""

    def parse_response(self, raw_data, request_method='GET'):
        # read HTTP message (response line + headers)
        try:
            if raw_data and raw_data.find(b'\r\n') > 0:
                pass
            else:
                return None, 0

            response_line, _ = raw_data.split(b'\r\n', 1)
            response_line = response_line.decode('utf-8', 'surrogateescape')
            version, status, reason, default_close = self._parse_responseline(response_line)

            if raw_data and raw_data.find(b'\r\n\r\n') > 0:
                consumed = raw_data.find(b'\r\n\r\n') + 4
            else:
                return None, 0

            _response = self._parse_response(raw_data, response_line, version, status, reason, request_method, default_close)
        except aiohttp.HttpProcessingError as bad_req:
            return bad_response(bad_req, response_line, raw_data), 0

        body_len = len(raw_data) - consumed
        if body_len > 0:
            if _response.content_length is not None and body_len > _response.content_length:
                body_len = _response.content_length
            body = raw_data[consumed: consumed + body_len]
            _response = _response._replace(body=body)
            consumed += body_len

        return _response, consumed

    def _parse_response(self, raw_data, line, version, status, reason, request_method='GET', default_close=True):
        lines = raw_data.split(b'\r\n')

        # read headers
        headers, raw_headers, close, compression, chunked, length = self.parse_headers(lines, status, request_method, default_close)

        # ['version', 'code', 'reason', 'headers', 'raw_headers',
        #  'should_close', 'compression', 'chunked', 'content_length', 'response_line',
        #  'head_length', 'var', 'body', 'raw_data', 'error'])
        return ResponseMessage(
            version, status, reason.strip(), headers, raw_headers,
            close, compression, chunked, length, line,
            len(raw_data), b'', raw_data, None, time.time())

    def _parse_responseline(self, line):
        # response line
        try:
            version, status = line.split(None, 1)
        except ValueError:
            raise errors.BadStatusLine(line) from None
        else:
            try:
                status, reason = status.split(None, 1)
            except ValueError:
                reason = ''

        # version
        close = self._parse_version(version)

        # The status code is a three-digit number
        try:
            status = int(status)
        except ValueError:
            raise errors.BadStatusLine(line) from None

        if status < 100 or status > 999:
            raise errors.BadStatusLine(line)

        return version, status, reason, close


# ['version', 'code', 'reason', 'headers', 'raw_headers',
#  'should_close', 'compression', 'chunked', 'content_length', 'response_line',
#  'head_length', 'var', 'body', 'raw_data', 'error'])
def https_proxy_response(version=None, headers=None):
    if not version:
        version = 'HTTP/1.1'
    response_line = '%s 200 Connection established' % version
    raw_data = (response_line.encode() + b'\r\nProxy-Agent: taige-Smart-Proxy/0.1.0\r\n')
    raw_headers = []
    if headers:
        for key in headers:
            raw_data += ('%s: %s\r\n' % (key, headers[key])).encode()
            raw_headers.append((key, headers[key]))
    raw_data += b'\r\n'
    return ResponseMessage(
        version, 200, 'Connection established', headers, raw_headers,
        True, False, False, None, response_line,
        len(raw_data), b'', raw_data, None, time.time())


def http_response(version=None, status=200, reason=None, headers=None, content=None):
    if not version:
        version = 'HTTP/1.1'
    if not reason:
        reason = responses[status] + '(TSP)'

    response_line = '%s %d %s' % (version, status, reason)
    raw_data = ('%s\r\n' % response_line).encode()
    raw_headers = []
    if headers:
        for key in headers:
            raw_data += ('%s: %s\r\n' % (key, headers[key])).encode()
            raw_headers.append((key, headers[key]))
    if content:
        raw_data += ('Content-Length: %d\r\n' % len(content)).encode()
        raw_data += b'\r\n' + content.encode()
    return ResponseMessage(
        version, status, reason, headers, raw_headers,
        True, False, False, None if not content else len(content), response_line,
        len(raw_data), content.encode() if content else b'', raw_data, None, time.time())


def test():

    request_text = (
        b'GET http://user:pass@pki.google.com/add?jp3.iss.tf&us1&hk2 HTTP/1.1\r\n'
        # b'GET http://user:pass@pki.google.com/GIAG2.crt;some_par?sdsf=sdf#some_fra HTTP/1.1\r\n'
        # b'HEAD /sdsdfs HTTP/1.1\r\n'
        # b'GET http://pki.google.com/GIAG2.crt;some_par?sdsf=sdf#some_fra HTTP/1.1\r\n'
        # b'CONNECT photos-thumb.dropbox.com:443 HTTP/1.1\r\n'
        b'Host: pki.google.com\r\n'
        b'Proxy-Connection: keep-alive\r\n'
        b'Accept: */*\r\n'
        b'User-Agent: ocspd/1.0.3\r\n'
        b'Accept-Language: zh-cn\r\n'
        b'Content-Length: 15\r\n'
        b'Accept-Encoding: gzip, deflate\r\n'
        b'Connection: keep-alive\r\n\r\n'
        b'safasdfa;jd;afd'
        )
    response_text = (
        b'HTTP/1.1 400 Bad Request\r\n'
        b'Server: bfe/1.0.8.14\r\n'
        b'Date: Sat, 19 Mar 2016 05:07:02 GMT\r\n\r\n'
        b'AAAsdfsdfsdf'
    )
    parser = HttpRequestParser()
    # res = parser.request_parse(request_text, hostname='www.google.com', port=80)
    res = parser._parse_request(request_text, None, None, None, None)
    test_parse(res)


def test_parse(res):
    print(res)
    # print(res.error_code)       # None  (check this first)
    # print(res.command)          # "GET"
    print(res.path)             # "/who/ken/trust.html"
    print(res.version)  # "HTTP/1.1"
    print(len(res.headers))     # 3
    # # print(request.headers.keys())   # ['accept-charset', 'host', 'accept']
    key = 'Proxy-Connection'
    if key in res.headers:
        print('del %s => %s' % (key, res.headers[key]))
        del res.headers[key]
    for key in res.headers:
        print('%s => %s' % (key, res.headers[key]))
    # print(res.headers['host'])  # "cm.bell-labs.com"


def test_unparse(res, parser):
    unrequest = parser.unparse_request(res)
    print("'" + unrequest.decode() + "'")

    # res = parser.read_response(response_text, 'GET')
    # print(res)
    # print("'%s' '%d' '%s' '%d' '%s'" % (res.version, res.status, res.reason, res.head_length, res.headers))
    # print("body='%s'" % response_text[res.head_length:].decode())

    # request_text = (
    #     # b'GET http://pki.google.com/GIAG2.crt HTTP/1.1\r\n'
    #     b'CONNECT photos-thumb.dropbox.com:443 HTTP/1.1\r\n'
    #     b'Host: pki.google.com\r\n'
    #     b'Proxy-Connection: keep-alive\r\n'
    #     b'Accept: */*\r\n'
    #     b'User-Agent: ocspd/1.0.3\r\n'
    #     b'Accept-Language: zh-cn\r\n'
    #     b'Accept-Encoding: gzip, deflate\r\n'
    #     b'Connection: keep-alive\r\n\r\n'
    #     b'safasdfa;jd;afd'
    #     )
    #
    # request.do_parse(request_text)
    #
    # print(request.error_code)       # None  (check this first)
    # print(request.command)          # "GET"
    # print(request.path)             # "/who/ken/trust.html"
    # print(request.request_version)  # "HTTP/1.1"
    # print(len(request.headers))     # 3
    # # print(request.headers.keys())   # ['accept-charset', 'host', 'accept']
    # for key in request.headers:
    #     print('%s => %s' % (key, request.headers[key]))
    # print(request.headers['host'])  # "cm.bell-labs.com"


if __name__ == '__main__':
    test()

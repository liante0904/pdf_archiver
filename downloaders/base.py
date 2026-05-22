import aiohttp
import ssl
from utils import _browser_like_headers, _cookie_header_from_response

class BaseDownloader:
    def __init__(self, session: aiohttp.ClientSession = None):
        self.session = session

    async def get_session(self):
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=45)
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
            connector = aiohttp.TCPConnector(ssl=ssl_ctx)
            self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
            self._own_session = True
        else:
            self._own_session = False
        return self.session

    async def close(self):
        if hasattr(self, '_own_session') and self._own_session and self.session:
            await self.session.close()

    async def fetch_cookies(self, url):
        session = await self.get_session()
        async with session.get(url, headers=_browser_like_headers(), allow_redirects=True) as resp:
            await resp.read()
            return _cookie_header_from_response(resp)

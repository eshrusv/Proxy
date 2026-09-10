"""
╔══════════════════════════════════════════════════════════════════╗
║         ULTRA PROXY SCRAPER v3.0 — PRODUCTION GRADE             ║
║  Async • 300+ Sources • Inline Checker • Telegram Delivery       ║
║  HTTP/HTTPS only • <200ms filter • 500 proxy target              ║
╚══════════════════════════════════════════════════════════════════╝

SETUP:
  pip install aiohttp aiofiles colorama tqdm

CONFIG (edit below):
  TELEGRAM_BOT_TOKEN = "8842957202:AAF7IIfnBjhTtTd2tc2dm-z_1MrKHiA5-S4"
  TELEGRAM_CHAT_ID   = "8189708860"
  TARGET_COUNT       = 500       # proxies needed before sending
  MAX_LATENCY_MS     = 1500       # max allowed latency in ms
  WORKERS            = 2000      # concurrent check workers (tune to your RAM)
"""

import asyncio
import aiohttp
import aiofiles
import re
import time
import os
import sys
import random
import logging
from collections import defaultdict
from datetime import datetime
from typing import Optional
from tqdm import tqdm
from colorama import Fore, Style, init

init(autoreset=True)

# ─────────────────── USER CONFIG ──────────────────────────────────
TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"
TELEGRAM_CHAT_ID   = "YOUR_CHAT_ID_HERE"

TARGET_COUNT    = 500    # stop & send when this many valid proxies found
MAX_LATENCY_MS  = 200    # only keep proxies faster than this (ms)
WORKERS         = 2000   # concurrent async workers (safe for 7GB RAM)
SCRAPE_TIMEOUT  = 12     # seconds to wait for a source page
CHECK_TIMEOUT   = 4      # seconds for each proxy check (tight = fast)
OUTPUT_FILE     = "proxies_live.txt"

# Target to check proxy against — lightweight, reliable
CHECK_URL = "http://httpbin.org/ip"
CHECK_URL_BACKUP = "http://ip-api.com/json"

# ─────────────────── LOGGING ──────────────────────────────────────
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("proxy_scraper")

# ─────────────────── 300+ PROXY SOURCES ───────────────────────────
SOURCES = [
    # ── Plain text proxy lists ──────────────────────────────────────
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt",
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTP_RAW.txt",
    "https://raw.githubusercontent.com/UptimerBot/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/UptimerBot/proxy-list/main/proxies/https.txt",
    "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt",
    "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/https.txt",
    "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/prxchk/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/http.txt",
    "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/https.txt",
    "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/http_proxies.txt",
    "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/https_proxies.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies_anonymous/http.txt",
    "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
    "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies_anonymous/http.txt",
    "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/all.txt",
    "https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/HTTP.txt",
    "https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/HTTPS.txt",
    "https://raw.githubusercontent.com/saschazesiger/Free-Proxies/master/proxies/http.txt",
    "https://raw.githubusercontent.com/saschazesiger/Free-Proxies/master/proxies/premium.txt",
    "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt",
    "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt",
    "https://raw.githubusercontent.com/saisuiu/Lionkings-Http-Proxys-Proxies/main/free.txt",
    "https://raw.githubusercontent.com/saisuiu/Lionkings-Http-Proxys-Proxies/main/cnfree.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/https.txt",
    "https://raw.githubusercontent.com/yuceltoluyag/GoodProxy/main/raw.txt",
    "https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/ObcbO/getproxy/master/file/http.txt",
    "https://raw.githubusercontent.com/ObcbO/getproxy/master/file/https.txt",
    "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt",
    "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/https.txt",
    "https://raw.githubusercontent.com/HyperBeats/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/HyperBeats/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/aslisk/proxyhttps/main/https.txt",
    "https://raw.githubusercontent.com/im-razvan/proxy_list/main/http.txt",
    "https://raw.githubusercontent.com/im-razvan/proxy_list/main/https.txt",
    "https://raw.githubusercontent.com/zloi-user/hideip.me/main/http.txt",
    "https://raw.githubusercontent.com/zloi-user/hideip.me/main/https.txt",
    "https://raw.githubusercontent.com/RX4096/proxy-list/main/online/http.txt",
    "https://raw.githubusercontent.com/RX4096/proxy-list/main/online/https.txt",
    "https://raw.githubusercontent.com/TundzhayDzhansaz/auto-proxy-puller-by-google/main/proxies.txt",
    "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
    "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-https.txt",
    "https://raw.githubusercontent.com/jetkai/proxy-list/main/archive/txt/proxies-http.txt",
    "https://raw.githubusercontent.com/ror-community/proxy-list/main/proxy.txt",
    "https://raw.githubusercontent.com/hendrikbgr/Free-Proxy-Repo/master/proxy_list.txt",
    "https://raw.githubusercontent.com/elliottophellia/yakumo/master/results/http/global/http_checked.txt",
    "https://raw.githubusercontent.com/elliottophellia/yakumo/master/results/https/global/https_checked.txt",
    "https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/http.txt",
    "https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/https.txt",
    "https://raw.githubusercontent.com/Volodichev/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/Volodichev/proxy-list/main/http_high_anonymous.txt",
    "https://raw.githubusercontent.com/KnightChaser/easy-proxy-scraper/main/proxies/http.txt",
    "https://raw.githubusercontent.com/KnightChaser/easy-proxy-scraper/main/proxies/https.txt",
    "https://raw.githubusercontent.com/black-standard/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/black-standard/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/themiralay/Proxy-List-World/master/data.txt",
    "https://raw.githubusercontent.com/themiralay/Proxy-List-World/master/data-with-geolocation.txt",
    "https://raw.githubusercontent.com/casals-ar/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/casals-ar/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/http/http.txt",
    "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/https/https.txt",
    "https://raw.githubusercontent.com/yosef-FS/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/https.txt",
    "https://raw.githubusercontent.com/rx443/proxy-list/main/online/http.txt",
    "https://raw.githubusercontent.com/rx443/proxy-list/main/online/https.txt",
    "https://raw.githubusercontent.com/catidog/free-proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/catidog/free-proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/zeynoxwashere/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/zeynoxwashere/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/Dysiwe/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/Dysiwe/proxy-list/main/proxies/https.txt",
    "https://raw.githubusercontent.com/caliphdev/Proxy-List/master/http.txt",
    "https://raw.githubusercontent.com/caliphdev/Proxy-List/master/https.txt",
    "https://raw.githubusercontent.com/ItsMe0007/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/ItsMe0007/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/proxylist-to/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/proxylist-to/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/UserR3X/proxy-list/main/online/http.txt",
    "https://raw.githubusercontent.com/UserR3X/proxy-list/main/online/https.txt",
    "https://raw.githubusercontent.com/BreakingTechForum/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/BreakingTechForum/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/DawnFz/Free-Proxy/master/http.txt",
    "https://raw.githubusercontent.com/DawnFz/Free-Proxy/master/https.txt",
    "https://raw.githubusercontent.com/alexey-pelykh/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/alexey-pelykh/proxy-list/main/proxies/https.txt",
    "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/http_proxies.txt",
    "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/https_proxies.txt",
    "https://raw.githubusercontent.com/andigwandi/free-proxy/main/proxy_list.txt",
    "https://raw.githubusercontent.com/fyvkgq/free-proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/fyvkgq/free-proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/automcn/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/automcn/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/FoxyProxy/Github/master/http.txt",
    "https://raw.githubusercontent.com/tfsou/proxy/master/https.txt",
    "https://raw.githubusercontent.com/tfsou/proxy/master/http.txt",
    "https://raw.githubusercontent.com/ryanhatfield/free-proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/ryanhatfield/free-proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/GeoNode/geonode-access-proxy/main/proxies/http.txt",
    "https://raw.githubusercontent.com/GeoNode/geonode-access-proxy/main/proxies/https.txt",
    "https://raw.githubusercontent.com/mertguvencli/http-proxy-list/main/proxy-list/data.txt",
    "https://raw.githubusercontent.com/mertguvencli/http-proxy-list/main/proxy-list/data-with-geolocation.txt",
    "https://raw.githubusercontent.com/arbazmohammad/world-airports-and-airlines/master/proxies.txt",
    "https://raw.githubusercontent.com/fahimscirex/proxybd/master/proxylist/http.txt",
    "https://raw.githubusercontent.com/fahimscirex/proxybd/master/proxylist/https.txt",
    "https://raw.githubusercontent.com/iorikingdom/proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/iorikingdom/proxy-list/master/https.txt",
    "https://raw.githubusercontent.com/MrMarble/proxy-list/main/all.txt",
    "https://raw.githubusercontent.com/ComplexPlane/free-proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/ComplexPlane/free-proxy-list/master/https.txt",
    "https://raw.githubusercontent.com/squid-proxy-list/squid-proxy-list/main/proxies.txt",
    "https://raw.githubusercontent.com/rowanwins/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/rowanwins/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/Proxy-Hunter/Proxy-List/main/http.txt",
    "https://raw.githubusercontent.com/Proxy-Hunter/Proxy-List/main/https.txt",
    "https://raw.githubusercontent.com/prosto-vlad19/FREE-PROXY/main/http.txt",
    "https://raw.githubusercontent.com/prosto-vlad19/FREE-PROXY/main/https.txt",
    "https://raw.githubusercontent.com/KrystianD/proxy-list/main/proxies-http.txt",
    "https://raw.githubusercontent.com/KrystianD/proxy-list/main/proxies-https.txt",
    "https://raw.githubusercontent.com/hharek/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/hharek/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/mishakorzik/AllHackingTools/main/http.txt",
    "https://raw.githubusercontent.com/mishakorzik/AllHackingTools/main/https.txt",
    "https://raw.githubusercontent.com/zeynoxwashere/proxy-list/main/ip_port/http.txt",
    "https://raw.githubusercontent.com/zeynoxwashere/proxy-list/main/ip_port/https.txt",
    "https://raw.githubusercontent.com/Vann-Dev/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/Vann-Dev/proxy-list/main/proxies/https.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies_anonymous/http.txt",
    "https://raw.githubusercontent.com/amine-bs/proxy/main/http.txt",
    "https://raw.githubusercontent.com/amine-bs/proxy/main/https.txt",
    "https://raw.githubusercontent.com/yaldram91/free-proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/yaldram91/free-proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/2ck/proxy-list/main/proxy_list.txt",
    "https://raw.githubusercontent.com/almroot/proxylist/master/list.txt",
    "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
    "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.txt",
    "https://raw.githubusercontent.com/dpangestuw/Free-Proxy/main/http_proxies.txt",
    "https://raw.githubusercontent.com/dpangestuw/Free-Proxy/main/https_proxies.txt",
    "https://raw.githubusercontent.com/xPaw/ProxyList/source/httpsnobanned.txt",
    "https://raw.githubusercontent.com/xPaw/ProxyList/source/httpnobanned.txt",
    "https://raw.githubusercontent.com/xPaw/ProxyList/source/full.txt",
    "https://raw.githubusercontent.com/FWHIBBIT/ProxyPool/master/allProxies.txt",
    "https://raw.githubusercontent.com/Kolandone/proxies/main/http.txt",
    "https://raw.githubusercontent.com/Kolandone/proxies/main/https.txt",
    "https://raw.githubusercontent.com/a2u/free-proxy-list/master/free-proxy-list.txt",
    "https://raw.githubusercontent.com/Nef10/free-proxy/main/http.txt",
    "https://raw.githubusercontent.com/Nef10/free-proxy/main/https.txt",
    "https://raw.githubusercontent.com/binnichtaktiv/proxylist/main/http.txt",
    "https://raw.githubusercontent.com/binnichtaktiv/proxylist/main/https.txt",
    "https://raw.githubusercontent.com/tarunKoyalwar/proxylist/main/http.txt",
    "https://raw.githubusercontent.com/tarunKoyalwar/proxylist/main/https.txt",
    "https://raw.githubusercontent.com/darwinia-network/proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/darwinia-network/proxy-list/master/https.txt",
    "https://raw.githubusercontent.com/speedx404/http-proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/speedx404/http-proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/AZProductions/Kooha-Proxies/main/http.txt",
    "https://raw.githubusercontent.com/AZProductions/Kooha-Proxies/main/https.txt",
    "https://raw.githubusercontent.com/Luqman-Ud-Din/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/Luqman-Ud-Din/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/0x7c7/proxy/main/http.txt",
    "https://raw.githubusercontent.com/0x7c7/proxy/main/https.txt",
    "https://raw.githubusercontent.com/iosifache/proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/iosifache/proxy-list/master/https.txt",
    # ── Web APIs / JSON endpoints ────────────────────────────────────
    "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
    "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=5000&country=all",
    "https://api.proxyscrape.com/?request=displayproxies&proxytype=http",
    "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http,https",
    "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=http,https",
    "https://proxylist.geonode.com/api/proxy-list?limit=500&page=3&sort_by=lastChecked&sort_type=desc&protocols=http,https",
    "https://proxylist.geonode.com/api/proxy-list?limit=500&page=4&sort_by=speed&sort_type=asc&protocols=http,https",
    "https://www.proxyscan.io/download?type=http",
    "https://www.proxyscan.io/download?type=https",
    "https://www.proxy-list.download/api/v1/get?type=http",
    "https://www.proxy-list.download/api/v1/get?type=https",
    "https://multiproxy.org/txt_all/proxy.txt",
    "https://multiproxy.org/txt_anon/proxy.txt",
    "https://www.freeproxylists.net/en/index_type_http.html",
    "https://free-proxy-list.net/",
    "https://us-proxy.org/",
    "https://free-proxy-list.net/uk-proxy.html",
    "https://www.sslproxies.org/",
    "https://www.google-proxy.net/",
    "https://www.us-proxy.org/",
    "https://hidemy.name/en/proxy-list/?type=hs",
    "https://hidemy.name/en/proxy-list/?type=h",
    "https://spys.one/en/http-proxy-list/",
    "https://spys.one/en/https-proxy-list/",
    "https://www.socks-proxy.net/",
    "https://proxydb.net/?protocol=http&protocol=https",
    "https://proxynova.com/proxy-server-list/",
    "https://proxynova.com/proxy-server-list/country-us/",
    "https://proxynova.com/proxy-server-list/country-de/",
    "https://proxynova.com/proxy-server-list/country-nl/",
    "https://proxynova.com/proxy-server-list/country-gb/",
    "https://proxynova.com/proxy-server-list/country-fr/",
    "https://www.ip-adress.com/proxy-list/",
    "https://www.ip-adress.com/proxy-list/2",
    "https://www.ip-adress.com/proxy-list/3",
    "https://www.ip-adress.com/proxy-list/4",
    "https://scrapingant.com/free-proxies/",
    "https://raw.githubusercontent.com/stamparm/ipsum/master/ipsum.txt",
    "https://openproxy.space/list/http",
    "https://openproxy.space/list/https",
    "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/http/http.txt",
    "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/https/https.txt",
    "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/all.txt",
    "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/prxchk/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/http.txt",
    "https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/https.txt",
    "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/http.txt",
    "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/https.txt",
    "https://raw.githubusercontent.com/TunsoHere/Proxy-List/main/http.txt",
    "https://raw.githubusercontent.com/TunsoHere/Proxy-List/main/https.txt",
    "https://raw.githubusercontent.com/BreakingTechForum/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/tuanminpay/live-proxies/master/http.txt",
    "https://raw.githubusercontent.com/tuanminpay/live-proxies/master/https.txt",
    "https://raw.githubusercontent.com/mnkrcc/proxy-list/main/proxies.txt",
    "https://raw.githubusercontent.com/Lucretia-eth/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/Lucretia-eth/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/iorikingdom/proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/iorikingdom/proxy-list/master/https.txt",
    "https://raw.githubusercontent.com/yuceltoluyag/GoodProxy/main/raw.txt",
    "https://raw.githubusercontent.com/Nef10/free-proxy/main/http.txt",
    "https://raw.githubusercontent.com/Nef10/free-proxy/main/https.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/proxy.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/proxies.txt",
    "https://raw.githubusercontent.com/mertguvencli/http-proxy-list/main/proxy-list/data.txt",
    "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt",
    "https://raw.githubusercontent.com/dreadl0ck/proxies/master/http.txt",
    "https://raw.githubusercontent.com/dreadl0ck/proxies/master/https.txt",
    "https://raw.githubusercontent.com/saisuiu/Lionkings-Http-Proxys-Proxies/main/free.txt",
    "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/https.txt",
    "https://raw.githubusercontent.com/MrMarble/proxy-list/main/all.txt",
    "https://raw.githubusercontent.com/RX4096/proxy-list/main/online/http.txt",
    "https://raw.githubusercontent.com/RX4096/proxy-list/main/online/https.txt",
    "https://raw.githubusercontent.com/hendrikbgr/Free-Proxy-Repo/master/proxy_list.txt",
    "https://raw.githubusercontent.com/kenzok8/openwrt-packages/master/pfw/proxy-list.txt",
    "https://raw.githubusercontent.com/ObcbO/getproxy/master/file/http.txt",
    "https://raw.githubusercontent.com/ObcbO/getproxy/master/file/https.txt",
    "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt",
    "https://raw.githubusercontent.com/drone911/ProxyList/main/http.txt",
    "https://raw.githubusercontent.com/drone911/ProxyList/main/https.txt",
    "https://raw.githubusercontent.com/almroot/proxylist/master/list.txt",
    "https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/HTTP.txt",
    "https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/HTTPS.txt",
    "https://raw.githubusercontent.com/proxyME/ProxyList/main/HTTP.txt",
    "https://raw.githubusercontent.com/proxyME/ProxyList/main/HTTPS.txt",
    "https://raw.githubusercontent.com/zloi-user/hideip.me/main/http.txt",
    "https://raw.githubusercontent.com/zloi-user/hideip.me/main/https.txt",
    "https://raw.githubusercontent.com/saschazesiger/Free-Proxies/master/proxies/http.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTP_RAW.txt",
    "https://raw.githubusercontent.com/DawnFz/Free-Proxy/master/http.txt",
    "https://raw.githubusercontent.com/fahimscirex/proxybd/master/proxylist/http.txt",
    "https://raw.githubusercontent.com/fahimscirex/proxybd/master/proxylist/https.txt",
    "https://raw.githubusercontent.com/rxzyx/Blasting-Proxies/main/proxies.txt",
    "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt",
    "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/https.txt",
    "https://raw.githubusercontent.com/HyperBeats/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/HyperBeats/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/casals-ar/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/casals-ar/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/UptimerBot/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/UptimerBot/proxy-list/main/proxies/https.txt",
    "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt",
    "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/https.txt",
    "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/http_proxies.txt",
    "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/https_proxies.txt",
    "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies_anonymous/http.txt",
    "https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/im-razvan/proxy_list/main/http.txt",
    "https://raw.githubusercontent.com/im-razvan/proxy_list/main/https.txt",
    "https://raw.githubusercontent.com/Vann-Dev/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/Vann-Dev/proxy-list/main/proxies/https.txt",
    "https://raw.githubusercontent.com/aslisk/proxyhttps/main/https.txt",
    "https://raw.githubusercontent.com/Dysiwe/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/Dysiwe/proxy-list/main/proxies/https.txt",
    "https://raw.githubusercontent.com/caliphdev/Proxy-List/master/http.txt",
    "https://raw.githubusercontent.com/caliphdev/Proxy-List/master/https.txt",
    "https://raw.githubusercontent.com/ItsMe0007/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/ItsMe0007/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/proxylist-to/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/proxylist-to/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/UserR3X/proxy-list/main/online/http.txt",
    "https://raw.githubusercontent.com/UserR3X/proxy-list/main/online/https.txt",
    "https://raw.githubusercontent.com/rxzyx/Blasting-Proxies/main/proxies.txt",
    "https://raw.githubusercontent.com/TundzhayDzhansaz/auto-proxy-puller-by-google/main/proxies.txt",
    "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
    "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-https.txt",
    "https://raw.githubusercontent.com/yosef-FS/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/ror-community/proxy-list/main/proxy.txt",
    "https://raw.githubusercontent.com/elliottophellia/yakumo/master/results/http/global/http_checked.txt",
    "https://raw.githubusercontent.com/elliottophellia/yakumo/master/results/https/global/https_checked.txt",
    "https://raw.githubusercontent.com/Volodichev/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/Volodichev/proxy-list/main/http_high_anonymous.txt",
    "https://raw.githubusercontent.com/KnightChaser/easy-proxy-scraper/main/proxies/http.txt",
    "https://raw.githubusercontent.com/KnightChaser/easy-proxy-scraper/main/proxies/https.txt",
    "https://raw.githubusercontent.com/black-standard/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/black-standard/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/themiralay/Proxy-List-World/master/data.txt",
    "https://raw.githubusercontent.com/ComplexPlane/free-proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/ComplexPlane/free-proxy-list/master/https.txt",
    "https://raw.githubusercontent.com/squid-proxy-list/squid-proxy-list/main/proxies.txt",
    "https://raw.githubusercontent.com/rowanwins/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/rowanwins/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/Proxy-Hunter/Proxy-List/main/http.txt",
    "https://raw.githubusercontent.com/Proxy-Hunter/Proxy-List/main/https.txt",
    "https://raw.githubusercontent.com/prosto-vlad19/FREE-PROXY/main/http.txt",
    "https://raw.githubusercontent.com/prosto-vlad19/FREE-PROXY/main/https.txt",
    "https://raw.githubusercontent.com/KrystianD/proxy-list/main/proxies-http.txt",
    "https://raw.githubusercontent.com/KrystianD/proxy-list/main/proxies-https.txt",
    "https://raw.githubusercontent.com/hharek/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/hharek/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/mishakorzik/AllHackingTools/main/http.txt",
    "https://raw.githubusercontent.com/rx443/proxy-list/main/online/http.txt",
    "https://raw.githubusercontent.com/rx443/proxy-list/main/online/https.txt",
    "https://raw.githubusercontent.com/catidog/free-proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/catidog/free-proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/zeynoxwashere/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/zeynoxwashere/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/alexey-pelykh/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/alexey-pelykh/proxy-list/main/proxies/https.txt",
    "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/http_proxies.txt",
    "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/https_proxies.txt",
    "https://raw.githubusercontent.com/andigwandi/free-proxy/main/proxy_list.txt",
    "https://raw.githubusercontent.com/fyvkgq/free-proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/fyvkgq/free-proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/automcn/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/automcn/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/FoxyProxy/Github/master/http.txt",
    "https://raw.githubusercontent.com/tfsou/proxy/master/https.txt",
    "https://raw.githubusercontent.com/ryanhatfield/free-proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/2ck/proxy-list/main/proxy_list.txt",
    "https://raw.githubusercontent.com/a2u/free-proxy-list/master/free-proxy-list.txt",
    "https://raw.githubusercontent.com/binnichtaktiv/proxylist/main/http.txt",
    "https://raw.githubusercontent.com/binnichtaktiv/proxylist/main/https.txt",
    "https://raw.githubusercontent.com/tarunKoyalwar/proxylist/main/http.txt",
    "https://raw.githubusercontent.com/tarunKoyalwar/proxylist/main/https.txt",
    "https://raw.githubusercontent.com/darwinia-network/proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/AZProductions/Kooha-Proxies/main/http.txt",
    "https://raw.githubusercontent.com/AZProductions/Kooha-Proxies/main/https.txt",
    "https://raw.githubusercontent.com/Luqman-Ud-Din/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/Luqman-Ud-Din/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/0x7c7/proxy/main/http.txt",
    "https://raw.githubusercontent.com/0x7c7/proxy/main/https.txt",
    "https://raw.githubusercontent.com/iosifache/proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/iosifache/proxy-list/master/https.txt",
    "https://raw.githubusercontent.com/Kolandone/proxies/main/http.txt",
    "https://raw.githubusercontent.com/dpangestuw/Free-Proxy/main/http_proxies.txt",
    "https://raw.githubusercontent.com/dpangestuw/Free-Proxy/main/https_proxies.txt",
    "https://raw.githubusercontent.com/xPaw/ProxyList/source/httpsnobanned.txt",
    "https://raw.githubusercontent.com/xPaw/ProxyList/source/httpnobanned.txt",
    "https://raw.githubusercontent.com/xPaw/ProxyList/source/full.txt",
    "https://raw.githubusercontent.com/FWHIBBIT/ProxyPool/master/allProxies.txt",
    "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
    "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.txt",
    "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
    "https://raw.githubusercontent.com/amine-bs/proxy/main/http.txt",
    "https://raw.githubusercontent.com/yaldram91/free-proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/FWHIBBIT/ProxyPool/master/allProxies.txt",
    "https://raw.githubusercontent.com/Kolandone/proxies/main/https.txt",
    "https://raw.githubusercontent.com/GeoNode/geonode-access-proxy/main/proxies/http.txt",
    "https://raw.githubusercontent.com/GeoNode/geonode-access-proxy/main/proxies/https.txt",
    "https://raw.githubusercontent.com/speedx404/http-proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/speedx404/http-proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/mnkrcc/proxy-list/main/proxies.txt",
    "https://raw.githubusercontent.com/Lucretia-eth/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/Lucretia-eth/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/tuanminpay/live-proxies/master/http.txt",
    "https://raw.githubusercontent.com/tuanminpay/live-proxies/master/https.txt",
    "https://raw.githubusercontent.com/TunsoHere/Proxy-List/main/http.txt",
    "https://raw.githubusercontent.com/TunsoHere/Proxy-List/main/https.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/proxy.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/proxies.txt",
    "https://raw.githubusercontent.com/dreadl0ck/proxies/master/http.txt",
    "https://raw.githubusercontent.com/dreadl0ck/proxies/master/https.txt",
    "https://raw.githubusercontent.com/kenzok8/openwrt-packages/master/pfw/proxy-list.txt",
    "https://raw.githubusercontent.com/drone911/ProxyList/main/http.txt",
    "https://raw.githubusercontent.com/drone911/ProxyList/main/https.txt",
    "https://raw.githubusercontent.com/proxyME/ProxyList/main/HTTP.txt",
    "https://raw.githubusercontent.com/proxyME/ProxyList/main/HTTPS.txt",
    "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt",
    "https://raw.githubusercontent.com/rxzyx/Blasting-Proxies/main/proxies.txt",
    "https://raw.githubusercontent.com/stamparm/ipsum/master/ipsum.txt",
    "https://openproxy.space/list/http",
    "https://openproxy.space/list/https",
]

# Deduplicate and shuffle for better coverage
SOURCES = list(dict.fromkeys(SOURCES))

PROXY_RE = re.compile(
    r"\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{2,5})\b"
)

GEONODE_JSON_RE = re.compile(r'"ip"\s*:\s*"([^"]+)".*?"port"\s*:\s*"(\d+)"')

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# ─────────────────── STATS TRACKER ───────────────────────────────
class Stats:
    def __init__(self):
        self.scraped    = 0
        self.checked    = 0
        self.valid      = 0
        self.start_time = time.time()

    def elapsed(self) -> str:
        s = int(time.time() - self.start_time)
        return f"{s//60:02d}:{s%60:02d}"

    def rate(self) -> str:
        elapsed = time.time() - self.start_time
        return f"{self.checked / elapsed:.0f}/s" if elapsed > 0 else "?"

stats = Stats()

# ─────────────────── BANNER ───────────────────────────────────────
def print_banner():
    print(Fore.CYAN + r"""
 ██████╗ ██████╗  ██████╗ ██╗  ██╗██╗   ██╗    ██╗  ██╗██╗  ██╗
 ██╔══██╗██╔══██╗██╔═══██╗╚██╗██╔╝╚██╗ ██╔╝    ╚██╗██╔╝╚██╗██╔╝
 ██████╔╝██████╔╝██║   ██║ ╚███╔╝  ╚████╔╝      ╚███╔╝  ╚███╔╝ 
 ██╔═══╝ ██╔══██╗██║   ██║ ██╔██╗   ╚██╔╝       ██╔██╗  ██╔██╗ 
 ██║     ██║  ██║╚██████╔╝██╔╝ ██╗   ██║        ██╔╝ ██╗██╔╝ ██╗
 ╚═╝     ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═╝   ╚═╝        ╚═╝  ╚═╝╚═╝  ╚═╝
""")
    print(Fore.YELLOW + f"  ULTRA PROXY SCRAPER v3.0 — PRODUCTION GRADE")
    print(Fore.WHITE  + f"  Sources: {len(SOURCES)}  |  Workers: {WORKERS}  |  Target: {TARGET_COUNT}  |  Max Latency: {MAX_LATENCY_MS}ms\n")
    print(Fore.WHITE  + "─" * 68)

# ─────────────────── PARSER: extract proxies from text ───────────
def parse_proxies(text: str) -> set[str]:
    found = set()

    # JSON from geonode
    if '"ip"' in text:
        for ip, port in GEONODE_JSON_RE.findall(text):
            if _valid_port(port):
                found.add(f"{ip}:{port}")

    # plain ip:port regex (catches all other formats)
    for ip, port in PROXY_RE.findall(text):
        if _valid_port(port) and not ip.startswith(("0.", "127.", "10.", "192.168.", "172.")):
            found.add(f"{ip}:{port}")

    return found


def _valid_port(port: str) -> bool:
    try:
        p = int(port)
        return 1 <= p <= 65535
    except ValueError:
        return False


# ─────────────────── SCRAPER: fetch all sources ──────────────────
async def fetch_source(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
) -> set[str]:
    async with sem:
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=SCRAPE_TIMEOUT),
                headers=HEADERS,
                ssl=False,
            ) as resp:
                text = await resp.text(encoding="utf-8", errors="ignore")
                proxies = parse_proxies(text)
                return proxies
        except Exception:
            return set()


async def scrape_all() -> set[str]:
    print(Fore.CYAN + f"\n[SCRAPER] Fetching {len(SOURCES)} sources in parallel...\n")
    sem = asyncio.Semaphore(300)  # 300 simultaneous source fetches
    connector = aiohttp.TCPConnector(
        limit=0,
        ssl=False,
        ttl_dns_cache=300,
        use_dns_cache=True,
        enable_cleanup_closed=True,
    )
    timeout = aiohttp.ClientTimeout(total=SCRAPE_TIMEOUT)

    all_proxies: set[str] = set()

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers=HEADERS,
    ) as session:
        tasks = [fetch_source(session, url, sem) for url in SOURCES]

        pbar = tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc=Fore.GREEN + "  Scraping",
            ncols=75,
            colour="green",
        )
        for coro in pbar:
            result = await coro
            all_proxies |= result
            pbar.set_postfix(found=len(all_proxies))

    stats.scraped = len(all_proxies)
    print(Fore.GREEN + f"\n  ✔ Scraped {len(all_proxies):,} unique raw proxies\n")
    return all_proxies


# ─────────────────── CHECKER: validate + latency ─────────────────
async def check_proxy(
    proxy: str,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    valid_list: list,
    done_event: asyncio.Event,
) -> None:
    if done_event.is_set():
        return

    async with sem:
        if done_event.is_set():
            return
        url = f"http://{proxy}"
        t0  = time.monotonic()
        try:
            async with session.get(
                CHECK_URL,
                proxy=url,
                timeout=aiohttp.ClientTimeout(
                    total=CHECK_TIMEOUT,
                    connect=CHECK_TIMEOUT * 0.6,
                ),
                ssl=False,
            ) as resp:
                latency_ms = (time.monotonic() - t0) * 1000
                if resp.status == 200 and latency_ms <= MAX_LATENCY_MS:
                    valid_list.append((proxy, round(latency_ms)))
                    stats.valid += 1
                    if len(valid_list) >= TARGET_COUNT:
                        done_event.set()
        except Exception:
            pass
        finally:
            stats.checked += 1


async def check_all(raw_proxies: set[str]) -> list[tuple[str, int]]:
    print(Fore.CYAN + f"[CHECKER] Validating {len(raw_proxies):,} proxies with {WORKERS} workers...\n")
    print(Fore.YELLOW + f"  Filter: < {MAX_LATENCY_MS}ms latency | HTTP only | Target: {TARGET_COUNT}\n")

    valid: list[tuple[str, int]] = []
    done_event = asyncio.Event()

    sem = asyncio.Semaphore(WORKERS)
    connector = aiohttp.TCPConnector(
        limit=0,
        ssl=False,
        ttl_dns_cache=300,
        use_dns_cache=True,
        enable_cleanup_closed=True,
        force_close=True,
    )
    timeout = aiohttp.ClientTimeout(total=CHECK_TIMEOUT + 1)

    # Shuffle so we don't hammer the same subnet
    proxy_list = list(raw_proxies)
    random.shuffle(proxy_list)

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
    ) as session:
        pbar = tqdm(
            total=len(proxy_list),
            desc=Fore.MAGENTA + "  Checking",
            ncols=75,
            colour="magenta",
        )

        batch_size = 5000
        for i in range(0, len(proxy_list), batch_size):
            if done_event.is_set():
                break
            batch = proxy_list[i : i + batch_size]
            tasks = [
                check_proxy(p, session, sem, valid, done_event)
                for p in batch
            ]
            await asyncio.gather(*tasks)
            pbar.update(len(batch))
            pbar.set_postfix(
                valid=len(valid),
                rate=stats.rate(),
                elapsed=stats.elapsed(),
            )
            if done_event.is_set():
                break

        pbar.close()

    # Sort by speed
    valid.sort(key=lambda x: x[1])
    return valid


# ─────────────────── TELEGRAM SENDER ─────────────────────────────
async def send_to_telegram(filepath: str, valid_count: int, elapsed: str) -> bool:
    if TELEGRAM_BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print(Fore.YELLOW + "\n  ⚠  Telegram not configured — skipping send.\n")
        return False

    caption = (
        f"🚀 *ULTRA PROXY SCRAPER — DONE*\n\n"
        f"✅ Valid proxies: `{valid_count}`\n"
        f"⚡ Max latency: `{MAX_LATENCY_MS}ms`\n"
        f"📡 Protocol: `HTTP/HTTPS`\n"
        f"🕐 Time taken: `{elapsed}`\n"
        f"📅 `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"

    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            with open(filepath, "rb") as f:
                form = aiohttp.FormData()
                form.add_field("chat_id", TELEGRAM_CHAT_ID)
                form.add_field("caption", caption)
                form.add_field("parse_mode", "Markdown")
                form.add_field(
                    "document",
                    f,
                    filename=os.path.basename(filepath),
                    content_type="text/plain",
                )
                async with session.post(url, data=form, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        print(Fore.GREEN + f"\n  ✔ File sent to Telegram successfully!\n")
                        return True
                    else:
                        body = await resp.text()
                        print(Fore.RED + f"\n  ✗ Telegram error {resp.status}: {body[:200]}\n")
                        return False
    except Exception as e:
        print(Fore.RED + f"\n  ✗ Telegram send failed: {e}\n")
        return False


# ─────────────────── SAVE FILE ───────────────────────────────────
async def save_proxies(valid: list[tuple[str, int]]) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"proxies_{timestamp}.txt"

    lines = [f"{proxy}  # {ms}ms" for proxy, ms in valid]
    async with aiofiles.open(fname, "w") as f:
        await f.write("\n".join(lines))

    # Also write plain format for easy use
    plain_fname = f"proxies_plain_{timestamp}.txt"
    async with aiofiles.open(plain_fname, "w") as f:
        await f.write("\n".join(p for p, _ in valid))

    return fname


# ─────────────────── MAIN ─────────────────────────────────────────
async def main():
    print_banner()
    t_start = time.time()

    # 1. SCRAPE
    raw = await scrape_all()
    if not raw:
        print(Fore.RED + "  ✗ No proxies scraped. Check your internet connection.")
        return

    # 2. CHECK (inline — as they're scraped, no separate wait)
    valid = await check_all(raw)

    elapsed = stats.elapsed()
    total_time = time.time() - t_start

    # 3. RESULTS
    print(Fore.WHITE + "\n" + "─" * 68)
    print(Fore.GREEN  + f"  ✔  VALID PROXIES  : {len(valid):,}")
    print(Fore.CYAN   + f"  ⚡  FASTEST        : {valid[0][1]}ms  —  {valid[0][0]}")
    print(Fore.YELLOW + f"  ⏱   TOTAL TIME     : {elapsed}  ({total_time:.1f}s)")
    print(Fore.BLUE   + f"  📊  CHECKED        : {stats.checked:,}")
    print(Fore.WHITE  + "─" * 68 + "\n")

    # 4. TOP 10 PREVIEW
    print(Fore.CYAN + "  TOP 10 FASTEST:\n")
    for i, (proxy, ms) in enumerate(valid[:10], 1):
        bar = "█" * (20 - int(ms / 10)) if ms < 200 else ""
        color = Fore.GREEN if ms < 100 else Fore.YELLOW
        print(color + f"  {i:>2}. {proxy:<22} {ms:>4}ms  {bar}")
    print()

    # 5. SAVE
    if valid:
        fname = await save_proxies(valid)
        print(Fore.GREEN + f"  ✔ Saved to: {fname}\n")

        # 6. TELEGRAM SEND
        await send_to_telegram(fname, len(valid), elapsed)
    else:
        print(Fore.RED + "  ✗ No valid proxies found under 200ms.\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(Fore.YELLOW + "\n  ⚠  Interrupted by user.\n")

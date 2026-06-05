import os
import json
import ipaddress
import urllib.request
from log import log_exception, log_activity

# Global MaxMind database readers
_country_reader = None
_asn_reader = None
_readers_initialized = False

def init_readers():
    global _country_reader, _asn_reader, _readers_initialized
    if _readers_initialized:
        return True

    try:
        import maxminddb
        base_dir = os.path.dirname(os.path.abspath(__file__))
        geoip_dir = os.path.join(base_dir, "geoip")
        country_path = os.path.join(geoip_dir, "GeoLite2-Country.mmdb")
        asn_path = os.path.join(geoip_dir, "GeoLite2-ASN.mmdb")

        if os.path.exists(country_path):
            _country_reader = maxminddb.open_database(country_path)
            log_activity(f"Loaded MaxMind Country DB: {country_path}")
        if os.path.exists(asn_path):
            _asn_reader = maxminddb.open_database(asn_path)
            log_activity(f"Loaded MaxMind ASN DB: {asn_path}")
    except Exception:
        log_exception("Failed to load local MaxMind database files")

    _readers_initialized = True
    return _country_reader is not None or _asn_reader is not None

def has_offline_readers() -> bool:
    init_readers()
    return _country_reader is not None or _asn_reader is not None

def _is_private_ip(ip: str) -> bool:
    try:
        ip_obj = ipaddress.ip_address(ip.strip())
        return ip_obj.is_private or ip_obj.is_loopback
    except ValueError:
        return False

def clean_isp_name(isp: str | None) -> str:
    if not isp:
        return "Unknown"
    import re
    # Strip prefixes like "AS12345 ", "AS12345 - ", etc.
    cleaned = re.sub(r'(?i)^as\s*\d+\s*[-–]?\s*', '', isp).strip()
    if not cleaned:
        return isp.strip()
    return cleaned

def lookup_offline(ip: str) -> dict | None:
    init_readers()
    if not _country_reader and not _asn_reader:
        return None

    result = {}
    try:
        if _country_reader:
            country_data = _country_reader.get(ip)
            if country_data:
                country = country_data.get("country", {})
                result["country_code"] = country.get("iso_code")
                
                # Get english name
                names = country.get("names", {})
                result["country_name"] = names.get("en") or names.get("zh-CN") or names.get("ja") or "Unknown"

        if _asn_reader:
            asn_data = _asn_reader.get(ip)
            if asn_data:
                result["isp"] = clean_isp_name(asn_data.get("autonomous_system_organization"))

        if "country_code" in result or "isp" in result:
            return {
                "country_code": result.get("country_code"),
                "country_name": result.get("country_name") or "Unknown",
                "isp": result.get("isp") or "Unknown",
                "source": "offline",
            }
    except Exception:
        log_exception(f"Offline lookup failed for IP: {ip}")
    return None

def _fetch_with_retry(ip: str) -> dict:
    import urllib.request
    import json
    import time

    last_exc = None
    # 1. Try ipwhois.app (HTTPS)
    for attempt in range(3):
        try:
            url = f"https://ipwhois.app/json/{ip}"
            req = urllib.request.Request(url, headers={'User-Agent': 'Nginx-Monitor/1.0'})
            with urllib.request.urlopen(req, timeout=5) as response:
                res_data = json.loads(response.read().decode('utf-8'))
                if res_data.get("success") is True:
                    return {
                        "country_code": res_data.get("country_code"),
                        "country_name": res_data.get("country"),
                        "isp": clean_isp_name(res_data.get("isp") or res_data.get("org")),
                        "source": "online",
                    }
                else:
                    return {
                        "country_code": "unknown",
                        "country_name": "Unknown",
                        "isp": "Unknown",
                        "source": "online",
                    }
        except Exception as e:
            last_exc = e
            time.sleep(0.5 * (attempt + 1))

    # 2. Fallback to ip-api.com (HTTP)
    for attempt in range(3):
        try:
            url = f"http://ip-api.com/json/{ip}"
            req = urllib.request.Request(url, headers={'User-Agent': 'Nginx-Monitor/1.0'})
            with urllib.request.urlopen(req, timeout=5) as response:
                res_data = json.loads(response.read().decode('utf-8'))
                if res_data.get("status") == "success":
                    return {
                        "country_code": res_data.get("countryCode"),
                        "country_name": res_data.get("country"),
                        "isp": clean_isp_name(res_data.get("isp") or res_data.get("org")),
                        "source": "online",
                    }
                else:
                    return {
                        "country_code": "unknown",
                        "country_name": "Unknown",
                        "isp": "Unknown",
                        "source": "online",
                    }
        except Exception as e:
            last_exc = e
            time.sleep(0.5 * (attempt + 1))

    raise last_exc or Exception("All GeoIP APIs failed")

async def resolve_ip(ip: str) -> dict:
    # 1. Check if Private/LAN IP
    if _is_private_ip(ip):
        return {
            "country_code": "local",
            "country_name": "Local / LAN",
            "isp": "Local Network",
            "source": "local",
        }

    # 2. Try Offline lookup
    offline_res = lookup_offline(ip)
    if offline_res:
        return offline_res

    # 3. Fallback to Free Online APIs with Retry
    import asyncio
    try:
        return await asyncio.to_thread(_fetch_with_retry, ip.strip())
    except Exception:
        log_exception(f"Online GeoIP lookup failed for IP: {ip}")
        return {
            "country_code": "unknown",
            "country_name": "Unknown",
            "isp": "Unknown",
            "source": "failed",
        }

def close_readers():
    global _country_reader, _asn_reader, _readers_initialized
    if _country_reader:
        try:
            _country_reader.close()
        except Exception:
            pass
        _country_reader = None
    if _asn_reader:
        try:
            _asn_reader.close()
        except Exception:
            pass
        _asn_reader = None
    _readers_initialized = False

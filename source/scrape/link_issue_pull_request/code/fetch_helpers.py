import time
from pathlib import Path

PROXY_FILE = Path("source/lib/proxies.txt")


def ReadProxyFromFile(proxy_file, proxy_num):
    if not proxy_file.exists():
        return None
    try:
        with proxy_file.open("r", encoding="utf-8") as fh:
            lines = [ln.strip() for ln in fh.readlines() if ln.strip()]
    except Exception:
        return None
    index = proxy_num - 1
    if proxy_num == 0 or proxy_num - 1 < 0 or proxy_num - 1 >= len(lines):
        return None
    chosen = lines[index]
    parts = chosen.split(":")

    host = parts[0]
    port = parts[1]
    username = parts[2]
    password = ":".join(parts[3:])
    return f"http://{username}:{password}@{host}:{port}"


def FetchGitHubPage(sesh, url, proxy_num=0):
    proxy_url = ReadProxyFromFile(PROXY_FILE, proxy_num)
    if proxy_url:
        sesh.proxies = {"http": proxy_url, "https": proxy_url}
    else:
        sesh.proxies = {}

    resp = sesh.get(url, allow_redirects=True)
    if resp.status_code == 404:
        time.sleep(5)
        resp = sesh.get(url, allow_redirects=True)

    text = resp.text if hasattr(resp, "text") else ""
    is_not_found = (
        (resp.status_code == 404) or
        ("This is not the webpage you are looking for" in text) or
        ("Not Found" in text)
    )
    is_rate_limited = (
        ("Please wait a few minutes before you try again" in text) or
        ("You Are Not Connected" in text)
    )
    return resp, is_not_found, is_rate_limited

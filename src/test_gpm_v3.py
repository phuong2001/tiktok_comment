import os, requests, json

GPM_URL = os.getenv("GPM_URL", "http://127.0.0.1:19995")  # đúng theo Node cũ của bạn
GPM_TOKEN = os.getenv("GPM_TOKEN")  # nếu có token, set env trước

def try_get(url, headers=None):
    try:
        r = requests.get(url, headers=headers or {}, timeout=10)
        ctype = r.headers.get("content-type", "")
        body = r.text or ""
        print(f"\nGET {url}")
        print("status:", r.status_code, "| ctype:", ctype)
        print("body[:200]:", body[:200].replace("\n", " "))
        if "application/json" in (ctype or ""):
            print("json:", json.dumps(r.json(), ensure_ascii=False)[:200])
        return r
    except Exception as e:
        print("EXC:", e)

def main():
    base = GPM_URL.rstrip("/") + "/api/v3"
    print("GPM_URL =", GPM_URL)
    print("Has token =", bool(GPM_TOKEN))

    # 1) Không header
    try_get(base)

    # 2) Authorization: Bearer
    if GPM_TOKEN:
        try_get(base, {"Accept":"application/json","Authorization":f"Bearer {GPM_TOKEN}"})

    # 3) X-Auth-Token
    if GPM_TOKEN:
        try_get(base, {"Accept":"application/json","X-Auth-Token":GPM_TOKEN})

    # 4) X-GPM-Token
    if GPM_TOKEN:
        try_get(base, {"Accept":"application/json","X-GPM-Token":GPM_TOKEN})

    # 5) thử /profiles
    try_get(base + "/profiles", {"Accept":"application/json", **({"Authorization":f"Bearer {GPM_TOKEN}"} if GPM_TOKEN else {})})

if __name__ == "__main__":
    main()

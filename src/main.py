import asyncio
import json
import re
import requests
from typing import Optional, Tuple
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ====== CẤU HÌNH GPM ======
PROFILE_ID = "47a1c920-1b08-4065-9a79-3f4202c99e9c"
GPM_API_URL = "http://127.0.0.1:19995"  # URL daemon GPM của bạn

# ====== GPM API HELPERS ======
def start_gpm_profile(api_base: str, profile_id: str) -> Tuple[str, str]:
    """
    Start profile qua GPM API và trả về (cdp_url, run_id).
    Tuỳ phiên bản GPM, response có thể:
      { "wsEndpoint": "ws://127.0.0.1:9222/devtools/browser/..." }
      hoặc { "ws": "ws://127.0.0.1:9222" } hoặc { "debuggingAddress": "127.0.0.1", "port": 9222 }
    """
    # Một số bản GPM có route khác; dùng /v1.0/browser/start là phổ biến
    url = f"{api_base.rstrip('/')}/v1.0/browser/start"
    resp = requests.post(url, json={"profile_id": profile_id}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    # GPM thường trả run_id để stop
    run_id = data.get("run_id") or data.get("uuid") or ""

    # Lấy endpoint CDP
    cdp = (
        data.get("wsEndpoint")
        or data.get("ws")
        or data.get("url")
        or data.get("cdpUrl")
    )
    if not cdp:
        addr = data.get("debuggingAddress") or "127.0.0.1"
        port = data.get("port") or data.get("debuggingPort")
        if port:
            cdp = f"http://{addr}:{port}"
    if not cdp:
        raise RuntimeError(f"Không tìm thấy CDP endpoint trong response: {data}")
    return cdp, run_id

def stop_gpm_profile(api_base: str, profile_id: str, run_id: Optional[str] = None) -> None:
    """
    Stop profile qua GPM API. Tuỳ bản GPM có thể cần run_id hoặc chỉ cần profile_id.
    """
    # Thử stop bằng run_id nếu có
    try:
        if run_id:
            url = f"{api_base.rstrip('/')}/v1.0/browser/stop"
            r = requests.post(url, json={"profile_id": profile_id, "run_id": run_id}, timeout=30)
            if r.ok:
                return
    except Exception:
        pass
    # Fallback: stop chỉ với profile_id
    try:
        url = f"{api_base.rstrip('/')}/v1.0/browser/stop"
        requests.post(url, json={"profile_id": profile_id}, timeout=30)
    except Exception:
        pass

# ====== PLAYWRIGHT FLOW ======
async def comment_with_gpm_profile(video_url: str, text: str,
                                   api_url: str = GPM_API_URL, profile_id: str = PROFILE_ID,
                                   wait_manual_login_sec: int = 60):
    # 1) Start profile → lấy CDP endpoint
    cdp_url, run_id = start_gpm_profile(api_url, profile_id)
    print("CDP endpoint:", cdp_url)

    async with async_playwright() as p:
        # 2) Kết nối vào browser của profile GPM
        browser = await p.chromium.connect_over_cdp(cdp_url)

        # Dùng persistent context sẵn có
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = context.pages[0] if context.pages else await context.new_page()

        # 3) Kiểm tra đã login hay chưa (cookie sessionid)
        cookies = await context.cookies(["https://www.tiktok.com"])
        logged_in = any(c["name"] in ("sessionid", "sessionid_ss") for c in cookies)
        print("Logged in:", logged_in)

        if not logged_in:
            # Mở trang login cho bạn đăng nhập tay (chỉ lần đầu của profile)
            print(f"⚠️ Chưa login. Mở trang login và chờ bạn đăng nhập tay (tối đa {wait_manual_login_sec}s)…")
            await page.goto("https://www.tiktok.com/login", timeout=60000)
            for _ in range(wait_manual_login_sec):
                await page.wait_for_timeout(1000)
                cookies = await context.cookies(["https://www.tiktok.com"])
                if any(c["name"] in ("sessionid", "sessionid_ss") for c in cookies):
                    logged_in = True
                    break
            print("Logged in (after manual)?", logged_in)
            if not logged_in:
                print("❌ Không thấy sessionid. Dừng.")
                await browser.close()
                stop_gpm_profile(api_url, profile_id, run_id)
                return

        # 4) Mở video
        await page.goto(video_url, wait_until="domcontentloaded", timeout=60000)

        # 5) Mở khung comment (nếu cần)
        for sel in ("[data-e2e='browse-comment']", "button:has-text('Comments')"):
            try:
                await page.click(sel, timeout=1500)
                break
            except PWTimeout:
                pass

        # 6) Tìm ô nhập comment (contenteditable)
        input_selectors = (
            "[data-e2e='comment-input']",
            "div[contenteditable='true'][data-e2e='comment-input']",
            "div[contenteditable='true'][data-text='true']",
            "div[contenteditable='true']",
            "div[role='textbox']",
        )
        box = None
        for sel in input_selectors:
            try:
                await page.wait_for_selector(sel, timeout=6000)
                box = await page.query_selector(sel)
                if box:
                    break
            except PWTimeout:
                continue
        if not box:
            raise RuntimeError("Không tìm thấy ô nhập bình luận (cần cập nhật selector).")

        await box.click()
        await page.keyboard.type(text)

        # 7) Gửi bình luận
        sent = False
        for sel in (
            "[data-e2e='comment-post']",
            "button:has-text('Post')",
            "button[type='submit']",
            "button[aria-label*='Send']",
        ):
            try:
                await page.click(sel, timeout=2000)
                sent = True
                break
            except PWTimeout:
                continue
        if not sent:
            await page.keyboard.press("Enter")

        # 8) Bắt response POST /api/comment/publish
        def is_publish(resp):
            return resp.request.method == "POST" and "/api/comment/publish" in resp.url

        try:
            resp = await page.wait_for_response(is_publish, timeout=15000)
            raw = await resp.text()
            ctype = resp.headers.get("content-type", "")
            print("=== PUBLISH RESPONSE ===")
            print("Status:", resp.status, "|", ctype)
            if "application/json" in ctype and raw:
                try:
                    print(json.dumps(json.loads(raw), ensure_ascii=False, indent=2))
                except Exception:
                    print(raw)
            else:
                print(raw if raw else "(empty body)")
        except PWTimeout:
            print("⚠️ Không bắt được /api/comment/publish (15s). Có thể UI/selector khác hoặc bị chặn.)")

        # 9) (tuỳ chọn) đóng kết nối & stop profile
        await browser.close()
        # Nếu bạn muốn giữ profile mở để dùng tiếp, hãy COMMENT dòng dưới:
        # stop_gpm_profile(api_url, profile_id, run_id)


# Cách dùng (chạy trong file .py):
if __name__ == "__main__":
    asyncio.run(comment_with_gpm_profile(
        video_url="https://www.tiktok.com/@hnhtmlinh.963133/video/7544200418780351752",
        text="video oke quá"
    ))

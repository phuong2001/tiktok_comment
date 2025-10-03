# main.py
# -*- coding: utf-8 -*-
"""
GPM v3 -> Playwright CDP -> Comment TikTok (single-file)
- Python 3.8+
- Matches Node flow you shared: /api/v3/profiles/start/{id} -> remote_debugging_address
  -> GET /json/version -> webSocketDebuggerUrl -> connect_over_cdp

Setup:
  pip install requests playwright
  python -m playwright install chromium

Env:
  GPM_URL   (default http://127.0.0.1:19995)
  GPM_TOKEN (if your GPM requires token)

Run:
  python main.py --profile <profile_id> --video <tiktok_video_url> --comment "<text>" --max-retries 2
"""

import os
import sys
import json
import time
import random
import argparse
import asyncio
from typing import Optional, Dict, Any, Tuple

import requests
from playwright.async_api import async_playwright, TimeoutError as PWTimeout, Page, Browser

# ----------------------- Config -----------------------
GPM_URL = os.getenv("GPM_URL", "http://127.0.0.1:19995")
GPM_TOKEN = os.getenv("GPM_TOKEN")
DEFAULT_WAIT_MS = 30000

# ----------------------- SDK (v3) -----------------------
class GPMLoginSDKPy:
    def __init__(self, url: str, token: Optional[str] = None):
        base = url.rstrip("/")
        self.api_url = base + "/api/v3"
        self.token = token or os.getenv("GPM_TOKEN")

    def _headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def check_connection(self) -> Dict[str, Any]:
        try:
            r = requests.get(self.api_url, headers=self._headers(), timeout=10)
            r.raise_for_status()
            return {"success": True, "message": "Connection OK", "data": r.json()}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def start_profile(self, profile_id: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.api_url}/profiles/start/{profile_id}"
        r = requests.post(url, params=params or {}, headers=self._headers(), timeout=60)
        body = r.text or ""
        # If returns login page or 401/403, user must enable Local API or supply token
        if "GPM-Login" in body or r.status_code in (401, 403):
            raise RuntimeError(
                "GPM returned login HTML or auth error. Open GPM app, sign in, enable Local API, "
                "and set GPM_TOKEN if required. Also verify GPM_URL points to the API port."
            )
        r.raise_for_status()
        try:
            return r.json()
        except Exception:
            raise RuntimeError(f"Failed to parse JSON from v3 start response: {body[:400]}")

    def get_remote_debugging_browser(self, remote_debugging_address: str) -> Dict[str, Any]:
        addr = remote_debugging_address
        if not addr.startswith(("http://", "https://")):
            addr = "http://" + addr
        v = requests.get(addr.rstrip("/") + "/json/version", timeout=10)
        v.raise_for_status()
        return v.json()

    def close_profile(self, profile_id: str) -> Dict[str, Any]:
        url = f"{self.api_url}/profiles/close/{profile_id}"
        r = requests.post(url, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json()

# ------------------- GPM connect helpers -------------------
async def connect_gpm_v3_and_get_browser(profile_id: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Any, Browser]:
    sdk = GPMLoginSDKPy(GPM_URL, GPM_TOKEN)

    # Gọi thẳng /api/v3/profiles/start/{id} (giống Node)
    data = sdk.start_profile(profile_id, params=params)
    core = data.get("data") if isinstance(data.get("data"), dict) else data

    # Lấy remote_debugging_address rồi gọi /json/version -> webSocketDebuggerUrl
    rda = core.get("remote_debugging_address") or core.get("debugging_address")
    if not rda:
        raise RuntimeError(f"Không thấy remote_debugging_address trong response: {data}")

    ver = sdk.get_remote_debugging_browser(rda)
    ws = ver.get("webSocketDebuggerUrl")
    if not ws:
        raise RuntimeError(f"Không có webSocketDebuggerUrl ở {rda}/json/version: {ver}")

    pw = await async_playwright().start()
    browser = await pw.chromium.connect_over_cdp(ws)
    return pw, browser

# ------------------- TikTok helpers -------------------
TT_BASE = "https://www.tiktok.com/"

async def ensure_logged_in(page: Page) -> bool:
    try:
        await page.wait_for_selector('[data-e2e="inbox-icon"]', timeout=4000)
        return True
    except PWTimeout:
        pass
    try:
        await page.wait_for_selector(
            '[data-e2e="top-login-button"], a[href*="/login"], button:has-text("Log in")',
            timeout=4000
        )
        return False
    except PWTimeout:
        return True

async def open_video(page: Page, video_url: str) -> None:
    await page.goto(video_url, wait_until="domcontentloaded", timeout=60000)
    for sel in (
        "button:has-text('Accept')",
        "button:has-text('Accept all')",
        "button[aria-label*='Close']",
        "button[aria-label*='Đóng']",
        "button:has-text('Open app')",
    ):
        try:
            await page.click(sel, timeout=800)
        except Exception:
            pass
    try:
        await page.click('[data-e2e="comment-icon"]', timeout=1500)
    except Exception:
        pass

async def type_comment(page: Page, text: str) -> None:
    selectors = (
        '[data-e2e="comment-text"]',
        '[data-e2e="comment-input"]',
        'div[contenteditable="true"][data-e2e*="comment"]',
        'div[role="textbox"][contenteditable="true"]',
        'div[contenteditable="true"]',
        'textarea',
    )
    # đảm bảo đóng overlay trước khi tương tác
    await dismiss_overlays(page)

    box = None
    for sel in selectors:
        loc = page.locator(sel)
        try:
            await loc.first.wait_for(timeout=6000)
            box = loc.first
            break
        except Exception:
            continue

    if not box:
        raise RuntimeError("Không tìm thấy ô nhập bình luận (cần cập nhật selector).")

    # dùng force để tránh intercept
    await box.click(force=True)
    try:
        await page.keyboard.press("Control+A")
        await page.keyboard.press("Backspace")
    except Exception:
        pass

    for ch in text:
        await page.keyboard.type(ch, delay=random.randint(20, 90))


async def submit_comment(page: Page) -> None:
    await dismiss_overlays(page)
    for sel in (
        '[data-e2e="post-comment"]',
        '[data-e2e="comment-post"]',
        'button:has-text("Post")',
        'button:has-text("Gửi")',
        'button[aria-label*="Post"]',
    ):
        try:
            loc = page.locator(sel).first
            await loc.wait_for(timeout=1500)
            await loc.click(force=True)
            return
        except Exception:
            # thử đóng overlay giữa các lần
            await dismiss_overlays(page)
            continue
    # Fallback: Enter
    await page.keyboard.press("Enter")


async def confirm_posted(page: Page, my_text: str = "") -> bool:
    # 1) thấy item comment có “You”
    try:
        await page.locator('[data-e2e="comment-item"] >> text=You').first.wait_for(timeout=3000)
        return True
    except Exception:
        pass
    # 2) hoặc thấy chính text mình vừa gửi
    if my_text:
        try:
            await page.locator(f'[data-e2e="comment-item"] >> text="{my_text[:30]}"').first.wait_for(timeout=2000)
            return True
        except Exception:
            pass
    # 3) kiểm tra toast lỗi
    try:
        toast = page.locator('div:has-text("failed"), div:has-text("captcha"), div:has-text("blocked")').first
        await toast.wait_for(timeout=1500)
        txt = (await toast.text_content()) or ""
        bad = ["commenting too fast", "bình luận không thành công", "action blocked", "captcha"]
        if any(x in txt.lower() for x in bad):
            return False
    except Exception:
        pass
    # không chắc: tạm coi là OK
    return True


async def solve_simple_captcha_if_any(page: Page) -> bool:
    try:
        await page.wait_for_selector('div:has-text("verify") , iframe[src*="captcha"]', timeout=2000)
        print("[!] Captcha detected. Pausing 60s for manual solve...")
        await page.wait_for_timeout(60_000)
        return True
    except PWTimeout:
        return True

# ------------------- Runner/CLI -------------------
def parse_args():
    ap = argparse.ArgumentParser(description="GPM v3 -> TikTok comment bot")
    ap.add_argument("--profile", required=True, help="GPM profile_id")
    ap.add_argument("--video", required=True, help="TikTok video URL")
    ap.add_argument("--comment", required=True, help="Comment text")
    ap.add_argument("--max-retries", type=int, default=2)
    ap.add_argument("--wait-login", type=int, default=90, help="Seconds to wait for manual login if needed")
    return ap.parse_args()

async def run(profile_id: str, video_url: str, comment_text: str, max_retries: int, wait_login_sec: int) -> None:
    print("[*] Using GPM_URL =", GPM_URL)
    print("[*] Has token    =", bool(GPM_TOKEN))
    print("[*] Starting GPM profile:", profile_id)

    pw, browser = await connect_gpm_v3_and_get_browser(profile_id)
    context = browser.contexts[0] if browser.contexts else await browser.new_context()
    page = context.pages[0] if context.pages else await context.new_page()

    try:
        await page.goto(TT_BASE, wait_until="domcontentloaded")
        if not await ensure_logged_in(page):
            print(f"[!] Not logged in. Opening /login and waiting {wait_login_sec}s...")
            await page.goto("https://www.tiktok.com/login", wait_until="domcontentloaded")
            await page.wait_for_timeout(wait_login_sec * 1000)

        await open_video(page, video_url)

        tries = 0
        while tries <= max_retries:
            tries += 1
            print(f"[*] Attempt {tries}/{max_retries + 1}")
            try:
                await type_comment(page, comment_text)
                await submit_comment(page)
                ok = await confirm_posted(page, comment_text)
                if ok:
                    print("[+] Comment posted (or likely posted).")
                    break
                else:
                    print("[!] Post failed. Checking captcha or rate-limit...")
                    await solve_simple_captcha_if_any(page)
            except Exception as e:
                print(f"[!] Error attempt {tries}: {e}")

            backoff = min(60, (2 ** tries) + random.randint(0, 5))
            print(f"    Backing off {backoff}s...")
            await asyncio.sleep(backoff)

    finally:
        try:
            await browser.close()
        except Exception:
            pass
        await pw.stop()

async def dismiss_overlays(page: Page) -> None:
    """Đóng các lớp overlay/modal/tooltip thường gặp trên TikTok desktop."""
    # thử nhấn ESC vài lần
    for _ in range(3):
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass
        # click vào các nút close/đồng ý/ok nếu có
        for sel in (
            "[data-e2e='modal-close-icon']",
            "button:has-text('Close')",
            "button:has-text('Đóng')",
            "button:has-text('OK')",
            "button:has-text('Got it')",
            "button:has-text('Tôi đã hiểu')",
            "[data-e2e='tux-modal-close']",
            "div.TUXModal-overlay",  # click ra ngoài
        ):
            try:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    # Dùng force để vượt qua intercept
                    await loc.first.click(timeout=800, force=True)
            except Exception:
                pass
        # nếu vẫn còn overlay đặc thù
        try:
            await page.locator("div.TUXModal-overlay").first.click(timeout=500, force=True)
        except Exception:
            pass
        # ngắn một chút giữa các vòng
        try:
            await page.wait_for_timeout(200)
        except Exception:
            pass

if __name__ == "__main__":
    # ⚠️ Điền cứng thông tin mặc định vào đây
    profile_id = "ddfdc3df-8a2f-40b4-b1ef-933b1802411d"
    video_url  = "https://www.tiktok.com/@elz.study/video/7554793319776128263"
    comment    = "bóng đèn sáng hết hạn sử dụng"
    max_retries = 2
    wait_login  = 90

    asyncio.run(run(profile_id, video_url, comment, max_retries, wait_login))

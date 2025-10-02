import asyncio
import json
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# Ví dụ: GPM mở profile ở cổng 9222 (Remote Debugging)
CDP_URL = "http://127.0.0.1:9222"

async def comment_with_gpm(video_url: str, text: str, cdp_url: str = CDP_URL):
    async with async_playwright() as p:
        # 1) Kết nối vào browser của GPM (đang chạy profile)
        browser = await p.chromium.connect_over_cdp(cdp_url)

        # Dùng "persistent context" của profile (thường là contexts[0])
        if browser.contexts:
            context = browser.contexts[0]
        else:
            # fallback – hiếm khi cần
            context = await browser.new_context()

        # Lấy (hoặc tạo) trang làm việc
        page = context.pages[0] if context.pages else await context.new_page()

        # 2) Kiểm tra đã login TikTok chưa (cookie sessionid)
        cookies = await context.cookies(["https://www.tiktok.com"])
        names = {c["name"] for c in cookies}
        logged_in = ("sessionid" in names or "sessionid_ss" in names)
        print("Logged in:", logged_in)

        # Nếu CHƯA login, mở trang login và chờ bạn đăng nhập tay trên GPM (1 phút)
        if not logged_in:
            print("⚠️ Chưa login. Mở trang login và chờ bạn đăng nhập tay (tối đa 60s).")
            await page.goto("https://www.tiktok.com/login", timeout=60000)
            # Tạm dừng cho bạn thao tác tay (có captcha/2FA thì xử lý luôn tại đây)
            for _ in range(60):
                await page.wait_for_timeout(1000)
                cookies = await context.cookies(["https://www.tiktok.com"])
                names = {c["name"] for c in cookies}
                if "sessionid" in names or "sessionid_ss" in names:
                    logged_in = True
                    break
            print("Logged in (after manual)?", logged_in)
            if not logged_in:
                print("❌ Không thấy sessionid. Hủy.")
                await browser.close()
                return

        # 3) Mở video
        await page.goto(video_url, wait_until="domcontentloaded", timeout=60000)

        # 4) Mở panel comment (nếu cần)
        for sel in ("[data-e2e='browse-comment']", "button:has-text('Comments')"):
            try:
                await page.click(sel, timeout=1500)
                break
            except PWTimeout:
                pass

        # 5) Tìm ô nhập (contenteditable) và gõ nội dung
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

        # 6) Gửi bình luận
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

        # 7) Bắt response POST /api/comment/publish và in kết quả
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
            print("⚠️ Không bắt được /api/comment/publish (15s). Có thể UI/selector khác hoặc bị chặn.")

        await browser.close()


# Cách dùng (chạy trong file .py):
if __name__ == "__main__":
    asyncio.run(comment_with_gpm(
        video_url="https://www.tiktok.com/@hnhtmlinh.963133/video/7544200418780351752",
        text="video oke quá"
    ))

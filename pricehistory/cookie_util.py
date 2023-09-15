from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync


def get_cookies(cookie_url: str) -> dict:
    with sync_playwright() as p:
        browser = p.firefox.launch()
        page = browser.new_page()
        stealth_sync(page)
        page.goto(cookie_url)
        cookies = page.context.cookies()
        browser.close()

    session_cookies = {}
    for cookie in cookies:
        if cookie["name"].startswith("incap_ses_"):
            session_cookies[cookie["name"]] = cookie["value"]

    return session_cookies

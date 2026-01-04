from playwright.sync_api import sync_playwright

URL = "https://www.uniqlo.com/kr/ko/products/E470549-000/00"

KEYWORDS = [
    "stock", "avail", "inventory", "order", "fulfill", "store"
]

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(locale="ko-KR")
        page = context.new_page()

        def on_response(resp):
            url = resp.url.lower()
            if any(k in url for k in KEYWORDS):
                print("STATUS:", resp.status)
                print("URL:", resp.url)
                try:
                    print("CONTENT-TYPE:", resp.headers.get("content-type"))
                except:
                    pass
                print("-" * 80)

        page.on("response", on_response)
        page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(5000)

        browser.close()

if __name__ == "__main__":
    main()
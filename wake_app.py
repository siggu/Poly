"""
Streamlit 앱을 자동으로 깨우는 스크립트
GitHub Actions에서 정기적으로 실행하여 앱이 sleep 모드에 들어가는 것을 방지합니다.
"""
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Streamlit 앱 URL 설정 (GitHub Secrets에서 가져오거나 여기에 직접 입력)
STREAMLIT_APP_URL = os.getenv("STREAMLIT_APP_URL", "YOUR_STREAMLIT_APP_URL_HERE")

def wake_streamlit_app():
    """Streamlit 앱을 방문하고 필요시 깨우기 버튼을 클릭"""
    print(f"🚀 Streamlit 앱 깨우기 시작: {STREAMLIT_APP_URL}")

    # Chrome 옵션 설정 (헤드리스 모드)
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    driver = None
    try:
        # ChromeDriver 설정
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)

        # 앱 방문
        print("🌐 앱 URL로 이동 중...")
        driver.get(STREAMLIT_APP_URL)

        # 페이지 로드 대기
        time.sleep(5)

        # Sleep 상태인지 확인하고 깨우기 버튼 클릭 시도
        try:
            # "Yes, get this app back up!" 버튼 찾기
            wait = WebDriverWait(driver, 10)
            wake_button = wait.until(
                EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'Yes, get this app back up!')]"))
            )

            print("😴 앱이 sleep 상태입니다. 깨우기 버튼 클릭 중...")
            wake_button.click()

            # 앱이 깨어날 때까지 대기
            time.sleep(10)
            print("✅ 앱을 성공적으로 깨웠습니다!")

        except Exception as e:
            # 버튼이 없으면 이미 활성 상태
            print("✅ 앱이 이미 활성 상태입니다!")

        # 페이지 스크린샷 (디버깅용)
        screenshot_path = "streamlit_app_status.png"
        driver.save_screenshot(screenshot_path)
        print(f"📸 스크린샷 저장됨: {screenshot_path}")

        print("🎉 작업 완료!")

    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        raise

    finally:
        if driver:
            driver.quit()
            print("🔚 브라우저 종료")

if __name__ == "__main__":
    if STREAMLIT_APP_URL == "YOUR_STREAMLIT_APP_URL_HERE":
        print("⚠️  경고: STREAMLIT_APP_URL을 설정해주세요!")
        print("GitHub Secrets에 'STREAMLIT_APP_URL'을 추가하거나")
        print("wake_app.py 파일의 STREAMLIT_APP_URL 변수를 수정하세요.")
        exit(1)

    wake_streamlit_app()

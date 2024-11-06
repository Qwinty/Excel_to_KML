import undetected_chromedriver as uc
import time
import os
import pickle

from selenium.common import WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class YandexMapAutomation:
    def __init__(self):
        self.cookies_dir = "browser_session"
        if not os.path.exists(self.cookies_dir):
            os.makedirs(self.cookies_dir)

        self.cookies_file = os.path.join(self.cookies_dir, "yandex_cookies.pkl")

        options = uc.ChromeOptions()
        options.add_argument('--start-maximized')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        self.session_dir = os.path.abspath("browser_session")
        self.driver = uc.Chrome(options=options, user_data_dir=self.session_dir)
        self.wait = WebDriverWait(self.driver, 15)

    def load_cookies(self, domain):
        """Load cookies if they exist"""
        try:
            if os.path.exists(self.cookies_file):
                with open(self.cookies_file, 'rb') as file:
                    cookies = pickle.load(file)
                    for cookie in cookies:
                        # Ensure cookie domain matches current domain
                        if domain in cookie.get('domain', ''):
                            try:
                                self.driver.add_cookie(cookie)
                            except WebDriverException as e:
                                print(f"Skipping invalid cookie: {e}")
                return True
        except Exception as e:
            print(f"Error loading cookies: {e}")
        return False

    def save_cookies(self):
        """Save current session cookies"""
        try:
            cookies = self.driver.get_cookies()
            with open(self.cookies_file, 'wb') as file:
                pickle.dump(cookies, file)
        except Exception as e:
            print(f"Error saving cookies: {e}")

    def navigate_to_map_constructor(self):
        """Navigate to Yandex Map Constructor"""
        try:
            # First navigate to the main domain
            print("Navigating to main Yandex domain...")
            self.driver.get("https://ya.ru")
            time.sleep(2)  # Wait for initial page load

            # Load cookies for yandex.ru domain
            # self.load_cookies("yandex.ru")

            print("Navigating to Map Constructor...")
            self.driver.get("https://yandex.ru/map-constructor/")

            # Wait for the page to load
            self.wait.until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            self.upload_map(
                r"C:\Users\makse\OneDrive\!PROGRAMMING\!WORK\Excel_to_KML\output\separated_regions\kml\Амурская_область.kml")

            # Save new cookies
            # self.save_cookies()

            return True

        except Exception as e:
            print(f"Navigation error: {str(e)}")
            return False

    def close_browser(self):
        """Close the browser and clean up"""
        try:
            # self.save_cookies()
            self.driver.quit()
        except Exception as e:
            print(f"Error while closing browser: {str(e)}")

    def upload_kml(self, filename):
        """
        Upload KML file to Yandex Map Constructor

        Args:
            filename (str): Relative or absolute path to the KML file
        """
        try:
            # Convert relative path to absolute path
            absolute_path = os.path.abspath(filename)

            if not os.path.exists(absolute_path):
                raise FileNotFoundError(f"KML file not found: {absolute_path}")

            # Wait for the import button to be clickable (up to 20 seconds)
            import_button = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'К импорту')]"))
            )

            # Click the import button
            import_button.click()

            # Wait for file input to be present
            file_input = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']"))
            )

            # Send the file path to the input
            file_input.send_keys(absolute_path)

            # Wait for upload to complete (you might need to adjust the selector)
            self.wait.until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'upload-success')]"))
            )

            print(f"Successfully uploaded KML file: {filename}")
            return True

        except FileNotFoundError as e:
            print(f"File error: {e}")
            return False
        except Exception as e:
            print(f"Upload error: {str(e)}")
            return False


def main():
    # Initialize the automation class
    automation = YandexMapAutomation()

    try:
        # Navigate to the map constructor
        success = automation.navigate_to_map_constructor()

        if success:
            print("Successfully navigated to Yandex Map Constructor")

            # Add a delay to keep the browser open for a while
            input("Press Enter to close the browser")  # Adjust the delay as needed

        else:
            print("Failed to navigate to Yandex Map Constructor")

    except Exception as e:
        print(f"An error occurred in main: {str(e)}")

    finally:
        # Close the browser
        automation.close_browser()


if __name__ == "__main__":
    main()

# Created/Modified files during execution:
# - browser_session/yandex_cookies.pkl

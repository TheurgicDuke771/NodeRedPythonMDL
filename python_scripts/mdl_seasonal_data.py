from datetime import datetime
import cloudscraper


ETL_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_seasonal_data() -> list:
    try:
        now = datetime.now()
        current_year = now.year
        current_quarter = ((now.month - 1) // 3) + 1

        scraper = cloudscraper.create_scraper()
        seasonal_raw = scraper.post(
            url="https://mydramalist.com/v1/quarter_calendar",
            data={"quarter": current_quarter, "year": current_year},
        )
        if seasonal_raw.status_code == 200:
            return seasonal_raw.json()
        else:
            raise Exception(f"URL: {seasonal_raw.url}. Response: {seasonal_raw.text}")
    except Exception as e:
        print(f"{ETL_TIME} ERROR: Got error while fetching API data. {str(e)}")
        return []
    finally:
        scraper.close()

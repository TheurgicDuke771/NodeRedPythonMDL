from datetime import datetime
import cloudscraper


def get_seasonal_data() -> list:
    try:
        now = datetime.now()
        current_year = now.year
        current_quarter = ((now.month - 1) // 3) + 1

        scraper = cloudscraper.create_scraper()
        seasonal = scraper.post(
            url="https://mydramalist.com/v1/quarter_calendar",
            data={"quarter": current_quarter, "year": current_year},
        ).json()

        return seasonal
    except Exception as e:
        print(f"Got error while fetching API data. {str(e)}")
        return []
    finally:
        scraper.close()

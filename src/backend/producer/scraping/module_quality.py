from dateutil.parser import parse
from datetime import datetime, timedelta, timezone

def filter_and_check_quality(results, logger, column):
    threshold_date = datetime.now(timezone.utc)
    filtered_results = []
    dates = []

    for item in results:
        if item[column]:
            try:
                if column == "published":
                    published_date = parse(item[column])
                    if published_date.tzinfo is None:
                        published_date = published_date.replace(tzinfo=timezone.utc)
                else:
                    if item[column] > 1e12:
                        item[column] /= 1000
                    published_date = datetime.fromtimestamp(item[column], tz=timezone.utc)

                if threshold_date - timedelta(hours=4) <= published_date < threshold_date:
                    filtered_results.append(item)
                    dates.append(published_date)

            except Exception as e:
                logger.warning(f"Error while parsing date for item: {e}")
                continue
        else:
            logger.info(f"There is null exists! At {item['domain']} - {item['title']}")

    logger.info(f"Total results: {len(results)}")
    logger.info(f"Filtered (last 4h): {len(filtered_results)}")

    if dates: 
        logger.info(f"Oldest day: {min(dates).isoformat()}")
        logger.info(f"Nearest day: {max(dates).isoformat()}")

    null_text_count = sum(1 for item in filtered_results if item.get("text") is None)
    logger.info(f"Text without body: {null_text_count}")

    return filtered_results
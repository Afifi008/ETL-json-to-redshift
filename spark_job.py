import logging
import sys

from spark_session import get_spark
from transformations import *
from redshift_writer import write_to_redshift
from config import JSON_PATH, REDSHIFT_URL, REDSHIFT_PROPERTIES
from email_notifier import send_failure_email
logging.basicConfig(level=logging.INFO)

def run():
    spark = get_spark()

    try:
        logging.info("Reading source JSON")

        #raise Exception("Email notification test") #test 
        df_json = spark.read.json(JSON_PATH)

        ######## write data
        jobs = {
           "silver.listings": build_listings(df_json),
           "silver.hosts": build_hosts(df_json),
           "silver.listing_address": build_listing_address(df_json),
           "silver.reviews": build_reviews(df_json),
           "silver.listing_amenities": build_listing_amenities(df_json),
           "silver.review_scores": build_review_scores(df_json),
            "gold.fact_listings": build_fact_listings(df_json),
            "gold.dim_hosts": build_dim_hosts(df_json),
            "gold.dim_listings": build_dim_listings(df_json)
        }
        ######################
        logging.info("Building listings dataframe")
        #df_listings = build_listings(df_json)

        for table_name, df_transformed in jobs.items():
            logging.info("Writing data to Redshift table: %s", table_name)

            write_to_redshift(
                df=df_transformed,
                table=table_name,
                url=REDSHIFT_URL,
                properties=REDSHIFT_PROPERTIES
            )

        logging.info("Spark job finished successfully for all Silver and Gold tables")

    except Exception as e:
        logging.exception("Spark job failed")
        send_failure_email(e)
        sys.exit(1)

    finally:
        spark.stop()

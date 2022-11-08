import os
import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, \
    explode, weekofyear, year, avg, count

LOG_TAG = 'YelpDatasetAggregator'


class YelpDatasetAggregator:

    def __init__(self):
        self._app_name = 'yelp_aggregator'
        self.logger = logging.getLogger(LOG_TAG)
        logging.basicConfig(level=logging.INFO)

        self._spark_session = False
        try:
            self._spark_session = SparkSession.builder \
                .appName(self._app_name) \
                .getOrCreate()
            logging.info(f'Spark session for {self._app_name} '
                         f'was successfully created')
        except:
            logging.info(f'Spark session for {self._app_name} '
                         f'was unsuccessful')

    def read_json_data(self,
                       input_data,
                       table_name='temp'):
        """
        Reads data from json

        Args:
            input_data (string): Full path to json file with input data
            table_name (string): Name of the temporary table

        Returns:
            (pyspark.sql.dataframe.DataFrame): Resulted cleaned up table
        """
        df = ''

        if os.path.exists(input_data):

            self.logger.info(f'Reading {input_data}')
            df = self._spark_session.read.json(input_data)
            df.createOrReplaceTempView(table_name)

        else:
            self.logger.info(f'T input data path does not exist {input_data}')

        return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-data',
                        type=str,
                        help='Path to raw data directory')
    parser.add_argument('--output-data',
                        type=str,
                        help='Path of directory to store the clean data')

    opt = parser.parse_args()

    yelp_aggregator = YelpDatasetAggregator()

    businesses_df = yelp_aggregator.read_json_data(
        os.path.join(opt.input_data, 'yelp_academic_dataset_business.json')
    )

    reviews_df = yelp_aggregator.read_json_data(
        os.path.join(opt.input_data, 'yelp_academic_dataset_review.json')
    )

    checkin_df = yelp_aggregator.read_json_data(
        os.path.join(opt.input_data, 'yelp_academic_dataset_checkin.json')
    )

    # Calculate weekly average stars per business
    reviews_df = reviews_df.withColumn('week', weekofyear('date'))
    reviews_df = reviews_df.withColumn('year', year('date'))

    grouped_weekly = reviews_df\
        .select("*")\
        .groupBy("business_id", "week", "year")\
        .agg(avg("stars").alias("avg_stars_weekly"))\
        .sort("avg_stars_weekly", ascending=False)

    yelp_aggregator.logger.info("Show average stars per business on weekly basis")
    grouped_weekly.show(10)

    grouped_weekly_yearly = grouped_weekly\
        .select("*")\
        .groupBy("business_id", "year")\
        .agg(avg("avg_stars_weekly").alias("avg_stars_weekly_yearly"))\
        .sort("avg_stars_weekly_yearly", ascending=False)

    yelp_aggregator.logger.info("Show weekly average of stars per business for an entire year")
    grouped_weekly_yearly.show(10)

    # Splitting date in checkins to count the entries per week and year
    checkin_df = checkin_df.withColumn('exploded_date', explode(split('date', ',')))
    checkin_df = checkin_df.withColumn('checkin_week', weekofyear('exploded_date'))
    checkin_df = checkin_df.withColumn('checkin_year', year('exploded_date'))

    checkin_df = checkin_df.drop('exploded_date')
    yelp_aggregator.logger.info("Exploding checkin dates based on week and year")
    checkin_df.show(10)

    # Combining businesses, weekly stars, overall stars and checkins
    businesses_reviews_weekly = businesses_df.\
        join(grouped_weekly, 'business_id')
    businesses_reviews_weekly_checkins = businesses_reviews_weekly.\
        join(checkin_df, 'business_id')

    yelp_aggregator.logger.info("Show businesses, weekly stars, overall stars and checkins")

    businesses_reviews_weekly_checkins\
        .select("name", "avg_stars_weekly", "stars",
                 "checkin_week", "checkin_year")\
        .groupBy("name", "avg_stars_weekly", "stars",
                 "checkin_week", "checkin_year") \
        .agg(count("checkin_week").alias("weekly_checkins"))\
        .sort("weekly_checkins", ascending=False)\
        .show(10)

    # Combining businesses, weekly stars averaged per year, overall stars and checkins
    businesses_reviews_wy = businesses_df.\
        join(grouped_weekly_yearly, 'business_id')
    businesses_reviews_wy_checkins = businesses_reviews_wy.\
        join(checkin_df, 'business_id')

    yelp_aggregator.logger.info("Show businesses, weekly stars averaged per year, overall stars and checkins")

    businesses_reviews_wy_checkins\
        .select("name", "avg_stars_weekly_yearly",
                "stars", "checkin_year")\
        .groupBy("name", "avg_stars_weekly_yearly",
                 "stars", "checkin_year") \
        .agg(count("checkin_year").alias("yearly_checking")) \
        .sort("yearly_checking", ascending=False) \
        .show(10)
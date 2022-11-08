import os
import argparse
import logging

from pyspark.sql import SparkSession

LOG_TAG = 'YelpDatasetProcessor'


class YelpDatasetProcessor:

    def __init__(self):
        self._app_name = 'yelp_processor'
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

    def data_clean_up(self,
                      input_data,
                      output_data,
                      query='',
                      join='',
                      table_name='temp'):
        """
        Performs very basic data cleaning and data filtering on a given table.
        It supports custom SELECT queries and JOINs, removes any rows with NA columns
        and stores the clean data into a new location.

        Args:
            input_data (string): Full path to json file with input data
            output_data (string): Full path to json file that output data will be written
            query (string): SQL query, optionally null if no specific query should be applied
            join (dict): Dictionary with the following properties
                - 'data': pyspark.sql.dataframe.DataFrame object
                - 'column_id': string for the column ID to be used for the join
                - 'type': string for the desired join type
            table_name (string): Name of the temporary table

        Returns:
            (pyspark.sql.dataframe.DataFrame): Resulted cleaned up table
        """
        sql_df = ''
        output_head, output_tail = os.path.split(output_data)

        if os.path.exists(input_data):

            self.logger.info(f'Reading {input_data}')
            df = self._spark_session.read.json(input_data)
            df.createOrReplaceTempView(table_name)

            if query:
                sql_df = self._spark_session.sql(query)
                self.logger.info(f'Querying rows {sql_df.count()}')
            else:
                sql_df = self._spark_session.sql(f'SELECT * FROM {table_name}')

            if join:
                sql_df = sql_df.join(join['data'],
                                     join['column_id'],
                                     join['type'])

            row_count = sql_df.count()
            sql_df.na.drop('any').show(truncate=False)
            self.logger.info(f'Dropped {row_count - sql_df.count()} rows with NA columns')

            if os.path.exists(output_head):
                self.logger.info(f'Writing result to {output_data}')
                sql_df.write.json(output_data)
            else:
                self.logger.info(f'The output data path does not exist {output_data}')
        else:
            self.logger.info(f'T input data path does not exist {input_data}')

        return sql_df


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-data',
                        type=str,
                        help='Path to raw data directory')
    parser.add_argument('--output-data',
                        type=str,
                        help='Path of directory to store the clean data')

    opt = parser.parse_args()

    yelp_processor = YelpDatasetProcessor()

    businesses_query = "SELECT * FROM businesses WHERE state='AZ' AND is_open=1"
    businesses_df = yelp_processor.data_clean_up(
        os.path.join(opt.input_data, 'yelp_academic_dataset_business.json'),
        os.path.join(opt.output_data, 'yelp_academic_dataset_business.json'),
        businesses_query,
        join={},
        table_name='businesses'
    )

    # Filtering reviews, checkin and tip by business_ID
    reviews_join = {
        'data': businesses_df,
        'column_id': 'business_id',
        'type': 'leftsemi'
    }
    reviews_df = yelp_processor.data_clean_up(
        os.path.join(opt.input_data, 'yelp_academic_dataset_review.json'),
        os.path.join(opt.output_data, 'yelp_academic_dataset_review.json'),
        query='',
        join=reviews_join,
        table_name='reviews'
    )

    checkin_join = {
        'data': businesses_df,
        'column_id': 'business_id',
        'type': 'leftsemi'
    }
    checkin_df = yelp_processor.data_clean_up(
        os.path.join(opt.input_data, 'yelp_academic_dataset_checkin.json'),
        os.path.join(opt.output_data, 'yelp_academic_dataset_checkin.json'),
        query='',
        join=checkin_join,
        table_name='reviews'
    )

    tip_join = {
        'data': businesses_df,
        'column_id': 'business_id',
        'type': 'leftsemi'
    }
    tip_df = yelp_processor.data_clean_up(
        os.path.join(opt.input_data, 'yelp_academic_dataset_tip.json'),
        os.path.join(opt.output_data, 'yelp_academic_dataset_tip.json'),
        query='',
        join=tip_join,
        table_name='reviews'
    )

    # Filtering user by user_id from the reviews table

    user_join = {
        'data': reviews_df,
        'column_id': 'user_id',
        'type': 'leftsemi'
    }
    user_df = yelp_processor.data_clean_up(
        os.path.join(opt.input_data, 'yelp_academic_dataset_user.json'),
        os.path.join(opt.output_data, 'yelp_academic_dataset_user.json'),
        query='',
        join=user_join,
        table_name='reviews'
    )

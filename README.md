# Yelp Dataset Data Engineering

This project is sections into two classes that can be further extended to support data engineering of the Yelp Dataset:

### `Class YelpDatasetProcessor`
Considering that the Yelp dataset is about 5GB, I decided to cherry-picked a small amount of raw data based on the `state` location of a business and is the business is still open. All data in `clean_data/` correspond to businesses, reviews, checkins and tips from businesses in `IL`.

I decided to create this class to flexibly read through all Yelp json raw data tables and to support custom queries and joins along with basic operations such as dropping any rows with null values. The class stores the clean data in a new directory.

### `Class YelpDatasetAggregator`

Reads from the clean data and performs the requested aggregations on the average stars of a business received on weekly and yearly basis, along with the overall business star rating and amount of checkins for the corresponding period.

#### Average stars per week

To calculate the average stars per week, I expanded the reviews table with two additional columns for the `weekofyear()` and `year()`, extracted by the `date` column. I then proceeded to generate the average of the given stars by grouping the `business_id`, `week`, `year` in a new column `avg_stars_weekly`.
Considering that this created additional rows of data on the table, I then created a new star average count that averages all `avg_stars_weekly` based on `business_id` and the `year`.

#### Number of checkins

For the number of checkins, I exploded the `date` column in the checkin table and expanded it with two new columns for `weekofyear` and `year`. I then proceeded in two different aggregations to counting the checkins `weekly` or `yearly` based on the following group of columns (`name`, `avg_stars_weekly`, `stars`, `checkin_year`).

Something I noticed here, was that the review dates did not always much the dates of the checkins, as one of my initial ideas was to join the three tables on a combined primary id of `business_id`, `week`, `year`. I am still doubting that my method is correct, but at this point I was too exhausted to keep on going..

# How to run

Unzip the `yel_dataset/clean_data.zip`:
```commandline
unzip yelp_dataset/clean_data.zip -d ./yelp_dataset
```

First build the docker image:
```commandline
docker build -t yelp_pyspark . 
```

Run the docker container on interactive mode:
```commandline
docker run -it yelp_pyspark
```

In the interactive mode, run the pipeline script:
```commandline
./pipeline.sh agg
```

The pipeline script is designed to accept one of the following three arguments:
- `full`: To run the full pipeline (data cleaning and aggregations). My idea was to also support downloading the Yelp dataset, but I was getting a 404 on the download URL and gave up hope. So if you'd decide to run the pipeline in full, you will have to extract the yelp dataset in `./yelp_dataset/raw_data/`
- `clean`: This step runs only the `YelpDatasetProcessor.py` to clean up the data. Similarly to `full` you need to place the yelp dataset in `./yelp_dataset/raw_data/`
- `agg`: Performs the aggregations on the data stored in `./yelp_dataset/clean_data`
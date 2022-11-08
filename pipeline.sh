#!/bin/bash

if [[ $1 == "full" ]]; then
	echo "Running the full pipeline";
  #	wget -P /app/yelp_dataset/ https://yelp-dataset.s3.amazonaws.com/YDC22/yelp_dataset.tgz;
  # unzip /app/yelp_dataset/yelp_dataset.tgz;
fi

if [[ $1 == "clean" ]] || [[ $1 == "full" ]]; then
	echo "Cleaning up raw data";
	python YelpDatasetProcessor.py \
	      --input-data /app/yelp_dataset/raw_data \
	      --output-data /app/yelp_dataset/clean_data
fi

if [[ $1 == "agg" ]] || [[ $1 == "full" ]]; then
	echo "Running Aggregations on the clean data";
	python YelpDatasetAggregator.py \
	      --input-data /app/yelp_dataset/clean_data
fi
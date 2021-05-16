# Cleaning and Profiling Big Data: Air Pollution

## About
The project Air Pollution leverages big data tools to examine the level of indoor and outdoor air pollution from 1990-2019. The dataset, air_quality.csv (downloaded from The State of Global Air) contains 8611 rows with information on every country from 1990-2019 with the low, mean, and high exposure levels of three different pollutant types (indoor, outdoor ozone, and outdoor particle matter). The code 

## Cleaning
The cleaning code runs a Map Reduce job to filter out rows without all the columns filled in, changes thhe country name "United States of America" to "United States" (to join by country with another dataset that has county name as "United States") and outputs the result in a comma separated format.

## Profling
The profiling code runs a Map Reduce job to count the number of rows in the existing or cleaned dataset and provides information on the minimum and maximum values for each pollutant type.
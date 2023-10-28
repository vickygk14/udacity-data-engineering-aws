# Project 3 - AWS Data Lakehouse

## Overview
Project Details
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

trains the user to do a STEDI balance exercise;
and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
has a companion mobile app that collects customer data and interacts with the device sensors.
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

## Tools Used
- Python and Spark
- AWS Glue
- AWS Athena
- AWS S3

## Project Data

1. Customer Records
    - serialnumber
    - sharewithpublicasofdate
    - birthday
    - registrationdate
    - sharewithresearchasofdate
    - customername
    - email
    - lastupdatedate
    - phone
    - sharewithfriendsasofdate

2. Step Trainer Records
    - sensorReadingTime
    - serialNumber
    - distanceFromObject

3. Accelerometer Records
    - timeStamp
    - user
    - x
    - y
    - z

## Solution

### Files
1. `customer_landing.sql`  - Create glue table to hold raw customer records.
2. `customer_trusted.sql` - Create glue table to hold customer records who aggreed to share data for research purpose.
3. `customer_trusted.py` - Script which copy data from customer_landing to customer_trusted who aggreed to share data for research purpose.
4. `customer_curated.sql` - Create glue table to hold data for customers who aggreed to share data for research purpose and has accelerometer data.
5. `customer_curated.py` -  Script which copy data from customer_trusted to customer_curated. It filter for customer having accelerometer reading and aggreed to share data for research purpose.
6. `accelerometer_landing.sql` - Create glue table to hold raw accelerometer data.
7. `accelerometer_trusted.sql` - Create glue table to hold accelerometer data for customer who agreed to share data for research
8. `accelerometer_trusted.py` - Script which copy data from landing zone to trusted zone. It filter for accelerometer reading from customer who agreed to share data for research purpose. 
9. `step_trainer_landing.sql` - Create glue table to hold raw step trainer data.
10. `step_trainer_trusted.sql` - Create glue table to hold step trainer data for customer who agree to share data and also customer having accelerometer data.
11. `step_trainer_trusted.py` - Script copy data from landing to trusted zone. It filter reading from customer who agree to share data and also customer having accelerometer data.
12. `machine_learning_curated.sql` - Combine the result of accelerometer data with step trainer data for customer who agree to share data and also customer holding accelerometer data.
13. `machine_learning_curated.py` - Script which combine data from accelerometer_trusted, customer_trusted and step_trainer_trusted and produce result for data scientist.

## Result
1. Counts
    - customer_landing - 999
    - customer_trusted - 497
    - customer_curated - 492
    - accelerometer_landing - 744413
    - accelerometer_trusted - 413460
    - step_trainer_landing - 239760
    - step_trainer_trusted - 29970
    - machine_learning_curated - 1804
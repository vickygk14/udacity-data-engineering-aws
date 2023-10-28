CREATE EXTERNAL TABLE IF NOT EXISTS `udacity-project3-glue`.`machine_learning_curated` (
  `serialNumber` string,
  `x` float,
  `y` float,
  `z` float,
  `user` string,
  `distanceFromObject` int,
  `sensorReadingTime` bigint,
  `timestamp` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://udacity-project3-glue/machine_learning_curated/'
TBLPROPERTIES ('classification' = 'json');
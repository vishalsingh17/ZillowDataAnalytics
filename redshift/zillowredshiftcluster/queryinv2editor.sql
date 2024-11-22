# create the table 

CREATE TABLE IF NOT EXISTS zillowdata(
    bathrooms NUMERIC,
    bedrooms NUMERIC,
    city VARCHAR(225),
    homeStatus VARCHAR(225),
    homeType VARCHAR(225),
    zipcode INT
);
select * from zillowdata;
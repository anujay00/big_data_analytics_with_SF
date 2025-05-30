CREATE TABLE test11_data (
   _id STRING,
   age INTEGER,
   civilState STRING,
   createdAt TIMESTAMP_NTZ,
   familyMembers INTEGER,
   fuelType STRING,
   gender STRING,
   image STRING, --Assuming image is stored as a URL or base64 string; NULL allowed
   job STRING,
   monthlyIncome INTEGER,
   sector STRING,
   updatedAt TIMESTAMP_NTZ,
   vehicleBrand STRING,
   vehicleType STRING
);

--Create a stream to track changes on the table
CREATE STREAM test11_data_stream ON TABLE test11_data;

DESC TABLE test11_data;

--Select data (to verify once inserted)
SELECT * FROM test11_data;

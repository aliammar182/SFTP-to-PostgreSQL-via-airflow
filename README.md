# SFTP-to-PostgreSQL-via-airflow

This DAG is written in python and contains the following:

##Sensor:

It will detect file menitoned in path.

##Download:

It will download the file.

##Processing:

Processing it via pandas and other libraries and appending the data to existing postgreSQL.


##Note: Please make sure you have created the connections to both SFTP and PostgreSQL in airflow. Please see below link for guidance on how to connect to postgreSQL. You can use the same approach to connect to SFTP

https://www.youtube.com/watch?v=S1eapG6gjLU&t=281s

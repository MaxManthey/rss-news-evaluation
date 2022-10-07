# Parquet Persistence Evaluation

The project evaluates the possibilities of persisting news articles with Apache Spark, as well as the possibilities to run queries in different ways.  
This packages examines the possibility persisting data from JSON files to a H2 Database table with Apache Spark Dataframes.

## Run

To run the project, make sure to add the path for where your JSON files are saved [first arg] and the connection url for your H2 table (embedded/remote/tcp) [second arg].

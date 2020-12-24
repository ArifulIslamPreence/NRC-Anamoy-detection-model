# studentwinter2020

1. change the parameters of psycopg2.connect() according to your local database configuration
2. The backup file of database is uploaded called lanfeature.bak. 
3. To restore the database, create an empty database in postgres and run the following command using bash shell:

sudo psql -U your_username your_database_name < lanfeature.bak

4. This program takes the raw dataset and modifies to expected dataframe.
5. PCA algorithm is applied on modified database to reduce dimension.Run pcaoperation.py for PCA operation.

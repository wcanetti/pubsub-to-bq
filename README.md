# Change Data Capture
## Postgres >> Debezium >> Pub/Sub >> Dataflow >> BigQuery

### Objective

Replicate Postgres DB changes (INSERT, UPDATE, DELETE) thru CDC Debezium Connector by reading Postgres DB WAL (write ahead log) file.
Captured DB changes are sent to Cloud Pub/Sub and pushed to BigQuery thru the template Dataflow Job, Pub/Sub to BigQuery.

### Prerequisites

Create a folder where we will be storing all the files needed, such as Debezium Connector files, SQL scripts, Cloud SQL Proxy files, etc. As of this example we created a folder called *debezium-server-postgres* by running command:  

    mkdir debezium-server-postgres  

### Cloud SQL Postgres Instance Deployment

1) Deploy a Cloud SQL instance (Postgres) with the name of your preference in Google Cloud Console.
As of this example, these are the chosen names for instance, database, schema and table:  

    - instance name: *cdc-postgres-instance*  
    - database name: *postgres*  
    - schema: *inventory*
    - table: *customers*  

    IMPORTANT: Expand the "Show Configurations Options" to customize your instance and go to FLAGS section. Select from the dropdown the FLAG "cloudsql.logical_decoding" and set it to ON. This feature will allow logical decoding on the Cloud SQL instance to convert WAL (write ahead log) entries to JSON format. These JSON entries will be published to the Cloud Pub/Sub topic that we will later create.

2) Once the Cloud SQL database (Postgres) has been deployed, connect to your SQL instance using Google Cloud Shell with command:  

        gcloud sql connect <instance-name> --database=<database> --user=<user>  

3) Copy and paste SQL statements in file script-inventory.sql and run them to create the template data to be used for the puspose of this tutorial.  

4) Execute the following command to allow database replication for the user the Debezium Connector will use to connect. After performing this step Google Cloud SDK shell can be closed.  

        ALTER USER existing_user WITH REPLICATION;  

### Cloud SQL Auth Proxy

To connect to our Cloud SQL instance we will be using Cloud SQL proxy.

Download the Cloud SQL Proxy using the following command:  
    
    VERSION=v1.21.0    
    wget "https://storage.googleapis.com/cloudsql-proxy/$VERSION/cloud_sql_proxy.linux.amd64" -O cloud_sql_proxy  

Give execution permissions to the file using the following command:  
    
    chmod +x cloud_sql_proxy  

Connect to your Cloud SQL instance using the following command:  

    ./cloud_sql_proxy --instances=\<project\>:\<location\>:\<instance-name\>=tcp:0.0.0.0:5432

Check tables existance by running the following query:  
    
    SELECT table_schema, table_name    
    FROM information_schema.tables  
    WHERE table_schema = 'inventory'    
    ORDER BY table_name;  

Leave this terminal open as we will be issuing SQL statements to generate changes in the database data and test debezium connector and its change data capture functionality.

### Cloud Pub/Sub - Topics creation

Create a Cloud Pub/Sub topic with the following naming convertion:  

    \<instance-name\>.\<schema\>.\<table\>  

Our example will use the customers table (one of the table created with the SQL script we ran before).
Topic name for this example:

    cdc-postgres-instance.inventory.customers  

When creating the topic, select the option "Add a default subscription".

### Debezium Connector

1) Download from the Debezium connector from:  
https://debezium.io/documentation/reference/1.6/operations/debezium-server.html

2) Once downloaded, extract the content and paste it in the *debezium-server-postgres* folder we created.

3) Before putting the Debezium Connector to run, we will first need to edit the properties on the configuration file provided by Debezium. On the folder we created before, the *debezium-server-postgres*, enter subfolder *conf* and rename file *application.properties.example* to *application.properties*.

4) Open the file to edit it. Copy / paste the list of properties below and kindly replace with your correspondent property values.

        debezium.sink.pravega.scope=''  
        debezium.sink.type=pubsub  
        debezium.sink.pubsub.project.id=mimetic-might-312320  
        debezium.sink.pubsub.ordering.enabled=false  
        debezium.format.value=json  
        debezium.format.value.schemas.enable=false  
        debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector  
        debezium.source.offset.storage.file.filename=data/offsets.dat  
        debezium.source.offset.flush.interval.ms=0  
        debezium.source.database.hostname=localhost  
        debezium.source.database.port=5432  
        debezium.source.database.user=postgres  
        debezium.source.database.password=postgres  
        debezium.source.database.dbname=postgres  
        debezium.source.database.server.name=cdc-postgres-instance  
        debezium.source.table.include.list=inventory.customers  
        #debezium.source.schema.include.list=transactional  
        debezium.source.plugin.name=wal2json  

    Main properties that need to be edited based on your project preferences / configurations:

        - debezium.sink.pubsub.project.id
        - debezium.source.database.user
        - debezium.source.database.password
        - debezium.source.database.dbname
        - debezium.source.database.server.name
        - debezium.source.table.include.list

5) Create folder data and and an empty file called "*offsets.dat*".

        mkdir data  
        touch data/offsets.dat  

6) Execute from the terminal the executable file run.sh with the following command by being at the debezium-server-postgres directory level:

        run.sh

### Testing the Debezium Connector and checking messages reception on Cloud Pub/Sub

1) Execute a quick SELECT statement to check the available information in the *customers* table.

        SELECT * FROM postgres.inventory.customers ORDER BY id;

        id   | first_name | last_name | email  
        -----+------------+-----------+-----------------------  
        1001 | Sally      | Thomas    | sally.thomas@acme.com  
        1002 | George     | Bailey    | gbailey@foobar.com  
        1003 | Edward     | Walker    | ed@walker.com  
        1004 | Anne       | Kretchmar | annek@noanswer.org  

2) By replace values 'your-name', 'your last-name' and 'your e-mail' with your actual name, lastname and e-mail on the statement below, we will update user 1001 to test the Debezium Connector capturing feature:

        UPDATE postgres.inventory.customers  
        SET first_name = 'your-name',  
        last_name = 'your last-name',  
        email = 'your email'  
        WHERE id = 1001;

3) Once executed, go to Google Cloud Pub/Sub and verify the reception of the message on the subscription of the topic create by selecting option "View Messages" >> "Pull"

    You should be able to see a message with the following message body:

        {  
        "before":{  
            "id":1001,  
            "first_name":"Sally",  
            "last_name":"Thomas",  
            "email":"sally.thomas@acme.com"  
        },  
        "after":{  
            "id":1001,  
            "first_name":"your-name",  
            "last_name":"your last-name",  
            "email":"your email"  
        },  
        "source":{  
            "version":"1.6.0.Final",  
            "connector":"postgresql",  
            "name":"cdc-postgres-instance",  
            "ts_ms":1626716201707,  
            "snapshot":"false",  
            "db":"postgres",  
            "sequence":"[null,\"118013376\"]",  
            "schema":"inventory",  
            "table":"customers",  
            "txId":2731,  
            "lsn":118069040,  
            "xmin":null  
            },  
        "op":"u",  
        "ts_ms":1626716196328,  
        "transaction":null  
        }  

Note that there are two main structures in the json file which are "before" and "after", reflecting the update performed in Postgres database. Also additional metadata information is included in the "source" structure and on the root of the json file, which is valuable information that we will later use in BigQuery to get the most recent version of each record in the table.

### Streaming Inserts to BigQuery with Cloud Dataflow TEMPLATE Job

Prerequisite: Create a BigQuery table in the project and dataset of your preference named "customers_delta". Use the *schema_customers_delta.json* file attached to this project (within the other_resources subfolder) to set the schema to the created table using the option "edit as text".

Note 1: This solution is limited to deploying one Dataflow Job per source DB table, as the Debezium connector (as of this writing) supports a 1:1 relationship between the table the changes are being captured for and the Pub/Sub topic deployed to buffer the captured changes. The Dataflow Template Job allows to only specify one topic, so there's a 1:1 relationship also established at this point, which doesn't help to workaround the current Debezium limitation. Consider also that there's a quota limit of 25 concurrent streaming Dataflow jobs per project, so just employ / propose this solution in case the scope of the project is limited to a few tables / specific need.

Note 2: For a 1:N relationship between a Dataflow Job and multiple tables on the source DB, please consider this solution which consists of a custom Dataflow Job and lets you group a number of tables and get them processed by a single Dataflow Job by listening to multiple Pub/Sub topics in parallel at a time.

Steps for the Cloud Dataflow Template Job:

1) Go to Cloud Dataflow and select option "Create Job from Template".

2) Fill in a Job Name of your preference, e.g: *cdc-pubsub-bigquery*.

3) Select Dataflow template: "Pub/Sub Topic to BigQuery".

4) Insert the Pub/Sub topic as projects/\<project\>/topics/\<topic-name\>

5) Insert the BigQuery output table as \<project\>:\<dataset\>.\<table\>

6) Insert a Temporary Location (a GCS bucket) where Cloud Dataflow will leave intermediatte files, product of its processing, e.g: gs://cdc-postres-debezium-bq/temp

7) Leave all the other parameters with the options selected by default and press "RUN JOB".

### Batch Loads / Streaming Inserts to BigQuery with Cloud Dataflow CUSTOM Job

Prerequisite 1: Create a BigQuery table in the project and dataset of your preference named "customers_delta". Use the *schema_customers_delta.json* file attached to this project (within the other_resources subfolder) to set the schema to the created table using the option "edit as text".

Prerequisite 2: Create a BigQuery table in the project and dataset of your preference named "products_delta". Use the *schema_products_delta.json* file attached to this project (within the other_resources subfolder) to set the schema to the created table using the option "edit as text".

Prerequisite 3: Create another topic (apart from the cdc-postgres-instance.inventory.customers ==> already created in section above) called cdc-postgres-instance.inventory.products.

The JAVA code in this repository, resolves the constraints mentioned on the previous section. It allows to specify in the *configuration.properties file* in the resources subfolder of this project, the different tables (mapped 1:1 with topics in Cloud Pub/Sub), that we want to group and get changes for, in a single Dataflow job. You can list the desired number of tables, but please consider setting up the correct number of maximum dataflow workers and machine type, by taking in consideration the volume of data and the frequency of the changes.

It's important to mention that this code, performs batch loads to BigQuery, accumulating the events (database captured changes) every 2 minutes, writing them to a temporary bucket and then uploading the records of this 2 minutes bounded data to BigQuery. This is done by setting the write option "withMethod" to FILE_LOADS *.withMethod(BigQueryIO.Write.Method.FILE_LOADS)* and the option "withTriggeringFrequency" to *.withTriggeringFrequency(Duration.standardMinutes(2))*.

If you want to do it with *Streaming Inserts*, you will need to just change the write option "withMethod" to STREAMING_INSERTS and remove the "withTriggeringFrequency" option / sentence from the code.

Below are the configuration file properties to be specified:

    project=<your GCP project>
    database_instance=<your database instance>
    source_db_schema=<your database schema>
    tables=customers,products # the tables we want to capture, specify them delimited with comma
    bq_dataset=<your BigQuery dataset>
    topics_prefix=projects/<your GCP project>/topics/
    bucket_url=<a GCP bucket> # to store temporary / staging data
    bucket_schema_root_path=<the folder within the GCP bucket where the BigQuery tables schema are stored> e.g.: bq_schemas/

With all the prerequistes above created, we will now proceed with the code compilation and the execution of the code in this solution. This is the command to execute the program from the Cloud Shell. Replace your corresponde values in properties below:

    mvn compile exec:java \
    -Dexec.mainClass=org.apache.beam.cdc \
    -Dexec.cleanupDaemonThreads=false \
    -Dexec.args=" \
        --project=<your GCP project> \
        --region=us-central1 \
        --tempGCSBQBucket=<a GCP bucket> \
        --tempGCSDataflowBucket=<a GCP bucket>/temp \
        --runner=DataflowRunner"

### Immediate Consistency approach

With this approach, queries reflect the current state of the replicated data. Immediate consistency requires a query that joins the MAIN table and the DELTA table, and selects the most recent row for each primary key.

Getting back to our example, we have to first perform an initial load in a table in a BigQuery dataset, that we will call *customers_main*. This table will contain an snapshot of the current state of our table in the Postgres DB instance.

Let's first create the table. Use the *schema_customers_main.json* file attached to this project (within the other_resources subfolder) to set the schema using the option "edit as text".

After having created the table, you can use the script below to perform the initial load to the BigQuery *customers_main* table.

    INSERT INTO <dataset>.customers_main VALUES (1001,'Sally','Thomas','sally.thomas@acme.com',3600);
    INSERT INTO <dataset>.customers_main VALUES (1002,'George','Bailey','gbailey@foobar.com',3600);
    INSERT INTO <dataset>.customers_main VALUES (1003,'Edward','Walker','ed@walker.com',3600);
    INSERT INTO <dataset>.customers_main VALUES (1004,'Anne','Kretchmar','annek@noanswer.org',3600);

Note: you may also use the *schema_products_main.json* to also create the products main table and load initial data to do exactly the same excercise as we are going to do with the customers main table.

As time goes by, the *customers_main* table will start having outdated information. To achieve immediate consistency and to show the current version of the data, this approach proposes to JOIN the MAIN table (*customers_main*) with the DELTA table (*customers_delta*) which contains the track of the changes that occured in the Postgres DB instance.

    CREATE VIEW \<dataset\>.immediate_consistency_customers AS (
        SELECT * EXCEPT(op, row_num)  
        FROM (  
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts_ms DESC) AS row_num  
            FROM (  
                SELECT COALESCE(after.id,before.id) AS id, after.first_name, after.last_name, after.email, ts_ms, op  
                FROM `mimetic-might-312320.gentera.customers_delta`  
                UNION ALL  
                SELECT *, 'i'  
                FROM `mimetic-might-312320.gentera.customers_main`))  
        WHERE  
        row_num = 1  
        AND op <> 'D'  
    )

The SQL statement in the preceding BigQuery view does the following:

- The innermost UNION ALL produces the rows from both the main and the delta tables:
    - SELECT * EXCEPT(op), op FROM customers_delta forces the op (op = operation type) column to be the last column in the list.
    - SELECT *, ‘i' FROM customers_main selects the row from the main table as if it were an insert row.
    - Using the * operator keeps the example simple. If there were additional columns or a different column order, replace the shortcut with explicit column lists.
- SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts_ms DESC) AS row_num uses an analytic function in BigQuery to assign sequential row numbers starting with 1 to each of the groups of rows that have the same value of id, defined by the PARTITION BY clause. The rows are ordered by ts_ms (timestamp in milliseconds) in descending order within that group. Because ts_ms is guaranteed to increase, the latest change has a row_num column that has a value of 1.
- WHERE row_num = 1 AND op <> 'D' selects only the latest row from each group. This is a common deduplication technique in BigQuery. This clause also removes the row from the result if its change type is delete.
- The topmost SELECT * EXCEPT(op, row_num) removes the extra columns that were introduced for processing and which aren't relevant otherwise.

After you create the view, you can run queries against it. To get the most recent changes, you can run a query like the following:

### Cost Optimized approach

With this approach, queries are faster and less expensive at the expense of some delay in data availability. Data in the DELTA table is periodically merged with the data into the MAIN table.

    MERGE `mimetic-might-312320.gentera.customers_main` m  
    USING  
    (  
    SELECT * EXCEPT(row_num)  
    FROM (  
         SELECT *, ROW_NUMBER() OVER(PARTITION BY COALESCE(after.id,before.id) ORDER BY delta.ts_ms DESC) AS row_num  
         FROM `mimetic-might-312320.gentera.customers_delta` delta
         )  
         WHERE row_num = 1) d  
    ON  m.id = COALESCE(after.id,before.id)  
    WHEN NOT MATCHED  
    AND op IN ("c", "u") THEN  
    INSERT (id, first_name, last_name, email, ts_ms)  
    VALUES (d.after.id, d.after.first_name, d.after.last_name, d.after.email, d.ts_ms)  
    WHEN MATCHED  
    AND d.op = "d" THEN  
    DELETE  
    WHEN MATCHED  
    AND d.op = "u"  
    AND (m.ts_ms < d.ts_ms) THEN  
    UPDATE  
    SET first_name = d.after.first_name, last_name = d.after.last_name, email = d.after.email, ts_ms = d.ts_ms

The best way to merge data frequently and consistently is to use a MERGE statement, which lets you combine multiple INSERT, UPDATE, and DELETE statements into a single atomic operation. Following are some of the nuances of the preceding MERGE statement:

- The customers_main table is merged with the data source that is specified in the USING clause, a subquery in this case.
- The subquery uses the same technique as the view in immediate consistency approach: it selects the latest row in the group of records that have the same id value—a combination of ROW_NUMBER() OVER(PARTITION BY COALESCE(after.id,before.id) ORDER BY ts_ms DESC) row_num and WHERE row_num = 1.
- Merge is performed on the id columns of both tables, which is the primary key.
- The WHEN NOT MATCHED clause checks for a match. If there is no match, the query checks that the latest record is either insert or update, and then inserts the record.
- When the record is matched and the operation type is delete, the record is deleted in the main table.
- When the record is matched, the operation type is update, and the delta table's ts_ms value is higher than the ts_ms value of the main record, the data is updated, including the most recent ts_ms value.  

The preceding MERGE statement works correctly for any combinations of the following changes:
- Multiple update rows for the same primary key: only the latest update will apply.
- Unmatched updates in the main table: if the main table doesn't have the record under the primary key, a new record is inserted.

This approach skips the main table extract and starts with the delta table. The main table is automatically populated.
Insert and update rows in the unprocessed delta batch. The most recent update row is used and a new record is inserted into the main table.
Insert and delete rows in the unprocessed batch. The record isn't inserted.

For more information please refer to Google official documentation in the following url:
https://cloud.google.com/architecture/database-replication-to-bigquery-using-change-data-capture
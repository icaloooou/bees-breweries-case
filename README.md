## BEES Data Engineering - Breweries Case
---
### How to run?
In the utils/config.py you need to put the AWS credentials and AWS region, to create a bucket at Amazon S3.

```
access_key = ''
secret_key = ''
region = ''
```

At the terminal, you need to run:
```docker-compose up airflow-init```
And then:
```docker-compose up```

Now we have an Apache Airflow running at ```localhost:8080```. The credentials are airflow:airflow.

Go and trigger the dag **breweries_case**.
At the end of the execution, the data will be in the bucket ```bees-case-ingrid``` with the medallion architecture.

### Why this choices?
Apache Airflow:
- For the orchestration, the Apache Airflow is dinamic and easy to run in any devices. So for this part, we don't want any errors.

Python and Pandas:
- The chosen language was Python, and I preferred to work with pandas because that the amount of data was very small.

Cloud Services:
- I chose AWS Cloud because it is the cloud I'm most familiar.

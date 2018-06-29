# Spark on Kudu - Up and Running

Spark on Kudu is well integrated and full of great features. Get up and
running quickly with this small sample code project.

This code base developed and validated on CDH 5.7.5 with Kudu 1.1.0 release.

# Building

To build this sample code, run:

```sh
mvn clean package
```

# Running samples

After building the examples, run:

```sh
spark2-submit \
--class org.apache.gettingstartedkudu.examples.SparkKuduGettingStarted \
--master yarn-client \
--jars jars/kudu-client-1.1.0.jar,jars/kudu-spark_2.10-1.1.0.jar \
kudu-spark-client-1.0.jar

spark2-submit \
--class org.apache.gettingstartedkudu.examples.SparkKuduGettingStarted \
--master yarn-client \
--jars /opt/cloudera/parcels/CDH/jars/kudu-client-1.6.0-cdh5.14.0.jar,/opt/cloudera/parcels/CDH/jars/kudu-spark2_2.11-1.6.0-cdh5.14.0.jar \
kudu-spark-client-1.0.jar
```

We run in `yarn-client` mode simply so that we can see the output of the
`show()` calls in the terminal.

IF(T2<10, CONCATENATE("00",T2), IF(T2 < 100, "100"))

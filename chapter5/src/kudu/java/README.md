# Kudu Java Example

Run Kudu through the Java client.

# Running the java program

In the following example, we've opted to NOT create a main class reference in
the manifest file. This means that we need to use the `-cp` option together
with specifying the class we want to run.

However, this makes our jar file much smaller, since we also simply refer
to the client libraries installed in our installed version of Kudu. In this
example, we are using the libraries included in the Cloudera distribution
that ships with jar files.

```sh
java -cp "kudu-java-client-1.0.jar:/opt/cloudera/parcels/CDH/jars/kudu-client-1.5.0-cdh5.13.1.jar:/opt/cloudera/parcels/CDH/jars/kudu-client-tools-1.5.0-cdh5.13.1.jar" -DkuduMaster=mladen-secure-kudu-8,mladen-secure-kudu-9,mladen-secure-kudu-10 org.apache.gettingstartedkudu.examples.KuduJavaExample
```

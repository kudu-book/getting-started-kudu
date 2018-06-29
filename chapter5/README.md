# Chapter 5

Supplementary code to go along with Chapter 5 of Getting Started with Kudu O'Reilly book.


## Dataset

New York Taxi Cab trip data
http://xiaming.me/posts/2014/10/23/leveraging-open-data-to-understand-urban-lives/

blog taxi cab 1: http://chriswhong.com/data-visualization/taxitechblog1/
blog taxi cab 2: http://chriswhong.com/open-data/taxi-techblog-2-leaflet-d3-and-other-frontend-fun/



## C++ Kudu Sample Application

The Kudu team supplies a sample application fully documented in the build
tree under `src/kudu/client/samples`.

C++ API documentation may be found here: https://kudu.apache.org/cpp-client-api

The following describes a very brief overview of what's required as we build
the sample application that goes along with our book.

See the following page and download and install Kudu C++ client libraries
for your platform.

```
http://kudu.apache.org/docs/installation.html#build_cpp_client
```

Here are the rpm packages in particular you should install (example given
the version at the time of this writing):

```
sudo rpm -ivh kudu-client0-1.3.0+cdh5.11.0+0-1.cdh5.11.0.p0.13.el7.x86_64.rpm
sudo rpm -ivh kudu-client-devel-1.3.0+cdh5.11.0+0-1.cdh5.11.0.p0.13.el7.x86_64.rpm
```

Ensure you have at least version 2.8 of `cmake` installed

```
sudo yum -y install cmake
```

Go under `src/kudu/c` and run:

```
cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=debug
```

Where `CMAKE_BUILD_TYPE` value should be `debug` (executable with debug
symbols and more) or `release` for an optimized build.

If you've performed a custom build of Kudu, you can change into the
`<build_dir/src/kudu/client` directory and install the libraries into a
custom location such as `/tmp/client_alt_root`.

```
make install DESTDIR=/tmp/client_alt_root
```

Now run `cmake` as follows:

```
cmake -G "Unix Makefiles" -DkuduClient_DIR=/tmp/client_alt_root/usr/local/share/kuduClient/cmake -DCMAKE_BUILD_TYPE=debug
```

Build the executable now by running:

```
make
```

Run the sample application passing in a master tablet server location as an
input parameter.

```
./sample-kudu-app
```

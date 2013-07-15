# Building YCSB

To build YCSB, run:

    mvn clean package

# Running YCSB

Once `mvn clean package` succeeds, you can run `ycsb` command:

    ./bin/ycsb load basic workloads/workloada
    ./bin/ycsb run basic workloads/workloada

# Oracle NoSQL Database

Oracle NoSQL Database binding doesn't get built by default because there is no
Maven repository for it. To build the binding:

1. Download kv-ce-1.2.123.tar.gz from here:

    http://www.oracle.com/technetwork/database/nosqldb/downloads/index.html

2. Untar kv-ce-1.2.123.tar.gz and install kvclient-1.2.123.jar in your local
   maven repository:

    tar xfvz kv-ce-1.2.123.tar.gz
    mvn install:install-file -Dfile=kv-1.2.123/lib/kvclient-1.2.123.jar \
        -DgroupId=com.oracle -DartifactId=kvclient -Dversion=1.2.123
        -Dpackaging=jar

3. Uncomment `<module>nosqldb</module>` and run `mvn clean package`.

# VoltDB NewSQL Database

VoltDB client libraries are not in the Maven Central Repository. So they need to be installed locally before the workloads can be run.

1. Download the appropriate version of VoltDB for the Operating System being used from VoltDB Download site:

    http://voltdb.com/community/downloads.php

2. Install VoltDB library in your local maven repository. Replace the path to voltdb-3.2.1.jar with the path that is appropriate on your machine:

    tar xfvz kv-ce-1.2.123.tar.gz
    mvn install:install-file -Dfile=~/voltdb-3.2.1/voltdb/voltdb-3.2.1.jar \
        -DgroupId=org.voltdb -DartifactId=voltdbclient -Dversion=3.2.1
        -Dpackaging=jar

5. run `mvn clean package`.

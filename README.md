# iashdfs
R based HDFS functions for directing the output of any standard print based functions into and out of an HDFS file. These functions do not need extra memory for caching, staging, or otherwise converting values. These functions are simple, fast, and memory efficient.

Pre-requisites for hdfs commands (NOTE: not used by hdfsWeb commands)
* Environment variable
  * HADOOP_CMD = location of the hadoop command line executable such as when running 'hadoop fs -ls /anHdfsFolder'
* Installation
  * library(devtools)
  * install_github('tcederquist/iashdfs')
* Run
  * library(iashdfs)

# hdfsWrite(output, hdfsFilename, append=FALSE)

* output = any command that outputs to the console or string
  * fwrite(mydatatable)
  * noquote(mystring)
* hdfsFilename = complete filename including the hdfs path
* append = default of false
  * False = delete the hdfs file before writing
  * True = append to the existing file
* Example

     hdfsWrite(fwrite(mydatatable), "/user/tcederquist/mysample.csv")

# hdfsReadP(hdfsFilename)
Creates a string that can be used by any pipe friendly function to stream the contents of the html file.

* hdfsFilename = path the file to read
* Example

     dat.df3<-fread(hdfsReadP("/user/tcederquist/tim_pop_14_5"),sep=",", header=TRUE, showProgress=F)
     
# hdfsDelete(hdfsFilename)
Delete removes the specified file from the supplied HDFS folder

* hdfsFilename = Full path to the hdfs file to remove
* Example

     hdfsDelete('/user/tcederquist/mysample.csv')
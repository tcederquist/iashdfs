% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/iashdfs.r
\name{hdfsWebRead}
\alias{hdfsWebRead}
\title{hdfsWebRead function}
\usage{
hdfsWebRead(host, hdfsFilename, sslVerify = TRUE)
}
\arguments{
\item{host}{Full host and port location for the hdfsFS service. Typically on port 14000, 50470, or 50070}

\item{hdfsFilename}{Full path to the file to read such as /user/myusername/test.csv}

\item{sslVerify}{Optional boolean instruction if the call should expect a valid ssl certificate. TRUE is default, use FALSE for a self signed certificate}
}
\value{
Return the file contents in a string with control characters for newlines
}
\description{
Read the contents of the supplied file
}
\examples{
myfile <- hdfsWebRead('https://my.server.com:14000', '/user/me/test.csv')
writeLines(myfile)
}

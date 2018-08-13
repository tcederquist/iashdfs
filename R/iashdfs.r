#' hdfsDelete function
#'
#' Delete removes the specified file from the supplied HDFS folder
#' System must have an environment variable defined for the location of the HADOOP command named HADOOP_CMD
#' @param hdfsFilename Full path to the hdfs file to remove.
#' @export
#' @examples
#' hdfsDelete('/user/tcederquist/mysample.csv') 
hdfsDelete <- function(hdfsFilename) {
    hcmd<-Sys.getenv('HADOOP_CMD')
    del <-"fs -rm"
    ret=system2(hcmd, paste(del, hdfsFilename),wait=TRUE, stdout=TRUE)
    ret
}

#' hdfsWrite function
#'
#' Writes the output from the supplied string or function directly to an hdfs file
#' System must have an environment variable defined for the location of the HADOOP command named HADOOP_CMD
#' @param output Any string or a function that outputs text directly to the console. This text will be redirected to the hdfs file
#' @param hdfsFilename Full path to the hdfs file to write the contents
#' @param append Default is FALSE and will delete the file before writing the contents. TRUE will append to the existing file
#' @export
#' @examples
#' hdfsWrite(fwrite(mydatatable), "/user/tcederquist/mysample.csv")
#' hdfsWrite(fwrite(mydatatable[100:200,]), "/user/tcederquist/mysample.csv", append=TRUE)
hdfsWrite <- function(output, hdfsFilename, append=FALSE) {
    hcmd<-Sys.getenv('HADOOP_CMD')
    apnd<-"fs -appendToFile -"
    if (!append) {
        hdfsDelete(hdfsFilename)
    }
    p<-pipe(paste(hcmd, apnd, hdfsFilename), 'w')
    capture.output(output, file=p)
    close(p)
}

#' hdfsReadP
#'
#' Creates a string that can be used by any pipe friendly function to stream the contents of the html file.
#' System must have an environment variable defined for the location of the HADOOP command named HADOOP_CMD
#' @param hdfsFilename Full path to the hdfs file
#' @return Text used to create a stream from the hdfs file
#' @export
#' @examples
#' dat.df3<-fread(hdfsReadP("/user/tcederquist/tim_pop_comm_14_5"),sep=",", header=TRUE, showProgress=F)
hdfsReadP <- function(hdfsFilename) {
    hcmd<-Sys.getenv('HADOOP_CMD')
    hcat<-"-cat"
    paste(Sys.getenv('HADOOP_CMD'), "fs", hcat, hdfsFilename)
}

#' ias.hdfsWebBuildURL
#'
#' Build a url with basic checks for leading and trailing / marks. Designed for assembling hdfsFS type rest request uris
#' @param host Host and port of the url 'https://my.server.com:14000
#' @param func leading section of the uri '/web/hdfs/v1' for example
#' @param path location segment of the hdfsFS request
#' @param op Operation parameter to perform on the requested location
#' @return string of the fully formed url
#' @examples
#' ias.hdfsWebBuildURL("https://my.server.com:14000", '/webhdfs/v1', "/user/me", 'LISTSTATUS')
#' ias.hdfsWebBuildURL("https://my.server.com:14000/", '/webhdfs/v1', "/user/me", 'LISTSTATUS')
ias.hdfsWebBuildURL <- function(host, func, path, op) {
    workingURL <- host
    if (stringr::str_sub(workingURL, -1, -1) == '/') {
        workingURL <- paste(workingURL, substr(func, 2, nchar(func)), sep='')
    } else {
        workingURL <- paste(workingURL, func, sep='')
    }
    if (substr(path, 1, 1) != '/') {
        workingURL <- paste(workingURL, '/', sep='')
    }
    paste(workingURL, path, "?op=", op, sep='')
}
        
#' hdfsWebLS function
#'
#' List the contents of the supplied path and return a data table of files
#' @param host Full host and port location for the hdfsFS service. Typically on port 14000, 50470, or 50070
#' @param path Full path to the folder to query such as /user/myusername
#' @param sslVerify Optional boolean instruction if the call should expect a valid ssl certificate. TRUE is default, use FALSE for a self signed certificate
#' @return data.frame with the folder information
#' @export
#' @examples
#' listing <- hdfsWebLS('https://my.server.com:14000', '/user/me')
hdfsWebLS <- function(host, path, sslVerify=TRUE) {
    url <- ias.hdfsWebBuildURL(host, '/webhdfs/v1', path, 'LISTSTATUS')
    rawListing <- RCurl::getURL(url, userpwd=":", httpauth = 4, ssl.verifypeer=sslVerify)
    jsonListing <- jsonlite::fromJSON(rawListing)
    as.data.frame(jsonListing$FileStatuses$FileStatus)
}

#' hdfsWebRead function
#'
#' Read the contents of the supplied file
#' @param host Full host and port location for the hdfsFS service. Typically on port 14000, 50470, or 50070
#' @param hdfsFilename Full path to the file to read such as /user/myusername/test.csv
#' @param sslVerify Optional boolean instruction if the call should expect a valid ssl certificate. TRUE is default, use FALSE for a self signed certificate
#' @return Return the file contents in a string with control characters for newlines
#' @export
#' @examples
#' myfile <- hdfsWebRead('https://my.server.com:14000', '/user/me/test.csv')
#' writeLines(myfile)
hdfsWebRead <- function(host, hdfsFilename, sslVerify=TRUE) {
    url <- ias.hdfsWebBuildURL(host, '/webhdfs/v1', hdfsFilename, 'OPEN')
    RCurl::getURL(url, userpwd=":", httpauth = 4, ssl.verifypeer=sslVerify)
}

#' hdfsWebDelete function
#'
#' Delete the specified file
#' @param host Full host and port location for the hdfsFS service. Typically on port 14000, 50470, or 50070
#' @param hdfsFilename Full path to the file to read such as /user/myusername/test.csv
#' @param sslVerify Optional boolean instruction if the call should expect a valid ssl certificate. TRUE is default, use FALSE for a self signed certificate
#' @return Returns boolean response from hdfsFS
#' @export
#' @examples
#' hdfsWebDelete("https://my.server.com:14000", "/user/me/tst2.txt", sslVerify=FALSE)
hdfsWebDelete <- function(host, hdfsFilename, sslVerify=TRUE, verbose=FALSE) {
    url <- ias.hdfsWebBuildURL(host, '/webhdfs/v1', hdfsFilename, 'DELETE')
    if (verbose) print(paste("Target url", url))
    
    h = RCurl::basicTextGatherer()
    resp = RCurl::basicTextGatherer()
    RCurl::httpDELETE(url, userpwd=":", httpauth = 4, ssl.verifypeer=sslVerify, headerFunction=h$update, writefunction=resp$update, followlocation=FALSE)
    if (verbose) print(paste("Call return raw response = ",resp$value()))
    response <- tryCatch({
            jsonlite::fromJSON(resp$value())$boolean
        },
        error=function(cond) {
            resp$value()
        },
        warning=function(cond) {
            cond
        },
        finally={}
    )
    return(response)
}

#' hdfsWebWrite function
#'
#' Write to the contents of the supplied file - creates and overwrites any existing file
#' @param contents String or Steam of data to write into the file
#' @param host Full host and port location for the hdfsFS service. Typically on port 14000, 50470, or 50070
#' @param hdfsFilename Full path to the file to read such as /user/myusername/test.csv
#' @param sslVerify Optional boolean instruction if the call should expect a valid ssl certificate. TRUE is default, use FALSE for a self signed certificate
#' @param verbose Detailed output of the hdfsFS responses
#' @return Error messages or empty if successful
#' @export
#' @examples
#' myfile <- hdfsWebRead('https://my.server.com:14000', '/user/me/test.csv')
#' hdfsWebWrite("Hello there tester2!", "https://my.server.com:14000", "/user/me/test.csv")
hdfsWebWrite <- function(contents, host, hdfsFilename, append=FALSE, sslVerify=TRUE, verbose=FALSE) {
    url <- ias.hdfsWebBuildURL(host, '/webhdfs/v1', hdfsFilename, 'CREATE')  #debug for error message response
    if (verbose) print(paste("Target url", url))

    cur.conf <- httr::config()
    httr::set_config(httr::config(ssl_verifypeer=sslVerify, followlocation=FALSE), override=TRUE)
    response<-httr::PUT(url, httr::authenticate(":", "", type="gssnegotiate"))
    location <- httr::headers(response)$location
    if (verbose) print(paste("Response code:", response$status, "   location:", location, "Body:", httr::content(response, 'text', encoding='UTF-8')))

    if (response$status >= 400) {
        return(
            tryCatch({
                    jsonlite::fromJSON(httr::content(response, 'test', encoding='UTF-8'))$RemoteException$message
                },
                error=function(cond) {
                    content(response, 'test', encoding='UTF-8')
                },
                warning=function(cond) {
                    cond
                },
                finally={
                    httr::set_config(cur.conf)
                }
            )
        )
    }

    # NOTE: Current version of libCurl will not stream large data correctly and results in unusable performance for large files.
    #       Workaround is to write a temp file and push that through libCurl which does stream and chunk efficiently
    httr::set_config(httr::config(ssl_verifypeer=sslVerify, postfieldsize_large=TRUE), override=TRUE )
    tmpFname <- tempfile(pattern="cache_ias")
    if (verbose) print(paste("Temp filename:", tmpFname))
    capture.output(contents, file=tmpFname)
    response2 <- httr::PUT(location, body=httr::upload_file(tmpFname, type=""), httr::add_headers('content-type' = "application/octet-stream"), httr::authenticate(":", "", type="gssnegotiate"))
    file.remove(tmpFname)
    if (verbose) print(paste("Tmp deleted, response for upload:", response2$status, "Body:", httr::content(response, 'text', encoding='UTF-8')))
    httr::set_config(cur.conf)
    return(httr::content(response2, 'text', encoding='UTF-8'))
}

#' impalaQuickLoad function
#'
#' Write to the contents of the supplied file - creates and overwrites any existing file
#' @param contents String or Steam of data to write into the file
#' @param host Full host and port location for the hdfsFS service. Typically on port 14000, 50470, or 50070
#' @param hdfsFilename Full path to the file to read such as /user/myusername/test.csv
#' @param sslVerify Optional boolean instruction if the call should expect a valid ssl certificate. TRUE is default, use FALSE for a self signed certificate
#' @param verbose Detailed output of the hdfsFS responses
#' @return Error messages or empty if successful
#' @export
#' @examples
#' myfile <- hdfsWebRead('https://my.server.com:14000', '/user/me/test.csv')
#' hdfsWebWrite("Hello there tester2!", "https://my.server.com:14000", "/user/me/test.csv")
impalaQuickLoad <- function(srcData, dsn='', table='', schema='', hdfsFSHost='', folder='/data/tmp', comment='', sslVerify=TRUE, verbose=FALSE) {
    #--------Upload and make a table in impala
    cols <- lapply(names(srcData), function(x) { paste(trimws(x), "string") } )
    fullTblNm <- paste(schema, '.', table, sep='')
    fullFilePath <- paste(folder, '/', table, '.csv', sep='')

    if (verbose) print(paste("Loading data to HDFS, temp file is:", fullFilePath))
    hdfsWebWrite(fwrite(srcData, col.names=FALSE), hdfsFSHost, fullFilePath, sslVerify=sslVerify, verbose=verbose)

    #---- sql commands
    stmtDrop <- paste('drop table if exists', fullTblNm, ';')
    stmtCreate <- paste(
                        "CREATE EXTERNAL TABLE", fullTblNm, "(", paste(cols, collapse=','), ")",
                        paste("COMMENT '", comment, "'", sep=''),
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;"
                        )
    stmtLoad <- paste("load data inpath '", fullFilePath, "' overwrite into table ", fullTblNm, ";", sep='')

    if (verbose) print(paste("Drop statement:", stmtDrop))
    if (verbose) print(paste("Create statement:", stmtCreate))
    if (verbose) print(paste("Load statement:", stmtLoad))

    db <- RODBC::odbcConnect(dsn=dsn, readOnlyOptimize=TRUE)
    #RODBC::sqlQuery(db, paste('truncate table if exists default.tcederquist_tmp5;')
    RODBC::sqlQuery(db, stmtDrop)
    RODBC::sqlQuery(db, stmtCreate)
    RODBC::sqlQuery(db, stmtLoad)
    RODBC::odbcClose(db)

    if (verbose) print(paste("Data from data.table:", deparse(substitute(srcData)), "loaded into table:", fullTblNm))
}
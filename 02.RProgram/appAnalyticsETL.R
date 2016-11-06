#### Title --------------------------------
# title: "Mobile App Analytics ETL"
# author: "Pradeep Menon"
# date: "04/11/2016"

### Load Util Files -------------------
# Load utilization files, packages and data
# We will load utilities file that contains functions that will be used for
# various actions and processing.


utilFileName <- paste0(getwd(), "/02.RProgram/utilFunctions.R")

# Extract the functions from the util file.

source(utilFileName)

### Load Packages -------------------
# The list of packages that needs to used for data processing.
packages <- c("dplyr",
              "lubridate",
              "ggplot2",
              "rvest",
              "jsonlite",
              "data.table",
              "reshape2")

# Calls the function that installs the required package if not installed and
# loads the packages.

loadPackage(packages = packages)

### Load Data --------------------
# Create the path where the raw data resides.

dataFilePath <- paste0(getwd(), "/01.Data")

# Construct the full path to the filename.

dataFileName <- paste0(dataFilePath, "/analytics.logs.28-Oct.json")

appData <- fromJSON(dataFileName) # load data



#### Column cleansing ####
# We will only use columns that have some analytic value and can be used for
# analysis.
# The columns below, don't have much analytic value so we can drop then.
dropCols <- names(appData) %in% c("etag", "firstLaunch", "appId")

# Use negation to drop the columns from the data set.
appData <- appData[!dropCols]

head(appData) # inspecting firs few rows
str(appData) # inspecting structure



#### Engineering Date Time -----------------------------------------
appDataTfm <-
    appData %>%
    mutate(
        transactionDt = as.Date(partitionKey),
        type = as.factor(type),
        value = as.factor(value),
        screenName = as.factor(screenName),
        platform = as.factor(platform),
        countryCode = as.factor(countryCode),
        languageCode = as.factor(languageCode),
        transactionTime = as.POSIXct(timestamp, origin = "1970-01-01"),
        transactionDtTime = lubridate::ymd_hms(paste0(
            partitionKey, " ",
            strftime(as.POSIXct(timestamp,
                                origin = "1970-01-01"),
                     format = "%H:%M:%S")
        ))
    ) %>%
    mutate(
        transactionHr = hour(transactionTime),
        transactionMin = minute(transactionTime)
    )

### Remove unwanted columns -----------------
dropColsFT <- names(appDataTfm) %in% c("partitionKey", "timestamp",
                                       "transactionTime")
appDataTfm <- appDataTfm[!dropColsFT]

#### Sorting data frame ---------------------------
## We will first arrange the columns and sort them by memberNumber
## and transactionTime.
## This is because memberNumber and transactionDtTime form a unique transaction.
appDataTfm <-
    appDataTfm %>%
    select(
        memberNumber,
        transactionDtTime,
        transactionDt,
        transactionHr,
        transactionMin,
        action,
        name,
        type:appVersion,
        customerNumber:languageCode
    ) %>%
    arrange(memberNumber, transactionDtTime)

#### Creating session master ---------------------------
## In this section we create a dataset called as sessionMaster.
## Session is defined as the series of transactions that occur continiously
## such that the time difference between a given transaction and the previous one is
## less than 10mins.
## Session master is the first transaction that occurs in a given set.
## The following is the strategy we use to create a Session master:
##      1.  Sort and group each transaction by member, transaction date and hour.
##      2.  Find the minutes difference between a given transaction and the last one.
##      3.  If the difference is greater than 10 then it is considered to be new
##          transaction.

sessionMaster <-
    appDataTfm %>%
    select(
        memberNumber,
        transactionDtTime,
        transactionDt,
        transactionHr,
        transactionMin,
        action,
        name
    ) %>%
    arrange(memberNumber, transactionDt, transactionHr, transactionMin) %>%
    group_by(memberNumber, transactionDt, transactionHr) %>%
    mutate(lastmin = lag(transactionMin),
           diffmin = ifelse(is.na(lag(transactionMin) - transactionMin), 0,
                            (transactionMin - lag(transactionMin)))) %>%
    mutate(isSessionMast = ifelse((is.na(lastmin)  &
                                       diffmin == 0), 1,
                                  ifelse(diffmin > 10, 1, 0))) %>%
    filter(isSessionMast == 1) %>%
    select(memberNumber,
           transactionDtTime,
           transactionDt,
           transactionHr,
           transactionMin) %>%
    as.data.frame()

#### Tagging session ids to transactions ---------------------------
## In this section, we tag each transaction with a session id.
## Following is the strategy we will employ:
##      1.  Create sequence of session ids for each row in session master.
##      2.  Join app data with session master based on member, transaction time
##          details.
##      3.  Step 2 will result in data frame that has all the masters tagged
##          with session ids.
##          The child sessions will be NA.
##      4.  Use a function replaceNA that will replace all the child sessions
##          with the previous session ids i.e. id of the parent session.

# Creating session id for master
sessionMaster$sessionId <-  seq.int(nrow(sessionMaster))
str(sessionMaster)
head(sessionMaster)

# Tagging session ids to transactions
appDataTfm <-
    # Join with transaction and create sessionIds
    
    left_join(
        appDataTfm,
        sessionMaster,
        by = c(
            "memberNumber",
            "transactionDtTime",
            "transactionDt",
            "transactionHr",
            "transactionMin"
        ),
        copy = FALSE
    ) %>%
    select(
        memberNumber,
        transactionDtTime,
        sessionId,
        transactionDt,
        transactionHr,
        transactionMin,
        action,
        name,
        type:appVersion,
        customerNumber:languageCode
    ) %>%
    # replace all the child sessions with the previous session ids i.e. id of the
    # parent session.
    
    mutate(sessionId = replaceNA(sessionId)) %>%
    select(
        memberNumber,
        transactionDtTime,
        sessionId,
        transactionDt,
        transactionHr,
        transactionMin,
        action,
        name,
        type:appVersion,
        customerNumber:languageCode
    ) %>%
    as.data.frame()


#### Reshaping Data -----------------------
## Here we will reshape the data such that interaction for each screen is recorded.
## This operation will result in new data frame.
## The strategy we will use is as follows:
##      1.  Select key dimensions for which we want to do analysis.
##      2.  Based on those dimensions, pivot the count of transactions based
##          on the screen name.
##      3.  The resultant will have columns(features) for each screen with the
##          count of interaction for that screen.


str(appDataTfm)
appDataPivot <-
    appDataTfm %>%
    select(
        memberNumber,
        sessionId,
        transactionDt,
        transactionHr,
        transactionMin,
        screenName,
        longitude,
        latitude
    ) %>%
    group_by(
        memberNumber,
        sessionId,
        transactionDt,
        transactionHr,
        transactionMin,
        screenName,
        longitude,
        latitude
    ) %>%
    summarise(count = n()) %>%
    # pivot the data
    
    reshape2::dcast(
        memberNumber + sessionId + transactionDt + transactionHr +
            transactionMin + longitude + latitude ~ screenName
    )
# All the values with NULL values need to be substituted with 0

appDataPivot[is.na(appDataPivot)] <- 0

# Calculate the total number of transactions for each row by adding a total column

appDataPivot <-
    appDataPivot %>%
    mutate(total = rowSums(appDataPivot[, c(8:ncol(appDataPivot))]))

#### Export Data -----------------
## In this section we will export the dataframes into csv files.
## We will export following dataframes:
##  1.  appDataTfm that contains the raw data with feature engineered columns.
##  2.  appDataPivot that containts pivoted data.
## These dataframes will be further used in Power BI for visualization.
exportPath <- paste0(getwd(), "/01.Data")
write.table(
    appDataTfm,
    paste0(exportPath, "/appDataTfm.csv"),
    sep = ",",
    row.names = F
)

write.table(
    appDataPivot,
    paste0(exportPath, "/appDataPivot.csv"),
    sep = ",",
    row.names = F
)

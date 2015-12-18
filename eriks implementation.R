
library(BTYD)
library(lubridate)
library(data.table)
library(reshape2)
library(quantmod)
library(RMySQL)
library(dplyr)

#Pareto/NBD,
# Frequency, recency, time active
setwd('/Users/transferwise/LTR-forecast')
db <- fread('Data/ltr_data.csv', sep=',', na.strings='NULL', data.table=F)

query <- "select id_user
, date_request_transferred
, fee_amount
, fee_amount_payment 
from report_req_metadata 
where date_request_transferred < '2014-01-01';"

con <- dbConnect(MySQL(),
                 user = .env$usr,
                 password = .env$pw,
                 host = '127.0.0.1',
                 port = 3307,
                 dbname='obfuscated')

fees <- 109:132
measures <- c('id_user', 'first_successful_payment')
include <- c(measures, names(db)[fees])

db <- dbGetQuery(con, query)
elog <- db %>%
  mutate(date_request_transferred = as.Date(date_request_transferred, '%Y-%m-%d')) %>% 
  select(id_user, date_request_transferred, fee_amount) %>% 
  setnames(names(.), c('cust', 'date', 'sales')) %>% 
  group_by(cust, date) %>% 
  summarise(sales = sum(sales)) %>% 
  mutate(key = paste(cust, date))

end.of.cal.period <- as.Date('2013-08-01')
elog.cal <- elog %>% filter(date <= end.of.cal.period)

first_transfers <- elog.cal %>% 
  group_by(cust) %>% 
  summarize(date = min(date)) %>% 
  mutate(key = paste(cust, date))

first_transfers_index <- elog.cal$key %in% first_transfers$key %>% which
clean.elog <- elog.cal[-first_transfers_index,]
spend.cbt <- dc.CreateSpendCBT(clean.elog)
tot.cbt <- dc.CreateSpendCBT(elog)
cal.cbt <- dc.MergeCustomers(tot.cbt, spend.cbt)
dc.SplitUpElogForRepeatTrans


ncol(spend.CBT)
?dim
ltr$first_successful_payment <- as.Date(ltr$first_successful_payment, '%Y-%m-%d')
ltr$first_successful_payment <- floor_date(ltr$first_successful_payment, unit='month')
ltr <- melt(ltr, id.vars=measures)
ltr$variable <- as.character(ltr$variable)
ltr$variable <- as.numeric(substr(ltr$variable, 12, 13))
ltr$value <- as.numeric(ltr$value)
ltr$date <- floor_date(as.Date(ltr$first_successful_payment + (ltr$variable -1 ) * 31), unit='month')
ltr <- ltr[c('id_user', 'date', 'value')]
names(ltr) <- c('cust', 'date', 'sales')

elog = ltr
elog <- elog[which(!is.na(elog$sales)),]
elog <- elog[which(elog$sales!=0),]
s <- sample(nrow(elog), 10000)
elog <- elog[s,]

end.of.cal.period <- as.Date("2014-01-01")
elog.cal <- elog[which(elog$date <= end.of.cal.period), ]
split.data <- dc.SplitUpElogForRepeatTrans(elog.cal)
clean.elog <- split.data$repeat.trans.elog;
freq.cbt <- dc.CreateFreqCBT(clean.elog);
freq.cbt[1:3,1:5]
tot.cbt <- dc.CreateFreqCBT(elog)
cal.cbt <- dc.MergeCustomers(tot.cbt, freq.cbt)
birth.periods <- split.data$cust.data$birth.per
last.dates <- split.data$cust.data$last.date
cal.cbs.dates <- data.frame(birth.periods, last.dates,
                            end.of.cal.period)
cal.cbs <- dc.BuildCBSFromCBTAndDates(cal.cbt, cal.cbs.dates,
                                      per="month")


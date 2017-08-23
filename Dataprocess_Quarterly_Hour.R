
#Part 0------------------------------------------------------------------------------
library(RMySQL)
library(zoo)

setwd("C:/Users/FengMac/Desktop/workspace/quarterly")

options(digits = 15) # setting the length of numbers

      #------------ how much quantity you want to consider-----------------------

quantCont <- 20      

hour <- 0


while(hour <= 9){

      #-------------PRODUCT (the time period to consider)-------------------------

Timeperiod <- paste("2016-10-13 0", hour, ":15:00",  sep = "")

product <- as.numeric(as.POSIXct(Timeperiod))*1000

      #---------------------------Start time----------------------------------------

#Timestart <-  "16:00" # the time (00:00 - 23:00) of yesterday from which to cosnider

#intervalstart <- as.numeric(as.POSIXct(paste(as.character(as.Date(substr(Timeperiod, 1, 10)) -1), Timestart)))*1000 


Timeahead <- 1 #  ( 1 - 24) # how long (2 - 10 hours) before the product you want to consider

intervalstart <- as.numeric(as.POSIXct(as.numeric(as.POSIXct(Timeperiod,  format="%Y-%m-%d %H:%M:%S")) - (Timeahead * 3600), origin = "1970-01-01"))*1000



#Part 1------------------ Database Connection----------------------------------------

conn_orderbook <- dbConnect(MySQL(), dbname = "orderbook_history", username="readonly", 
                  password="2NIsW",
                  host="sql.test.de",port=3306)

conn_comxerv <- dbConnect(MySQL(), dbname = "comxerv", username="readonly", 
                            password="2NIsW",
                            host="sql.test.de",port=3306)


#Part 2--------------------Getting data from database--------------------------------


db_contactID <- dbGetQuery(conn_comxerv, paste("SELECT *, from_unixtime(deliveryStart/1000) as FmDate,", 
                           "from_unixtime(deliveryEnd/1000) as ToDate FROM comxerv.contracts",
                           "WHERE deliveryStart =", product, "and duration = 0.25 ORDER BY deliveryStart;"))


contractID <- db_contactID$id[1]   # setting the contract ID

      #-----------------------------------------------------------------------------

db_contratID_BUY <- dbGetQuery(conn_orderbook, paste("SELECT *, from_unixtime(fromDate/1000) as FmDate,",
                                                     "from_unixtime(toDate/1000) as ToDate", 
                                                     "FROM orderbook_history.comxerv", 
                                                     "WHERE contractId =", contractID , "and side = 'BUY';"))

db_contratID_SELL <- dbGetQuery(conn_orderbook, paste("SELECT *, from_unixtime(fromDate/1000) as FmDate,",
                                                     "from_unixtime(toDate/1000) as ToDate", 
                                                     "FROM orderbook_history.comxerv", 
                                                     "WHERE contractId =", contractID , "and side = 'SELL';"))

db_contactID_trade <- dbGetQuery(conn_comxerv, paste("SELECT *, from_unixtime(executionTime/1000) as exeTime", 
                                                     "FROM comxerv.trades WHERE contractId =", contractID, ";"))

db_contactID_OURtrade <- dbGetQuery(conn_comxerv, paste("SELECT *, from_unixtime(executionTime/1000) as exeTime", 
                                                     "FROM comxerv.clienttrades WHERE contractId =", contractID, ";"))

    #--------------------------------------------------------------------------------

data_buy <- db_contratID_BUY[,c("quantity", "price", "fromDate", "toDate")]

data_buy <- data_buy[order(-data_buy$price),]     # sorting by price decreasing

data_buy[data_buy$fromDate == data_buy$toDate, "toDate"] <- data_buy[data_buy$fromDate == data_buy$toDate, 
                                                                     "toDate"] + 1000 # exclude the row with 0 time inteval


data_sell <- db_contratID_SELL[,c("quantity", "price", "fromDate", "toDate")]

data_sell <- data_sell[order(data_sell$price),]     # sorting by price increasing

data_sell[data_sell$fromDate == data_sell$toDate, "toDate"] <- data_sell[data_sell$fromDate == data_sell$toDate, 
                                                                     "toDate"] + 1000 # exclude the row with 0 time inteval


data_trade <- db_contactID_trade[db_contactID_trade$executionTime >= intervalstart, 
                                 c("executionTime", "quantity", "price")] 
   
data_OURtrade <- db_contactID_OURtrade[db_contactID_OURtrade$executionTime >= intervalstart, 
                                       c("executionTime", "quantity", "price", "side")] 

 #--------------------------------------------------------------------------------

dbDisconnect(conn_orderbook) # close the connection
dbDisconnect(conn_comxerv)


#Part 3-----------------Preprocessing the data from database--------------------------------------------------

data_buy[,"quantity"] <- data_buy[, "quantity"]/1000

data_buy[,"price"] <- data_buy[, "price"]/100

interval_buy <- c(data_buy$fromDate, data_buy$toDate, intervalstart) 

interval_buy <- interval_buy[interval_buy >= intervalstart]

interval_buy <- unique(interval_buy)

interval_buy <- sort(interval_buy)


      #---------------------------------------BUY/SELL separation-----------------------------------

data_sell[,"quantity"] <- data_sell[, "quantity"]/1000

data_sell[,"price"] <- data_sell[, "price"]/100

interval_sell <- c(data_sell$fromDate, data_sell$toDate, intervalstart) 

interval_sell <- interval_sell[interval_sell >= intervalstart]

interval_sell <- unique(interval_sell)

interval_sell <- sort(interval_sell)

    #------------------------------------Trade separation--------------------------------------------


data_trade[,"quantity"] <- data_trade[, "quantity"]/1000

data_trade[,"price"] <- data_trade[, "price"]/100


data_OURtrade[,"quantity"] <- data_OURtrade[, "quantity"]/1000

data_OURtrade[,"price"] <- data_OURtrade[, "price"]/100


data_OURtrade_buy <- data_OURtrade[data_OURtrade$side == "BUY", c("executionTime", "quantity", "price", "side")]

data_OURtrade_sell <- data_OURtrade[data_OURtrade$side == "SELL", c("executionTime", "quantity", "price", "side")]

#Part 4--------------Calculating the average price---------------------------------------------------

i <- 1 #  initialize the 1st interval
Apricesque_buy <- NULL # average price sequence

while(i < length(interval_buy)){
  quant <- 0
  data_buy_pos <- 1
  SumQP <- 0 # quantity * price
  Aprice <- 0  # average price
  
  while ((quant < quantCont) && data_buy_pos <= nrow(data_buy)) {
    
    if((data_buy[data_buy_pos,3] <= interval_buy[i]) && (data_buy[data_buy_pos,4] >= interval_buy[i+1])) {
      
        if ((quant + data_buy[data_buy_pos, 1]) > quantCont) {
        SumQP <- (SumQP + ((quantCont - quant) * data_buy[data_buy_pos, 2]))
        quant <- quantCont
        } 
       
        else {
        quant <- (quant + data_buy[data_buy_pos, 1])
        SumQP <- (SumQP + (data_buy[data_buy_pos, 1] * data_buy[data_buy_pos, 2]))
        data_buy_pos <- (data_buy_pos + 1)
      }
      
    } 
    else  data_buy_pos <- (data_buy_pos + 1);
  }
  
  Aprice <- SumQP/quantCont
  Apricesque_buy <- append(Apricesque_buy, Aprice)
  i <- i+1
}


      #---------------------------------------BUY/SELL separation-----------------------------------


i <- 1 #  initialize the 1st interval
Apricesque_sell <- NULL # average price sequence

while(i < length(interval_sell)){
  quant <- 0
  data_sell_pos <- 1
  SumQP <- 0 # quantity * price
  Aprice <- 0  # average price
  
  while ((quant < quantCont) && data_sell_pos <= nrow(data_sell)) {
    
    if((data_sell[data_sell_pos,3] <= interval_sell[i]) && (data_sell[data_sell_pos,4] >= interval_sell[i+1])) {
      
      if ((quant + data_sell[data_sell_pos, 1]) > quantCont) {
        SumQP <- (SumQP + ((quantCont - quant) * data_sell[data_sell_pos, 2]))
        quant <- quantCont
      } 
      
      else {
        quant <- (quant + data_sell[data_sell_pos, 1])
        SumQP <- (SumQP + (data_sell[data_sell_pos, 1] * data_sell[data_sell_pos, 2]))
        data_sell_pos <- (data_sell_pos + 1)
      }
      
    } 
    else  data_sell_pos <- (data_sell_pos + 1);
  }
  
  Aprice <- SumQP/quantCont
  Apricesque_sell <- append(Apricesque_sell, Aprice)
  i <- i+1
}



#Part 5--------------------Drawing the figure----------------------------------------------------------
 
interval_buy  <- interval_buy[-length(interval_buy)] # remove the last value
interval_sell <- interval_sell[-length(interval_sell)]

tmp <- as.numeric(substr(Timeperiod, 15,16))+15

minprice <- min(Apricesque_sell,Apricesque_buy, data_trade$price)
maxprice <- max(Apricesque_sell,Apricesque_buy, data_trade$price)

mintime <- min(interval_buy, interval_sell, data_trade$executionTime)/1000 
maxtime <- max(interval_buy, interval_sell, data_trade$executionTime)/1000

figurename <-paste(substr(Timeperiod, 1,13), substr(Timeperiod, 15,16), "-", substr(Timeperiod, 12,13), tmp, " (",quantCont,"MW).png", sep = "")

file.remove(figurename)

png(file = figurename, width = 1000, height = 700, res = 90)

plot(as.POSIXct(interval_buy/1000, origin = "1970-01-01"), Apricesque_buy, ann=FALSE, pch = 20, type ="o", 
     col = "red", ylim = c(minprice, maxprice) )

lines(as.POSIXct(interval_sell/1000, origin = "1970-01-01"), Apricesque_sell, pch = 20, type ="o", col= "green")

lines(as.POSIXct(data_trade[,c("executionTime")]/1000, origin = "1970-01-01"), data_trade[, c("price")], pch = 20, type ="o", lty = 2, col= "blue")

lines(as.POSIXct(data_OURtrade_buy[,c("executionTime")]/1000, origin = "1970-01-01"), data_OURtrade_buy[, c("price")], pch = 12, type ="p", col= "red")

lines(as.POSIXct(data_OURtrade_sell[,c("executionTime")]/1000, origin = "1970-01-01"), data_OURtrade_sell[, c("price")], pch = 12, type ="p", col= "green")

title(main = paste(substr(Timeperiod, 1,16), "-", substr(Timeperiod, 12,13), ":", tmp, " (",quantCont,"MW)", sep = ""))

dev.off()

hour <- hour +1

file.show(figurename)
}
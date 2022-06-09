library(DBI)
library(magrittr)
library(googlesheets4)

con2 <- dbConnect(odbc::odbc(), "reproreplica", timeout = 10)

result <- dbGetQuery(con2,"


")


sheet_write(result,ss="1bcAoyy0DUBzNuYcyGmZTCyZ2uGqPpgW7kXBmOOxlmhM",sheet="DADOS")
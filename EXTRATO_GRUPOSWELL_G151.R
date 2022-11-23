## PEDIDOS POR ORIGEM 
## Bibliotecas necessárias

library(DBI)
library(tidyverse)
library(gmailr)
library(xlsx)



## conexão com banco replica 

con2 <- dbConnect(odbc::odbc(), "reproreplica")

queryg151 <- dbGetQuery(con2, statement = read_file('SQL/EXTRATO_GRUPO_SWELL_G151.sql'))

##  SEND EMAIL  ==============================================================================================


filewd_mailg151 <-  paste0("C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\BASES\\EXTRATO_G151.xlsx")

write.xlsx(queryg151, file = filewd_mailg151,row.names=FALSE,sheetName = "PEDIDOS",showNA=FALSE)


gm_auth_configure(path = "C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\sendmail.json")

mymailg151 <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,douglas.simao@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("EXTRATO FATURAMENTO GRUPO SWELL G151") %>%
  gm_text_body("

DESCRIÇÃO: RELAÇÃO DE PEDIDOS POR DATA DE FATURAMENTO DO MÊS ANTERIOR.

ESSE É UM EMAIL AUTOMÁTICO.") %>% 
  gm_attach_file(filewd_mailg151)
gm_send_message(mymailg151)


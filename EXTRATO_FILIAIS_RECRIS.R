## EXTRATO FRANQUIAS RECRIS SEM MONTAGEM
## SANDRO JAKOSKA
## 24.11.2022

library(DBI)
library(tidyverse)
library(gmailr)
library(xlsx)
library(reshape2)
library(lubridate)

## DB CONNECTION

con2 <- dbConnect(odbc::odbc(), "reproreplica")

## INITIATE

query_RECRIS <- dbGetQuery(con2, statement = read_file("C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\SQL\\EXTRATO_FILIAIS_RECRIS.sql"))


R_RECRIS <- query_RECRIS %>% 
   group_by(CLICODIGO,PROCODIGO,DESCRICAO,MES=format(floor_date(DATA,"month"),"%b/%Y")) %>% 
     summarize(VALOR=sum(VRVENDA)) %>% 
      dcast(CLICODIGO + PROCODIGO + DESCRICAO ~ 
              factor(MES, levels = format(seq(floor_date(Sys.Date(), "year"),by="month",length.out = 12),"%b/%Y"))
              ,value.var = "VALOR",fun.aggregate=sum) %>% 
  rowwise() %>%  mutate(CLICODIGO=as.character(CLICODIGO)) %>% 
  mutate(TOTAL= rowSums(across(where(is.numeric)),na.rm = TRUE)) %>% 
  arrange(desc(TOTAL)) %>% as.data.frame()

R_4496 <- R_RECRIS %>% filter(CLICODIGO==4496)

R_4483 <- R_RECRIS %>% filter(CLICODIGO==4483)

R_4528 <- R_RECRIS %>% filter(CLICODIGO==4528)

R_4536 <- R_RECRIS %>% filter(CLICODIGO==4536)


##  SEND EMAIL  ==============================================================================================


filewd_mail_RECRIS <-  paste0("C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\BASES\\EXTRATO_RECRIS.xlsx")


write.xlsx(R_4496, file = filewd_mail_RECRIS ,row.names=FALSE,sheetName = "4496",showNA=FALSE)
write.xlsx(R_4483, file = filewd_mail_RECRIS ,row.names=FALSE,sheetName = "4483",showNA=FALSE,append = TRUE)
write.xlsx(R_4528, file = filewd_mail_RECRIS ,row.names=FALSE,sheetName = "4528",showNA=FALSE,append = TRUE)
write.xlsx(R_4536, file = filewd_mail_RECRIS ,row.names=FALSE,sheetName = "4536",showNA=FALSE,append = TRUE)


gm_auth_configure(path = "C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\sendmail.json")

mymail_RECRIS <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,lindamir@oticasrecris.com,ethieli@oticasrecris.com,cristiano.regis@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("EXTRATO FATURAMENTO FRANQUIAS RECRIS") %>%
  gm_text_body("

DESCRIÇÃO: RELAÇÃO DE PEDIDOS POR DATA DE FATURAMENTO DO MÊS ANTERIOR.

ESSE É UM EMAIL AUTOMÁTICO.") %>% 
  gm_attach_file(filewd_mail_RECRIS)
gm_send_message(mymail_RECRIS)


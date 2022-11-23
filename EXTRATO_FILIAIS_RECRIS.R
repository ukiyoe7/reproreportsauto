## PEDIDOS POR ORIGEM 
## Bibliotecas necessárias

library(DBI)
library(tidyverse)
library(gmailr)
library(xlsx)
library(reshape2)
library(lubridate)



## conexão com banco replica 

con2 <- dbConnect(odbc::odbc(), "reproreplica")

query_RECRIS <- dbGetQuery(con2, statement = read_file('SQL/EXTRATO_FILIAIS_RECRIS.sql'))

View(query_RECRIS)


query_RECRIS %>% 
   group_by(CLICODIGO,PROCODIGO,DESCRICAO,MES=format(floor_date(DATA,"month"),"%b/%Y")) %>% 
     summarize(VALOR=sum(VRVENDA)) %>% 
      dcast(CLICODIGO + PROCODIGO + DESCRICAO ~ 
              factor(MES, levels = format(seq(floor_date(Sys.Date(), "year"),by="month",length.out = 12),"%b/%Y"))
              ,value.var = "VALOR",fun.aggregate=sum) %>% 
  rowwise() %>%  mutate(CLICODIGO=as.character(CLICODIGO)) %>% 
  mutate(TOTALS= rowSums(across(where(is.numeric)),na.rm = TRUE)) %>% 
  arrange(desc(TOTALS)) %>% 
       View()






##  SEND EMAIL  ==============================================================================================


filewd_mail_RECRIS <-  paste0("C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\BASES\\EXTRATO_RECRIS.xlsx")

write.xlsx(query_RECRIS, file = filewd_maiL_RECRIS,row.names=FALSE,sheetName = "PEDIDOS",showNA=FALSE)

write.xlsx(filewd_maiL_RECRIS, file = filewd_emp_mail,row.names=FALSE,sheetName = "4496",showNA=FALSE)
write.xlsx(filewd_maiL_RECRIS, file = filewd_emp_mail,row.names=FALSE,sheetName = "4483",showNA=FALSE,append = TRUE)
write.xlsx(filewd_maiL_RECRIS, file = filewd_emp_mail,row.names=FALSE,sheetName = "4528",showNA=FALSE,append = TRUE)


gm_auth_configure(path = "C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\sendmail.json")

mymailg151 <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("EXTRATO FATURAMENTO FRANQUIAS RECRIS") %>%
  gm_text_body("

DESCRIÇÃO: RELAÇÃO DE PEDIDOS POR DATA DE FATURAMENTO DO MÊS ANTERIOR.

ESSE É UM EMAIL AUTOMÁTICO.") %>% 
  gm_attach_file(filewd_mailg151)
gm_send_message(mymailg151)


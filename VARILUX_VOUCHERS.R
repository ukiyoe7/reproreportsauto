## LOAD ==================================


library(DBI)
library(tidyverse)
library(readr)
library(lubridate)
library(reshape2)
library(gmailr)
library(xlsx)

con2 <- dbConnect(odbc::odbc(), "reproreplica")


## QUERY ==================================

## 2022 DATA

VARILUX_VOUCHERS2022 <- dbGetQuery(con2, statement = read_file('SQL/VARILUX_VOUCHERS2022.sql'))

VARILUX_VOUCHERS2022 <- save(VARILUX_VOUCHERS2022,file = "BASES/VARILUX_VOUCHERS2022.RData")

VARILUX_VOUCHERS2022 <-
  get(load("BASES/VARILUX_VOUCHERS2022.RData"))

View(VARILUX_VOUCHERS2022)


## 2023 DATA

VARILUX_VOUCHERS2023 <- dbGetQuery(con2, statement = read_file('SQL/VARILUX_VOUCHERS2023.sql'))


## UNION

VARILUX_VOUCHERS_22_23 <- union_all(VARILUX_VOUCHERS2022,VARILUX_VOUCHERS2023)

View(VARILUX_VOUCHERS_22_23)


VENDAS <-
VARILUX_VOUCHERS_22_23  %>% group_by(CLICODIGO) %>% 
  summarize(  

YTD22=sum(QTD[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date()-years(1), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date()-years(1), "day")-1],na.rm = TRUE),

YTD23=sum(QTD[floor_date(PEDDTBAIXA,"day") >= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date(), "day")-1],na.rm = TRUE),
) %>%  mutate(VAR2023=ifelse(is.finite(YTD23/YTD22-1),YTD23/YTD22-1,0))

## CLIENTES ==================================================================

CLIENTES <- dbGetQuery(con2, statement = read_file('SQL/CLIENTES.sql')) %>% 
              select(CLICODIGO,CLINOMEFANT,GCLCODIGO,SETOR)


VENDAS2 <- 
left_join(CLIENTES,VENDAS,by="CLICODIGO") %>% arrange(desc(YTD23)) %>% 
   filter(!is.na(YTD22) | !is.na(YTD23)) %>% 
    filter(YTD22!=0 & YTD22!=0) %>% 
      mutate(VAR2023=)

View(VENDAS2)



## SUMMARIZE

varilux_vouchers %>%  
  group_by(CLIENTE, MES=toupper(format(floor_date(PEDDTBAIXA,"month"),"%b"))) %>% 
   summarize(VRVENDA=sum(VRVENDA)) %>% 
    dcast(CLIENTE ~ factor(MES, levels =c("JAN","FEV","MAR","ABR","MAI","JUN","JUL","AGO",
                                          "SET","OUT","NOV","DEZ")),fun.aggregate = sum) %>% 
  rowwise() %>%  
  mutate(TOTAL2023= rowSums(across(where(is.numeric)),na.rm = TRUE)) %>% 
  arrange(desc(TOTAL2023)) 


##  SEND EMAIL  ==============================================================================================


gm_auth_configure(path = "C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\sendmail.json")



varilux_cli_vouchers_wd  <-  paste0("C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\BASES\\varilux_cli_vouchers","_",format(Sys.Date(),"%d_%m_%y"),".xlsx")

write.xlsx(fillrate_resumo, file = filewd_emp_mail,row.names=FALSE,sheetName = "RESUMO",showNA=FALSE)


mymail_fillrate <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO FILL RATE") %>%
  gm_text_body("Segue Anexo relatorio dos últimos 7 dias.Esse e um email automatico.") %>% 
  gm_attach_file(filewd_emp_mail)

gm_send_message(mymail_fillrate)





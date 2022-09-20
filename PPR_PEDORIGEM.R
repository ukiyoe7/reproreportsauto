## Bibliotecas necessárias

library(DBI)
library(dplyr)
library(reshape2)
library(googlesheets4)
library(lubridate)
library(gmailr)


## conexão com banco replica 

con2 <- dbConnect(odbc::odbc(), "reproreplica")



library(tidyverse)

#CURRENT MONTH

pedidosorigem2021 <- dbGetQuery(con2,"
    
    WITH FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','R','SR'))
    
    

    SELECT ID_PEDIDO,
            FISCODIGO1,
            PEDDTBAIXA,
             PEDORIGEM
    FROM PEDID 
     LEFT JOIN FIS ON PEDID.FISCODIGO1=FIS.FISCODIGO
      WHERE 
       PEDDTBAIXA BETWEEN '01.01.2021'
AND DATEADD(-1 YEAR TO (CURRENT_DATE) - EXTRACT(DAY FROM CURRENT_DATE) + 32 - 
EXTRACT(DAY FROM (CURRENT_DATE) - EXTRACT(DAY FROM CURRENT_DATE) + 32)) AND 
        PEDSITPED<>'C' AND PEDLCFINANC IN ('S', 'L','N')
    ")  



po1 <- union_all(pedidosorigem2021 %>% group_by(MES=toupper(format(floor_date(PEDDTBAIXA,"month"),"%b")),PEDORIGEM) %>% 
                   summarize(N=n_distinct(ID_PEDIDO[PEDORIGEM=='W'],na.rm = TRUE)) %>% filter(PEDORIGEM=='W') , 
                 
                 
                 pedidosorigem2021 %>% group_by(MES=toupper(format(floor_date(PEDDTBAIXA,"month"),"%b"))) %>% 
                   summarize(N=n_distinct(ID_PEDIDO)) %>% mutate(PEDORIGEM='TOTAL')) %>% 
  
  dcast(PEDORIGEM ~ factor(MES, levels =c("JAN","FEV","MAR","ABR","MAI","JUN","JUL","AGO",
                                          "SET","OUT","NOV","DEZ"))) %>% arrange(desc(PEDORIGEM)) 
  
  
##  rowwise() %>%  mutate(TOTAL= rowSums(across(where(is.numeric)),na.rm = TRUE))  %>%  as.data.frame() %>% mutate(PERCENTUAL=TOTAL/lag(TOTAL))  %>%  arrange(TOTAL) 

pedidosorigem2022 <- dbGetQuery(con2,"
    
    WITH FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','R','SR'))
    
    

    SELECT ID_PEDIDO,
            PEDDTBAIXA,
             PEDORIGEM
    FROM PEDID 
     LEFT JOIN FIS ON PEDID.FISCODIGO1=FIS.FISCODIGO
      WHERE 
       PEDDTBAIXA BETWEEN '01.01.2022' AND 'TODAY' AND 
        PEDSITPED<>'C' AND PEDLCFINANC IN ('S', 'L','N')
    ")  




po2 <- union_all(pedidosorigem2022 %>% group_by(MES=toupper(format(floor_date(PEDDTBAIXA,"month"),"%b")),PEDORIGEM) %>% 
                   summarize(N=n_distinct(ID_PEDIDO[PEDORIGEM=='W'],na.rm = TRUE)) %>% filter(PEDORIGEM=='W') , 
                 
                 
                 pedidosorigem2022%>% group_by(MES=toupper(format(floor_date(PEDDTBAIXA,"month"),"%b"))) %>% 
                   summarize(N=n_distinct(ID_PEDIDO)) %>% mutate(PEDORIGEM='TOTAL')) %>% 
  
  dcast(PEDORIGEM ~ factor(MES, levels =c("JAN","FEV","MAR","ABR","MAI","JUN","JUL","AGO",
                                          "SET","OUT","NOV","DEZ"))) %>% arrange(desc(PEDORIGEM))



range_write("1RxdRGKDgSdYr970NTX8HXxmStFeDDrhceAjHrReJZHA",
            sheet = "WEBPEDIDOS",data =po1, range = "B2:N4",reformat = FALSE)

range_write("1RxdRGKDgSdYr970NTX8HXxmStFeDDrhceAjHrReJZHA",
            sheet = "WEBPEDIDOS",data =po2, range = "B7",reformat = FALSE)




## SEND MAIL ====================================================================


gm_auth_configure(path = "C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\sendmail.json")

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,cristiano.regis@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO PPR PEDORIGEM") %>%
  gm_text_body("RELATÓRIO ATUALIZADO.
ACESSE O LINK https://docs.google.com/spreadsheets/d/1RxdRGKDgSdYr970NTX8HXxmStFeDDrhceAjHrReJZHA/edit?usp=sharing

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)
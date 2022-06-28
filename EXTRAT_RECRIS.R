## RELATORIO DE DESCONTOS COMERCIAIS
## SANDRO JAKOSKA 15_02_2022

library(dplyr)
library(lubridate)
library(googlesheets4)
library(DBI)
library(gmailr)
library(xlsx)


gm_auth_configure(path = "C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\sendmail.json")

con2 <- dbConnect(odbc::odbc(), "reproreplica")


extrat_cli <- dbGetQuery(con2,"
  
WITH FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','R','SR')),

PED AS (SELECT ID_PEDIDO,PEDID.CLICODIGO,PEDDTBAIXA FROM PEDID 
INNER JOIN FIS ON PEDID.FISCODIGO1=FIS.FISCODIGO
   WHERE PEDDTBAIXA
     BETWEEN DATEADD(MONTH, -1, CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) + 1)
      AND CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE)  AND 
     PEDSITPED<>'C' AND 
      PEDLCFINANC IN ('S', 'L','N') AND
        CLICODIGO IN (4483,7796))
  
SELECT PEDDTBAIXA DATA_FATURAMENTO,PD.ID_PEDIDO, CLICODIGO,PDPDESCRICAO DESCRICAO,
SUM(PDPQTDADE)QTD,SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA 
 FROM PDPRD PD
  INNER JOIN PED P ON P.ID_PEDIDO=PD.ID_PEDIDO
  GROUP BY 1,2,3,4

")



 resumo_prod <- extrat_cli %>% group_by(DESCRICAO) %>% 
   summarize(VENDAS=sum(VRVENDA),QTD=sum(QTD)) %>% arrange(desc(VENDAS)) %>% 
     mutate(MES=format(floor_date(Sys.Date()-30,"month"),"%m/%y")) %>% .[,c(4,1,2,3)] %>% as.data.frame()

 
 
 ## WRITE XLS ==================================================================
 
 
 filewd4 <-  paste0("C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\BASES\\extrat_recris","_",format(Sys.Date(),"%d_%m_%y"),".xlsx")
 
 write.xlsx(resumo_prod, file = filewd4,row.names=FALSE,sheetName = "RESUMO")
 
 write.xlsx(extrat_cli, file = filewd4,row.names=FALSE,sheetName = "DADOS", append = TRUE)
 
 
 ## SEND MAIL ==================================================================
 
 
 mymailextrat<- gm_mime() %>% 
   gm_to("sandro.jakoska@repro.com.br,cristiano.regis@repro.com.br,ethieli@oticasrecris.com,lindamir@oticasrecris.com") %>% 
   gm_from ("comunicacao@repro.com.br") %>%
   gm_subject("EXTRATO PEDIDOS REPRO - OTICA RECRIS") %>%
   gm_text_body("Segue anexo relatorio com o extrato dos pedidos realizados com a Repro no mês anterior.Esse e um email automatico.") %>% 
   gm_attach_file(filewd4)
 
 gm_send_message(mymailextrat)
 
 
 


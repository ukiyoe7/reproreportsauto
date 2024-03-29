## RELATORIO INSIGNE 
## 31.05.202
## SANDRO JAKOSKA


library(tidyverse)
library(lubridate)
library(googlesheets4)
library(reshape2)
library(DBI)

con2 <- dbConnect(odbc::odbc(), "reproreplica")


insigne <- dbGetQuery(con2,"
  
  WITH FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','R','SR')),
  
  CLI AS (SELECT C.CLICODIGO,SETOR FROM CLIEN C
  INNER JOIN (SELECT CLICODIGO, ZODESCRICAO SETOR FROM ENDCLI E
  INNER JOIN (SELECT ZOCODIGO,ZODESCRICAO FROM ZONA WHERE ZOCODIGO IN(20,21,22,23,24,28))Z ON E.ZOCODIGO=Z.ZOCODIGO
  WHERE ENDFAT='S'
  ) ED ON C.CLICODIGO=ED.CLICODIGO
  WHERE CLICLIENTE='S'),
  
  
  PED AS (SELECT ID_PEDIDO,PEDDTBAIXA,P.CLICODIGO,SETOR FROM PEDID P
  INNER JOIN FIS ON P.FISCODIGO1=FIS.FISCODIGO
   INNER JOIN CLI C ON P.CLICODIGO=C.CLICODIGO
    WHERE PEDDTBAIXA BETWEEN '01.01.2022' AND 'TODAY' AND PEDSITPED<>'C' AND PEDLCFINANC IN ('S', 'L','N')),
    
     PROD AS (SELECT PROCODIGO FROM PRODU WHERE MARCODIGO='189' AND PROTIPO<>'T')
  
  
  SELECT PD.ID_PEDIDO,
            PEDDTBAIXA,
             CLICODIGO,
              SETOR,
               PD.PROCODIGO,
                PDPDESCRICAO,
                 SUM(PDPQTDADE) QTD,
                 SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA 
   FROM PDPRD PD
    INNER JOIN PED P ON PD.ID_PEDIDO=P.ID_PEDIDO
     INNER JOIN PROD PRD ON PRD.PROCODIGO=PD.PROCODIGO
  GROUP BY 1,2,3,4,5,6
  ")  


## QUANTIDADE


insigne2022_q <- insigne %>% group_by(SETOR,MES=toupper(format(floor_date(PEDDTBAIXA,"month"),"%b"))) %>% 
  summarize(QTD=round(sum(QTD),1)) %>% 
  dcast(SETOR ~ factor(MES, levels =c("JAN","FEV","MAR","ABR","MAI","JUN","JUL","AGO",
                                                       "SET","OUT","NOV","DEZ"))) 


range_write("1izIb0rDEtyAh48Jtv_GkjbyXWhWEeqNTiGeKeMAoeB0",
            data=insigne2022_q,sheet = "QTD",
            range = "A15",reformat = FALSE)

## VALOR

insigne2022_v <- insigne %>% group_by(SETOR,MES=toupper(format(floor_date(PEDDTBAIXA,"month"),"%b"))) %>% 
  summarize(VALOR=round(sum(VRVENDA),1)) %>% 
  dcast(SETOR ~ factor(MES, levels =c("JAN","FEV","MAR","ABR","MAI","JUN","JUL","AGO",
                                      "SET","OUT","NOV","DEZ"))) 


range_write("1izIb0rDEtyAh48Jtv_GkjbyXWhWEeqNTiGeKeMAoeB0",
            data=insigne2022_v,sheet = "VALOR",
            range = "A15",reformat = FALSE)


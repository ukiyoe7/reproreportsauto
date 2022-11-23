## RANKING CLIENTS INSIGNE 
## 20.09.2022

library(DBI)
library(dplyr)
library(lubridate)
library(googlesheets4)
library(reshape2)


con2 <- dbConnect(odbc::odbc(), "reproreplica")

## ===============================================================================


# CLIENTS

insigne_cli <- dbGetQuery(con2,"
                 WITH PROD AS (SELECT TPLCODIGO,PRODESCRICAO FROM PRODU 
                                  WHERE MARCODIGO=189)
                                  
                        SELECT DISTINCT T.CLICODIGO FROM TPLCLI T
                         INNER JOIN PROD P ON T.TPLCODIGO=P.TPLCODIGO")




cli <- dbGetQuery(con2,"SELECT DISTINCT C.CLICODIGO,
                                 CLINOMEFANT,
                                     IIF(C.GCLCODIGO IS NULL,C.CLICODIGO || ' ' || CLINOMEFANT,'G' || C.GCLCODIGO || ' ' || GCLNOME) CLIENTE,
                                      SETOR
                                       FROM CLIEN C
                                        LEFT JOIN (SELECT CLICODIGO,E.ZOCODIGO,CIDNOME,ZODESCRICAO SETOR,ENDCODIGO FROM ENDCLI E
                                         LEFT JOIN CIDADE CID ON E.CIDCODIGO=CID.CIDCODIGO
                                          LEFT JOIN (SELECT ZOCODIGO,ZODESCRICAO FROM ZONA WHERE ZOCODIGO IN (20,21,22,23,24,25,28))Z ON E.ZOCODIGO=Z.ZOCODIGO WHERE ENDFAT='S')A ON C.CLICODIGO=A.CLICODIGO
                                           LEFT JOIN GRUPOCLI GR ON C.GCLCODIGO=GR.GCLCODIGO
                                            WHERE CLICLIENTE='S'")





cli_insigne <- inner_join(insigne_cli,cli,by="CLICODIGO") 



insigne_promo <- dbGetQuery(con2,"SELECT CLICODIGO FROM CLIPROMO WHERE ID_PROMO=18") %>% 
  mutate(TEM_CAMPANHA='S')


cli_insigne2 <- left_join(cli_insigne,insigne_promo,by="CLICODIGO")


## SALES


insigne_sales <- dbGetQuery(con2,"
  
    WITH FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','R','SR')),
  
  
  PED AS (SELECT ID_PEDIDO,PEDDTBAIXA,CLICODIGO FROM PEDID P
  INNER JOIN FIS ON P.FISCODIGO1=FIS.FISCODIGO
    WHERE PEDDTBAIXA BETWEEN '01.01.2022' AND 'TODAY' 
     AND PEDSITPED<>'C' AND PEDLCFINANC IN ('S', 'L','N')),
    
     PROD AS (SELECT PROCODIGO FROM PRODU WHERE MARCODIGO='189' AND PROTIPO<>'T')
  
  
  SELECT PD.ID_PEDIDO,
            PEDDTBAIXA,
             CLICODIGO,
               PD.PROCODIGO,
                PDPDESCRICAO,
                 SUM(PDPQTDADE) QTD,
                 SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA 
   FROM PDPRD PD
    INNER JOIN PED P ON PD.ID_PEDIDO=P.ID_PEDIDO
     INNER JOIN PROD PRD ON PRD.PROCODIGO=PD.PROCODIGO
  GROUP BY 1,2,3,4,5
  ") 



## CREATE MATRIX
## LOJAS ============================================================================

insigne_sales2 <- insigne_sales %>% 
  dcast(CLICODIGO ~ floor_date(PEDDTBAIXA,"month"), 
        value.var = "QTD",fun.aggregate=sum) %>% 
  mutate(CLICODIGO=as.character(CLICODIGO)) %>% 
  rowwise() %>%  
  mutate(TOTALS= rowSums(across(where(is.numeric)),na.rm = TRUE))


insigne_sales_lojas <- left_join(cli_insigne2 %>% 
                                   select(CLICODIGO,CLINOMEFANT,TEM_CAMPANHA,SETOR) %>% 
                                   mutate(CLICODIGO=as.character(CLICODIGO)),insigne_sales2,by="CLICODIGO") %>% 
  arrange(desc(TOTALS)) %>% 
  rename_at(5:(5+ month(Sys.Date())-1), ~ c(seq.Date(from = as.Date("2022-01-01"), 
                                                     length.out = month(Sys.Date()), by = "month") %>% 
                                              format("%b") %>% 
                                              toupper(.)))



range_write("1FHfgcewWUpRaPTOM0sbByqOzornjc2TriIEd3bIQMnQ",data=insigne_sales_lojas ,
            sheet = "DADOS",
            range = "A1",reformat = FALSE) 

## GRUPOS ===========================================================================



insigne_sales_clientes <- insigne_sales %>% 
  left_join(.,cli_insigne2,by="CLICODIGO") %>% 
  group_by(PEDDTBAIXA,CLIENTE,SETOR,TEM_CAMPANHA,QTD) %>% 
  summarize(QTD=sum(QTD)) %>% 
  as.data.frame() %>% 
  dcast(CLIENTE + TEM_CAMPANHA + SETOR ~ floor_date(PEDDTBAIXA,"month"), 
        value.var = "QTD",fun.aggregate=sum) %>% 
  rowwise() %>%  
  mutate(TOTALS= rowSums(across(where(is.numeric)),na.rm = TRUE)) %>% 
  arrange(desc(TOTALS)) %>% 
  rename_at(4:(4+ month(Sys.Date())-1), ~ c(seq.Date(from = as.Date("2022-01-01"), 
                                                     length.out = month(Sys.Date()), by = "month") %>% 
                                              format("%b") %>% 
                                              toupper(.))) %>% 
                                                 filter(!is.na(CLIENTE))






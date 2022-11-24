## RELATÓRIO CAMPANHA TRANSITIONS
## 11.2022
## SANDRO JAKOSKA


## LIBRARIES =======================================================================

library(tidyverse)
library(lubridate)
library(DBI)
library(googlesheets4)
library(reshape2)
library(gmailr)

## DB CONNECTION ====================================================================

con2 <- dbConnect(odbc::odbc(), "reproreplica")

## SQL  ====================================================================


sales_trans <- dbGetQuery(con2,"

WITH FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','R','SR')),

       CLI AS (SELECT DISTINCT C.CLICODIGO,
                       CLINOMEFANT,
                        ENDCODIGO,
                         SETOR
                          FROM CLIEN C
                           LEFT JOIN (SELECT CLICODIGO,E.ZOCODIGO,ZODESCRICAO SETOR,ENDCODIGO FROM ENDCLI E
                            LEFT JOIN (SELECT ZOCODIGO,ZODESCRICAO FROM ZONA WHERE ZOCODIGO IN (20,21,22,23,24,25,28))Z ON E.ZOCODIGO=Z.ZOCODIGO WHERE ENDFAT='S')A ON C.CLICODIGO=A.CLICODIGO
                             WHERE CLICLIENTE='S'),

         PED AS (SELECT ID_PEDIDO,
                         P.CLICODIGO,
                          SETOR,
                           PEDDTBAIXA
                            FROM PEDID P
                             INNER JOIN FIS F ON P.FISCODIGO1=F.FISCODIGO
                              INNER JOIN CLI C ON P.CLICODIGO=C.CLICODIGO AND P.ENDCODIGO=C.ENDCODIGO
                               WHERE PEDDTBAIXA BETWEEN '01.10.2022' AND '31.12.2022' AND PEDSITPED<>'C' AND PEDLCFINANC IN ('S', 'L','N')),
            
                               
        PROD AS  (SELECT PROCODIGO FROM PRODU WHERE PROTIPO IN ('P','F','E')) ,
        
        VLX AS  (SELECT PROCODIGO FROM PRODU WHERE MARCODIGO=57),
        
        VS AS  (SELECT PROCODIGO FROM PRODU WHERE GR2CODIGO=3),
        
        TRANS AS  (SELECT PROCODIGO FROM PRODU WHERE (PRODESCRICAO LIKE '%TGEN8%' OR PRODESCRICAO LIKE '%TRANS%'))  

        SELECT PEDDTBAIXA,
                CLICODIGO,
                 PR.PROCODIGO,
                  PDPDESCRICAO,
                   SETOR,
                    CASE 
                     WHEN VX.PROCODIGO IS NOT NULL THEN 'VARILUX'
                      WHEN VS.PROCODIGO IS NOT NULL THEN 'VISAO SIMPLES'
                       ELSE 'OUTROS' END MARCA,
                        IIF (T.PROCODIGO IS NOT NULL,'TRANSITIONS','') TRANSITIONS,
                         SUM(PDPQTDADE)QTD,
                          SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA 
                              FROM PDPRD PD
                               INNER JOIN PED P ON PD.ID_PEDIDO=P.ID_PEDIDO
                                INNER JOIN PROD PR ON PD.PROCODIGO=PR.PROCODIGO
                                 LEFT JOIN VLX VX ON PD.PROCODIGO=VX.PROCODIGO
                                  LEFT JOIN VS VS ON PD.PROCODIGO=VS.PROCODIGO
                                   LEFT JOIN TRANS T ON PD.PROCODIGO=T.PROCODIGO
                                    GROUP BY 1,2,3,4,5,6,7")

## GROUP ====================================================================

sales_trans_vlx_kdk_1234 <- sales_trans %>% filter(TRANSITIONS=="TRANSITIONS") %>% mutate(MARCA=str_trim(MARCA)) %>% 
  filter(!str_detect(MARCA,"OUTROS")) %>% 
  filter(!str_detect(SETOR,"SETOR 5 - BLUMENAU - VALE")) %>%  
  filter(!str_detect(SETOR,"SETOR 6 - BALNEARIO-LITORAL")) %>% 
  group_by(SETOR,MARCA) %>%  summarize(QTD=sum(QTD)) 


sales_trans_geral_1234 <- sales_trans %>% filter(TRANSITIONS=="TRANSITIONS") %>% mutate(MARCA=str_trim(MARCA)) %>% 
  filter(!str_detect(SETOR,"SETOR 5 - BLUMENAU - VALE")) %>%  
  filter(!str_detect(SETOR,"SETOR 6 - BALNEARIO-LITORAL")) %>%
  group_by(SETOR,MARCA='TRANSITIONS') %>%  summarize(QTD=sum(QTD)) 

sales_trans_rs_1234 <- union_all(sales_trans_vlx_kdk_1234,sales_trans_geral_1234)



## only sector 5 and 6

sales_trans_vlx_kdk_56 <- sales_trans %>% filter(TRANSITIONS=="TRANSITIONS") %>% mutate(MARCA=str_trim(MARCA)) %>% 
  filter(!str_detect(MARCA,"OUTROS")) %>% 
  filter(SETOR %in% c("SETOR 5 - BLUMENAU - VALE","SETOR 6 - BALNEARIO-LITORAL")) %>%  
  group_by(MARCA) %>%  summarize(QTD=sum(QTD)) 


sales_trans_geral_56 <- sales_trans %>% filter(TRANSITIONS=="TRANSITIONS") %>% mutate(MARCA=str_trim(MARCA)) %>% 
  filter(SETOR %in% c("SETOR 5 - BLUMENAU - VALE","SETOR 6 - BALNEARIO-LITORAL")) %>%  
  group_by(MARCA='TRANSITIONS') %>%  summarize(QTD=sum(QTD)) 

sales_trans_rs_56 <- union_all(sales_trans_vlx_kdk_56,sales_trans_geral_56) %>% mutate(SETOR='SETORES 5 | 6') %>% .[,c(3,1,2)]

## GERAL

sales_trans_rs_vlx_kdk  <- sales_trans %>% filter(TRANSITIONS=="TRANSITIONS") %>% mutate(MARCA=str_trim(MARCA)) %>% 
  filter(!str_detect(MARCA,"OUTROS")) %>%  group_by(MARCA) %>%  summarize(QTD=sum(QTD)) %>% mutate(SETOR="GERAL") %>% .[,c(3,1,2)]

sales_trans_rs_geral  <- sales_trans %>% filter(TRANSITIONS=="TRANSITIONS") %>% mutate(MARCA=str_trim(MARCA)) %>% 
  group_by(MARCA="TRANSITIONS") %>%  summarize(QTD=sum(QTD)) %>% mutate(SETOR="GERAL")

sales_trans_rs <- union_all(sales_trans_rs_vlx_kdk,sales_trans_rs_geral)


sales_trans_rs_123456 <- union_all(sales_trans_rs_1234,sales_trans_rs_56) %>% union_all(.,sales_trans_rs)




## GET METAS ====================================================================


metas_trans <- read_sheet("19MlrfBtyPBMHWL3yHnsaNxauX0HVeCG6GeSHfIagT9E",sheet = "METAS") %>% 
  mutate(MARCA=str_trim(MARCA))

metas_trans_1234 <- metas_trans %>%  filter(!SETOR %in% c("SETOR 5 - BLUMENAU - VALE","SETOR 6 - BALNEARIO-LITORAL"))

metas_trans_56 <- metas_trans %>% filter(SETOR %in% c("SETOR 5 - BLUMENAU - VALE","SETOR 6 - BALNEARIO-LITORAL")) %>% 
  group_by(MARCA) %>% summarize(META=max(META)) %>% mutate(SETOR='SETORES 5 | 6') %>% .[,c(3,1,2)]


metas_trans_123456 <- union_all(metas_trans_1234,metas_trans_56)


## MEASURE PREFORMANCE ====================================================================


perf_trans_123456 <- left_join(sales_trans_rs_123456,metas_trans_123456,by=c("SETOR","MARCA")) %>% 
  mutate(ALCANCE=round(QTD/META,2)) %>% mutate(DIF=QTD-META) %>% arrange(SETOR)



## WRITE GOOGLE ====================================================================

## TODOS SETORES
range_write("19MlrfBtyPBMHWL3yHnsaNxauX0HVeCG6GeSHfIagT9E",
            data=perf_trans_123456,sheet = "RESUMO",
            range = "A:F",reformat = FALSE)

## DADOS
range_write("19MlrfBtyPBMHWL3yHnsaNxauX0HVeCG6GeSHfIagT9E",
            data=sales_trans %>% filter(TRANSITIONS=="TRANSITIONS") ,sheet = "DADOS",
            range = "A:I",reformat = FALSE)


## SEND MAIL ====================================================================


gm_auth_configure(path = "C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\sendmail.json")

## GERAL 

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,leandro.fritzen@repro.com.br,cristiano.regis@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO CAMPANHA TRANSITIONS OUT -> DEZ 2022") %>%
  gm_text_body("
  ACOMPANHE A CAMPANHA TRANSITIONS.O RELATÓRIO FOI ATUALIZADO.
  
ACESSE O LINK https://docs.google.com/spreadsheets/d/19MlrfBtyPBMHWL3yHnsaNxauX0HVeCG6GeSHfIagT9E/edit?usp=sharing.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)

## SETOR 1 

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,leandro.fritzen@repro.com.br,cinthia.noronha@repro.com.br,diego.machado@essilor.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RRELATORIO CAMPANHA TRANSITIONS OUT -> DEZ 2022 | SETOR 1") %>%
  gm_text_body("
  ACOMPANHE A CAMPANHA TRANSITIONS.O RELATÓRIO FOI ATUALIZADO.
  
ACESSE O LINK https://docs.google.com/spreadsheets/d/1FRtsrZRbPEh2DEH1ut4yIWlSVck92afYVe97HpsjVyE/edit?usp=sharing.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)


## SETOR 2

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,joao.avila@repro.com.br,cinthia.noronha@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO CAMPANHA TRANSITIONS OUT -> DEZ 2022 | SETOR 2") %>%
  gm_text_body("
  ACOMPANHE A CAMPANHA TRANSITIONS.O RELATÓRIO FOI ATUALIZADO.
  
ACESSE O LINK https://docs.google.com/spreadsheets/d/15r90LdLeIPS6HH6SUwSbF-nBwRQMcYcTYJERsASB_O4/edit?usp=sharing.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)


## SETOR 3

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,douglas.simao@repro.com.br,jenifer.santos@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO CAMPANHA TRANSITIONS OUT -> DEZ 2022 | SETOR 3") %>%
  gm_text_body("
  ACOMPANHE A CAMPANHA TRANSITIONS.O RELATÓRIO FOI ATUALIZADO.
  
ACESSE O LINK https://docs.google.com/spreadsheets/d/1BHIoFbhkjgv_KXQtumXvNnQtUrb6W7VVkmmadBtUkdc/edit?usp=sharing.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)

## SETOR 4

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,jenifer.santos@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO CAMPANHA TRANSITIONS OUT -> DEZ 2022 | SETOR 4") %>%
  gm_text_body("
  ACOMPANHE A CAMPANHA TRANSITIONS.O RELATÓRIO FOI ATUALIZADO.
  
ACESSE O LINK https://docs.google.com/spreadsheets/d/13BMVjwKyl2F_oNnjzB7eHpEiMeaITAw1_qtHipwf4us/edit?usp=sharing.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)

## SETOR 5 6

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,alexandre.cipriani@repro.com.br,fabiana.godoy@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO CAMPANHA TRANSITIONS OUT -> DEZ 2022 | SETOR 5 6 ") %>%
  gm_text_body("
  ACOMPANHE A CAMPANHA TRANSITIONS.O RELATÓRIO FOI ATUALIZADO.
  
ACESSE O LINK https://docs.google.com/spreadsheets/d/1EvEUsnXu3RzR0PdanKHmVz2UYPC2lb8PMh7MCQWMsaY/edit?usp=sharing.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)





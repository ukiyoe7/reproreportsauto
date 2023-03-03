## RELATORIO TRANSITIONS 2023
## 03.2023
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
                            LEFT JOIN (SELECT ZOCODIGO,ZODESCRICAO FROM ZONA WHERE ZOCODIGO IN (20,21,22,23,24,25,26,27,28))Z ON E.ZOCODIGO=Z.ZOCODIGO WHERE ENDFAT='S')A ON C.CLICODIGO=A.CLICODIGO
                             WHERE CLICLIENTE='S'),

         PED AS (SELECT ID_PEDIDO,
                         P.CLICODIGO,
                          SETOR,
                           PEDDTBAIXA
                            FROM PEDID P
                             INNER JOIN FIS F ON P.FISCODIGO1=F.FISCODIGO
                              INNER JOIN CLI C ON P.CLICODIGO=C.CLICODIGO AND P.ENDCODIGO=C.ENDCODIGO
                               WHERE PEDDTBAIXA BETWEEN '01.01.2023' AND 'YESTERDAY' AND PEDSITPED<>'C' AND PEDLCFINANC IN ('S', 'L','N')),
            
                               
        PROD AS  (SELECT PROCODIGO FROM PRODU WHERE PROTIPO IN ('P','F','E')) ,
        
        VLX AS  (SELECT PROCODIGO FROM PRODU WHERE MARCODIGO=57),
        
        KDK AS  (SELECT PROCODIGO FROM PRODU WHERE MARCODIGO=24 AND GR2CODIGO=1),
        
        TRANS AS  (SELECT PROCODIGO FROM PRODU WHERE (PRODESCRICAO LIKE '%TGEN8%' OR PRODESCRICAO LIKE '%TRANS%'))  

        SELECT PEDDTBAIXA,
                CLICODIGO,
                 PR.PROCODIGO,
                  PDPDESCRICAO,
                   SETOR,
                    CASE 
                     WHEN VX.PROCODIGO IS NOT NULL THEN 'VARILUX'
                      WHEN KD.PROCODIGO IS NOT NULL THEN 'KODAK'
                       ELSE 'OUTROS' END MARCA,
                        IIF (T.PROCODIGO IS NOT NULL,'TRANSITIONS','') TRANSITIONS,
                         SUM(PDPQTDADE)QTD,
                          SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA 
                              FROM PDPRD PD
                               INNER JOIN PED P ON PD.ID_PEDIDO=P.ID_PEDIDO
                                INNER JOIN PROD PR ON PD.PROCODIGO=PR.PROCODIGO
                                 LEFT JOIN VLX VX ON PD.PROCODIGO=VX.PROCODIGO
                                  LEFT JOIN KDK KD ON PD.PROCODIGO=KD.PROCODIGO
                                   LEFT JOIN TRANS T ON PD.PROCODIGO=T.PROCODIGO
                                    GROUP BY 1,2,3,4,5,6,7")


## METAS  ====================================================================

metas <-
read_sheet(ss="1Pzm97Otd9T524zudbxLmu_OT4hgESQHMv1P-Xa3e3VA",sheet = "METAS")



## RESUMO  ====================================================================

sales_trans1 <- 
sales_trans %>% 
    filter(TRANSITIONS=='TRANSITIONS') %>% 
   group_by(SETOR=substr(SETOR,1,7)) %>% 
     summarize(T1=sum(QTD))

sales_trans2 <- 
  sales_trans %>% 
   filter(TRANSITIONS=='TRANSITIONS') %>% 
    group_by(SETOR='GERAL') %>% summarize(T1=sum(QTD))

sales_trans3 <- rbind(sales_trans1,sales_trans2)


sales_trans4 <- left_join(metas,sales_trans3,by="SETOR") %>%  
                  as.data.frame() %>%
                   mutate(DIF_META=`META T1`-T1) %>% 
                    mutate(ALCANCE=(T1/`META T1`))




## ESPERADO ====================================================================

first_day <- as.Date('2023-01-01')
num_days <- as.numeric(difftime( as.Date('2023-03-31'), first_day, units = "days")) +1


num_weekends <- sum(wday(seq(first_day,as.Date('2023-03-31'), by = "day")) %in% c(1,7))
num_days = num_days - num_weekends-2


num_days_2 <- as.numeric(difftime(Sys.Date(), first_day, units = "days"))
num_weekends_2 <- sum(wday(seq(first_day, Sys.Date(), by = "day")) %in% c(1,7))
num_days_2 = num_days_2 - num_weekends_2-2

sales_trans5 <-
 sales_trans4 %>% 
  mutate(ESPERADO=((`META T1`/num_days)*num_days_2/`META T1`)) %>% 
   mutate(DIF_T1xESPERADO=ALCANCE-ESPERADO) %>% 
    mutate(ALCANCE=round(ALCANCE,4)) %>% 
     mutate(ESPERADO=round(ESPERADO,4)) %>% 
      mutate(DIF_T1xESPERADO=round(DIF_T1xESPERADO,4)) 


range_write(sales_trans5,ss="1Pzm97Otd9T524zudbxLmu_OT4hgESQHMv1P-Xa3e3VA",sheet = "RESUMO T1", range = "A:I",reformat = FALSE)


## SEND MAIL ====================================================================


gm_auth_configure(path = "C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\sendmail.json")

## GERAL 

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,leandro.fritzen@repro.com.br,cristiano.regis@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO METAS TRANSITIONS T1 2023") %>%
  gm_text_body("
  ACOMPANHE AS METAS TRANSITIONS.O RELATÓRIO FOI ATUALIZADO.
  
ACESSE O LINK https://docs.google.com/spreadsheets/d/1Pzm97Otd9T524zudbxLmu_OT4hgESQHMv1P-Xa3e3VA/edit?usp=sharing.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)

## SETOR 1 

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,leandro.fritzen@repro.com.br,cinthia.noronha@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO METAS TRANSITIONS T1 2023 | SETOR 1") %>%
  gm_text_body("
 ACOMPANHE AS METAS TRANSITIONS.O RELATÓRIO FOI ATUALIZADO.
  
ACESSE O LINK https://docs.google.com/spreadsheets/d/1huzFGv1RBiNURKt9bKUDOnYhN6Fa18ZSTPklGOApzQY/edit?usp=sharing.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)


## SETOR 2

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,joao.avila@repro.com.br,cinthia.noronha@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO METAS TRANSITIONS T1 2023 | SETOR 2") %>%
  gm_text_body("
  ACOMPANHE AS METAS TRANSITIONS.O RELATÓRIO FOI ATUALIZADO.
  
ACESSE O LINK https://docs.google.com/spreadsheets/d/1spDKFq3N6bX7eqfhwzk29--xPDgV2Rj4jyHOUB5ScOw/edit?usp=sharing.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)


## SETOR 3

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,douglas.simao@repro.com.br,jenifer.santos@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO METAS TRANSITIONS T1 2023 | SETOR 3") %>%
  gm_text_body("
  ACOMPANHE AS METAS TRANSITIONS.O RELATÓRIO FOI ATUALIZADO.
  
ACESSE O LINK https://docs.google.com/spreadsheets/d/1jsyEysKHMW8gy5_GOJMHkQ6XFnQ_uBSNGvZjey2MB_Y/edit?usp=sharing.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)

## SETOR 4

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,joao.alves@repro.com.br,jenifer.santos@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO METAS TRANSITIONS T1 2023 | SETOR 4") %>%
  gm_text_body("
  ACOMPANHE AS METAS TRANSITIONS.O RELATÓRIO FOI ATUALIZADO.
  
ACESSE O LINK https://docs.google.com/spreadsheets/d/1RiOPrXwJnXzJxuzEN9L2AWBNamHivN6yfgtFe4z26s8/edit?usp=sharing.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)

## SETOR 5 

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,alexandre.cipriani@repro.com.br,fabiana.godoy@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO METAS TRANSITIONS T1 2023 | SETOR 5 ") %>%
  gm_text_body("
  ACOMPANHE AS METAS TRANSITIONS.O RELATÓRIO FOI ATUALIZADO.
  
ACESSE O LINK https://docs.google.com/spreadsheets/d/1gX7JrP-kq47jqTQaMHxs9f75HUBCI1qNQy2R0BTgf_U/edit?usp=sharing.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)

## SETOR 6

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,francielle.pasqualino@repro.com.br,fabiana.godoy@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO METAS TRANSITIONS T1 2023 | SETOR 6 ") %>%
  gm_text_body("
  ACOMPANHE AS METAS TRANSITIONS.O RELATÓRIO FOI ATUALIZADO.
  
ACESSE O LINK https://docs.google.com/spreadsheets/d/1TONRTeSO9hAwQBzTbW2NJbQSMogjxz6Wb2BwpKh4clc/edit?usp=sharing.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)


## SETOR 7

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,diego.machado@repro.com.br,cinthia.noronha@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO METAS TRANSITIONS T1 2023 | SETOR 7 ") %>%
  gm_text_body("
  ACOMPANHE AS METAS TRANSITIONS.O RELATÓRIO FOI ATUALIZADO.
  
ACESSE O LINK https://docs.google.com/spreadsheets/d/1GppwfhCoerDR2IJgmlg-NUipIE9od0JHon3MGq4wf20/edit?usp=sharing.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)

## SETOR 8

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,rafael.michelon@repro.com.br,jenifer.santos@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO METAS TRANSITIONS T1 2023 | SETOR 8 ") %>%
  gm_text_body("
  ACOMPANHE AS METAS TRANSITIONS.O RELATÓRIO FOI ATUALIZADO.
  
ACESSE O LINK https://docs.google.com/spreadsheets/d/1e5U5ovTOKDRSKR4RfXgpvu8DluD009Zm5ITj-VT2KEQ/edit?usp=sharing.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)



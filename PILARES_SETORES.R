## RANKING DE PILARES SETORES
## 07.2022
## SANDRO JAKOSKA

## LIBRARIES =======================================================================

library(tidyverse)
library(lubridate)
library(reshape2)
library(DBI)
library(googlesheets4)

## DB CONNECTION ====================================================================

con2 <- dbConnect(odbc::odbc(), "reproreplica")

## SALES

sales_pilares_2022 <- dbGetQuery(con2,"

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
                               WHERE PEDDTBAIXA BETWEEN '01.01.2022' AND 'YESTERDAY' AND PEDSITPED<>'C' AND PEDLCFINANC IN ('S', 'L','N')),
            
                               
        PROD AS  (SELECT PROCODIGO FROM PRODU WHERE PROTIPO IN ('P','F','E')) ,
        
        VLX AS  (SELECT PROCODIGO FROM PRODU WHERE MARCODIGO=57),
        
        KDK AS  (SELECT PROCODIGO FROM PRODU WHERE MARCODIGO=24),
        
        LA AS  (SELECT PROCODIGO FROM PRODU WHERE GR1CODIGO=2),
        
        LACRZ AS  (SELECT PROCODIGO FROM PRODU WHERE (PRODESCRICAO LIKE '%CRIZAL%' OR PRODESCRICAO LIKE '%C.FORTE%')  AND GR1CODIGO=2),
        
        MPR AS  (SELECT PROCODIGO FROM PRODU WHERE MARCODIGO IN (128,189,135,106,158,159) AND PROTIPO<>'T'),
        
        TRANS AS  (SELECT PROCODIGO FROM PRODU WHERE (PRODESCRICAO LIKE '%TGEN8%' OR PRODESCRICAO LIKE '%TRANS%'))  

        SELECT PEDDTBAIXA,
                CLICODIGO,
                 PR.PROCODIGO,
                  PDPDESCRICAO,
                   SETOR,
                    CASE 
                     WHEN VX.PROCODIGO IS NOT NULL THEN 'VARILUX'
                      WHEN KD.PROCODIGO IS NOT NULL THEN 'KODAK'
                       WHEN MP.PROCODIGO IS NOT NULL THEN 'MARCA REPRO'
                        WHEN LZ.PROCODIGO IS NOT NULL THEN 'LA CRIZAL'
                         ELSE 'OUTROS' END MARCA,
                          IIF (T.PROCODIGO IS NOT NULL,'TRANSITIONS','') TRANSITIONS,
                           CASE 
                            WHEN L.PROCODIGO IS NOT NULL THEN 'LA'
                             ELSE '' END TIPO,
                              SUM(PDPQTDADE)QTD,
                               SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA 
                                FROM PDPRD PD
                                 INNER JOIN PED P ON PD.ID_PEDIDO=P.ID_PEDIDO
                                  INNER JOIN PROD PR ON PD.PROCODIGO=PR.PROCODIGO
                                   LEFT JOIN VLX VX ON PD.PROCODIGO=VX.PROCODIGO
                                    LEFT JOIN KDK KD ON PD.PROCODIGO=KD.PROCODIGO
                                     LEFT JOIN MPR MP ON PD.PROCODIGO=MP.PROCODIGO
                                      LEFT JOIN LACRZ LZ ON PD.PROCODIGO=LZ.PROCODIGO
                                       LEFT JOIN TRANS T ON PD.PROCODIGO=T.PROCODIGO
                                        LEFT JOIN LA L ON PD.PROCODIGO=L.PROCODIGO
                                         GROUP BY 1,2,3,4,5,6,7,8")


sales_pilares_2021 <- get(load("C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\BASES\\sales_pilares_2021.RData"))

sales_pilares <- union_all(sales_pilares_2021,sales_pilares_2022) %>% filter(!str_detect(MARCA,"OUTROS")) %>% as.data.frame()


##  VALORES ==========================================================================

npilares_setores_marcas <- sales_pilares %>% group_by(SETOR,MARCA) %>% 
  summarize(
    LASTMONTHLASTYEAR=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>=floor_date((Sys.Date()-years(1)) %m-% months(1), 'month') & floor_date(PEDDTBAIXA,"day")<=ceiling_date((Sys.Date()-years(1)) %m-% months(1), 'month') %m-% days(1)],na.rm = TRUE),
    
    LASTMONTHTHISYEAR=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>=floor_date(Sys.Date() %m-% months(1), 'month') & floor_date(PEDDTBAIXA,"day")<=ceiling_date(Sys.Date() %m-% months(1), 'month') %m-% days(1)],na.rm = TRUE),
    
    CURRENTMONTH=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>=floor_date(Sys.Date(), "month") & floor_date(PEDDTBAIXA,"day")<=ceiling_date(Sys.Date(),'month') %m-% days(1)],na.rm = TRUE),
    
    YTD21=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date()-years(1), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date()-years(1), "month")-1],na.rm = TRUE),
    
    YTD22=sum(VRVENDA[floor_date(PEDDTBAIXA,"day") >= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date(), "month")-1],na.rm = TRUE),
    
    MEDIA21=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date()-years(1), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date()-years(1), "month")-1]/as.numeric(length(seq(floor_date(Sys.Date(),"year"),floor_date(Sys.Date(),"month"),by="month"))-1)),
    
    MEDIA22=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") < floor_date(Sys.Date(), "month")]/as.numeric(length(seq(floor_date(Sys.Date(),"year"),floor_date(Sys.Date(),"month"),by="month"))-1)),
    
    PAST12=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")< Sys.Date()],na.rm = TRUE),
    
    SEMRECEITA=sum(VRVENDA[floor_date(PEDDTBAIXA,"day") > ceiling_date((Sys.Date()-years(1)) %m-% months(1), 'month') %m-% days(1)])
  ) %>%  mutate(VAR2022=ifelse(is.finite(YTD22/YTD21-1),YTD22/YTD21-1,0))


npilares_setores_trans <- sales_pilares %>% filter(TRANSITIONS=='TRANSITIONS') %>% group_by(SETOR,TRANSITIONS) %>% 
  summarize(
    LASTMONTHLASTYEAR=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>=floor_date((Sys.Date()-years(1)) %m-% months(1), 'month') & floor_date(PEDDTBAIXA,"day")<=ceiling_date((Sys.Date()-years(1)) %m-% months(1), 'month') %m-% days(1)],na.rm = TRUE),
    
    LASTMONTHTHISYEAR=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>=floor_date(Sys.Date() %m-% months(1), 'month') & floor_date(PEDDTBAIXA,"day")<=ceiling_date(Sys.Date() %m-% months(1), 'month') %m-% days(1)],na.rm = TRUE),
    
    CURRENTMONTH=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>=floor_date(Sys.Date(), "month") & floor_date(PEDDTBAIXA,"day")<=ceiling_date(Sys.Date(),'month') %m-% days(1)],na.rm = TRUE),
    
    YTD21=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date()-years(1), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date()-years(1), "month")-1],na.rm = TRUE),
    
    YTD22=sum(VRVENDA[floor_date(PEDDTBAIXA,"day") >= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date(), "month")-1],na.rm = TRUE),
    
    MEDIA21=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date()-years(1), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date()-years(1), "month")-1]/as.numeric(length(seq(floor_date(Sys.Date(),"year"),floor_date(Sys.Date(),"month"),by="month"))-1)),
    
    MEDIA22=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") < floor_date(Sys.Date(), "month")]/as.numeric(length(seq(floor_date(Sys.Date(),"year"),floor_date(Sys.Date(),"month"),by="month"))-1)),
    
    PAST12=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")< Sys.Date()],na.rm = TRUE),
    
    SEMRECEITA=sum(VRVENDA[floor_date(PEDDTBAIXA,"day") > ceiling_date((Sys.Date()-years(1)) %m-% months(1), 'month') %m-% days(1)])
  ) %>%  mutate(VAR2022=ifelse(is.finite(YTD22/YTD21-1),YTD22/YTD21-1,0)) %>% 
     mutate(MARCA='TRANSITIONS') %>% .[,-2] %>% .[,c(1,12,3:ncol(.)-1)]


npilares_setores <- union_all(npilares_setores_marcas,npilares_setores_trans) %>% arrange(SETOR)

npilares_setores <- npilares_setores %>%  mutate(STATUS=case_when(
  SEMRECEITA==0 ~ 'SEM RECEITA',
  VAR2022>=0 ~ 'CRESCIMENTO',
  VAR2022<0 ~ 'QUEDA',
  TRUE ~ ''
))


LASTMONTHLASTYEAR1 <- toupper(format(floor_date(Sys.Date()-years(1), "month")-1,"%b%/%Y"))

LASTMONTHTHISYEAR1 <- toupper(format(floor_date(Sys.Date(), "month")-1,"%b%/%Y"))

CURRENTMONTH1 <- toupper(format(floor_date(Sys.Date(), "month"),"%b%/%Y"))



npilares_setores <- npilares_setores  %>% 
              rename_at(3:5,~ c(LASTMONTHLASTYEAR1,LASTMONTHTHISYEAR1,CURRENTMONTH1)) %>% 
               .[,c(1:7,12,13,8,9)]



##  GOOGLE ==========================================================================

range_write("1QxZf89jJjmMkwhBtPqy__HEm8wczWva5gc9dVy6tEbQ",
            data=npilares_setores,sheet = "VENDAS",
            range = "A:M",reformat = FALSE)



##  QUANTIDADE ==========================================================================

npilares_setores_marcas_qtd <- sales_pilares %>% group_by(SETOR,MARCA) %>% 
  summarize(
    LASTMONTHLASTYEAR=sum(QTD[floor_date(PEDDTBAIXA,"day")>=floor_date((Sys.Date()-years(1)) %m-% months(1), 'month') & floor_date(PEDDTBAIXA,"day")<=ceiling_date((Sys.Date()-years(1)) %m-% months(1), 'month') %m-% days(1)],na.rm = TRUE),
    
    LASTMONTHTHISYEAR=sum(QTD[floor_date(PEDDTBAIXA,"day")>=floor_date(Sys.Date() %m-% months(1), 'month') & floor_date(PEDDTBAIXA,"day")<=ceiling_date(Sys.Date() %m-% months(1), 'month') %m-% days(1)],na.rm = TRUE),
    
    CURRENTMONTH=sum(QTD[floor_date(PEDDTBAIXA,"day")>=floor_date(Sys.Date(), "month") & floor_date(PEDDTBAIXA,"day")<=ceiling_date(Sys.Date(),'month') %m-% days(1)],na.rm = TRUE),
    
    YTD21=sum(QTD[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date()-years(1), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date()-years(1), "month")-1],na.rm = TRUE),
    
    YTD22=sum(QTD[floor_date(PEDDTBAIXA,"day") >= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date(), "month")-1],na.rm = TRUE),
    
    MEDIA21=sum(QTD[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date()-years(1), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date()-years(1), "month")-1]/as.numeric(length(seq(floor_date(Sys.Date(),"year"),floor_date(Sys.Date(),"month"),by="month"))-1)),
    
    MEDIA22=sum(QTD[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") < floor_date(Sys.Date(), "month")]/as.numeric(length(seq(floor_date(Sys.Date(),"year"),floor_date(Sys.Date(),"month"),by="month"))-1)),
    
    PAST12=sum(QTD[floor_date(PEDDTBAIXA,"day")< Sys.Date()],na.rm = TRUE),
    
    SEMRECEITA=sum(QTD[floor_date(PEDDTBAIXA,"day") > ceiling_date((Sys.Date()-years(1)) %m-% months(1), 'month') %m-% days(1)])
  ) %>%  mutate(VAR2022=ifelse(is.finite(YTD22/YTD21-1),YTD22/YTD21-1,0))


npilares_setores_trans_qtd <- sales_pilares %>% filter(TRANSITIONS=='TRANSITIONS') %>% group_by(SETOR,TRANSITIONS) %>% 
  summarize(
    LASTMONTHLASTYEAR=sum(QTD[floor_date(PEDDTBAIXA,"day")>=floor_date((Sys.Date()-years(1)) %m-% months(1), 'month') & floor_date(PEDDTBAIXA,"day")<=ceiling_date((Sys.Date()-years(1)) %m-% months(1), 'month') %m-% days(1)],na.rm = TRUE),
    
    LASTMONTHTHISYEAR=sum(QTD[floor_date(PEDDTBAIXA,"day")>=floor_date(Sys.Date() %m-% months(1), 'month') & floor_date(PEDDTBAIXA,"day")<=ceiling_date(Sys.Date() %m-% months(1), 'month') %m-% days(1)],na.rm = TRUE),
    
    CURRENTMONTH=sum(QTD[floor_date(PEDDTBAIXA,"day")>=floor_date(Sys.Date(), "month") & floor_date(PEDDTBAIXA,"day")<=ceiling_date(Sys.Date(),'month') %m-% days(1)],na.rm = TRUE),
    
    YTD21=sum(QTD[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date()-years(1), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date()-years(1), "month")-1],na.rm = TRUE),
    
    YTD22=sum(QTD[floor_date(PEDDTBAIXA,"day") >= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date(), "month")-1],na.rm = TRUE),
    
    MEDIA21=sum(QTD[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date()-years(1), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date()-years(1), "month")-1]/as.numeric(length(seq(floor_date(Sys.Date(),"year"),floor_date(Sys.Date(),"month"),by="month"))-1)),
    
    MEDIA22=sum(QTD[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") < floor_date(Sys.Date(), "month")]/as.numeric(length(seq(floor_date(Sys.Date(),"year"),floor_date(Sys.Date(),"month"),by="month"))-1)),
    
    PAST12=sum(QTD[floor_date(PEDDTBAIXA,"day")< Sys.Date()],na.rm = TRUE),
    
    SEMRECEITA=sum(QTD[floor_date(PEDDTBAIXA,"day") > ceiling_date((Sys.Date()-years(1)) %m-% months(1), 'month') %m-% days(1)])
  ) %>%  mutate(VAR2022=ifelse(is.finite(YTD22/YTD21-1),YTD22/YTD21-1,0)) %>% 
  mutate(MARCA='TRANSITIONS') %>% .[,-2] %>% .[,c(1,12,3:ncol(.)-1)]


npilares_setores_qtd <- union_all(npilares_setores_marcas_qtd,npilares_setores_trans_qtd) %>% arrange(SETOR)

npilares_setores_qtd <- npilares_setores_qtd %>%  mutate(STATUS=case_when(
  SEMRECEITA==0 ~ 'SEM RECEITA',
  VAR2022>=0 ~ 'CRESCIMENTO',
  VAR2022<0 ~ 'QUEDA',
  TRUE ~ ''
))


LASTMONTHLASTYEAR1 <- toupper(format(floor_date(Sys.Date()-years(1), "month")-1,"%b%/%Y"))

LASTMONTHTHISYEAR1 <- toupper(format(floor_date(Sys.Date(), "month")-1,"%b%/%Y"))

CURRENTMONTH1 <- toupper(format(floor_date(Sys.Date(), "month"),"%b%/%Y"))



npilares_setores_qtd <- npilares_setores_qtd  %>% 
  rename_at(3:5,~ c(LASTMONTHLASTYEAR1,LASTMONTHTHISYEAR1,CURRENTMONTH1)) %>% 
  .[,c(1:7,12,13,8,9)]



##  GOOGLE ==========================================================================

range_write("1QxZf89jJjmMkwhBtPqy__HEm8wczWva5gc9dVy6tEbQ",
            data=npilares_setores_qtd,sheet = "QUANTIDADE",
            range = "A:K",reformat = FALSE)

## THE END





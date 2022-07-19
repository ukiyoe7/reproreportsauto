
## RANKING DE CIDADES
## 06.2022
## SANDRO JAKOSKA

## LIBRARIES =======================================================================

library(tidyverse)
library(lubridate)
library(reshape2)
library(DBI)
library(googlesheets4)

## DB CONNECTION ====================================================================

con2 <- dbConnect(odbc::odbc(), "reproreplica")


##  SALES ==========================================================================

sales_city_2022 <- dbGetQuery(con2,"
WITH CLI AS (SELECT C.CLICODIGO,CIDADE,SETOR FROM CLIEN C
LEFT JOIN (SELECT CLICODIGO,CIDNOME CIDADE, ZODESCRICAO SETOR FROM ENDCLI E
           LEFT JOIN CIDADE CD ON E.CIDCODIGO=CD.CIDCODIGO
           LEFT JOIN (SELECT ZOCODIGO,ZODESCRICAO FROM ZONA 
                       WHERE ZOCODIGO IN(20,21,22,23,24,28))Z ON E.ZOCODIGO=Z.ZOCODIGO
           WHERE ENDFAT='S'
) ED ON C.CLICODIGO=ED.CLICODIGO
WHERE CLICLIENTE='S'),
  
FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','SR','R')),
  
PED AS (SELECT ID_PEDIDO,PEDDTBAIXA,CIDADE FROM PEDID P
   INNER JOIN FIS F ON P.FISCODIGO1=F.FISCODIGO
    INNER JOIN CLI C ON P.CLICODIGO=C.CLICODIGO
     WHERE PEDSITPED<>'C' AND PEDDTBAIXA BETWEEN '01.01.2022' AND 'TODAY'),
     
AUX AS (SELECT PROCODIGO,PROTIPO FROM PRODU)     

SELECT 
 PEDDTBAIXA,
  P.CIDADE,
   SUM(PDPQTDADE) QTD,
    SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA  
     FROM PDPRD PD
      INNER JOIN PED P ON PD.ID_PEDIDO=P.ID_PEDIDO
       INNER JOIN AUX ON PD.PROCODIGO= AUX.PROCODIGO
        GROUP BY 1,2") 

sales_city_2021 <- get(load("C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\BASES\\sales_citY_2021.RData"))

sales_city <- union_all(sales_city_2021,sales_city_2022)



##  RANKING CLIENTS ==========================================================================

nsales_city <- sales_city %>% group_by(CIDADE) %>% 
  summarize(
    LASTMONTHLASTYEAR=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>=floor_date((Sys.Date()-years(1)) %m-% months(1), 'month') & floor_date(PEDDTBAIXA,"day")<=ceiling_date((Sys.Date()-years(1)) %m-% months(1), 'month') %m-% days(1)],na.rm = TRUE),
    
    LASTMONTHTHISYEAR=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>=floor_date(Sys.Date() %m-% months(1), 'month') & floor_date(PEDDTBAIXA,"day")<=ceiling_date(Sys.Date() %m-% months(1), 'month') %m-% days(1)],na.rm = TRUE),
    
    CURRENTMONTH=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>=floor_date(Sys.Date(), "month") & floor_date(PEDDTBAIXA,"day")<=ceiling_date(Sys.Date(),'month') %m-% days(1)],na.rm = TRUE),
    
    YTD21=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date()-years(1), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date()-years(1), "month")-1],na.rm = TRUE),
    
    YTD22=sum(VRVENDA[floor_date(PEDDTBAIXA,"day") >= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date(), "month")-1],na.rm = TRUE),
    
    MEDIA21=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date()-years(1), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date()-years(1), "month")-1]/as.numeric(length(seq(floor_date(Sys.Date(),"year"),floor_date(Sys.Date(),"month"),by="month"))-1)),
    
    MEDIA22=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") < floor_date(Sys.Date(), "month")]/as.numeric(length(seq(floor_date(Sys.Date(),"year"),floor_date(Sys.Date(),"month"),by="month"))-1)),
    
    PAST12=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")< Sys.Date()],na.rm = TRUE),
    
    SEMRECEITA=sum(VRVENDA[floor_date(PEDDTBAIXA,"day") >  ceiling_date((Sys.Date()-years(1)) %m-% months(1), 'month') %m-% days(1)])
  ) %>% mutate(VAR2022=ifelse(is.finite(YTD22/YTD21-1),YTD22/YTD21-1,0))


nsales_city %>% mutate(across(2:5,round,2)) %>% view()



nsales_city <- nsales_city %>%  mutate(STATUS=case_when(
  SEMRECEITA==0 ~ 'SEM RECEITA',
  VAR2022>=0 ~ 'CRESCIMENTO',
  VAR2022<0 ~ 'QUEDA',
  PAST12==0  ~ 'PERDIDO',
  YTD21==0 & YTD22>0 ~ 'RECUPERADO',
  TRUE ~ ''
))

LASTMONTHLASTYEAR1 <- toupper(format(floor_date(Sys.Date()-years(1), "month")-1,"%b%/%Y"))

LASTMONTHTHISYEAR1 <- toupper(format(floor_date(Sys.Date(), "month")-1,"%b%/%Y"))

CURRENTMONTH1 <- toupper(format(floor_date(Sys.Date(), "month"),"%b%/%Y"))

nsales_city <- nsales_city %>% arrange(desc(.$YTD22)) %>% as.data.frame() %>% 
  rename_at(2:4,~ c(LASTMONTHLASTYEAR1,LASTMONTHTHISYEAR1,CURRENTMONTH1)) %>% .[,c(1:6,11:12,7:8)]




##  GOOGLE ==========================================================================

range_write("1GpUPX7RQWL-TDrujKNhDYrKSZ5VzmumZPE8a3TXwaek",
            data=nsales_city,sheet = "CIDADES",
            range = "A:J",reformat = FALSE)



## the end


## RANKING DE FILIAIS
## 05.2022
## SANDRO JAKOSKA

## LIBRARIES =======================================================================

library(tidyverse)
library(lubridate)
library(reshape2)
library(DBI)
library(googlesheets4)

## DB CONNECTION ====================================================================

con2 <- dbConnect(odbc::odbc(), "reproreplica")


## CLIENTS ==========================================================================


clientes <- dbGetQuery(con2,"
SELECT 
 DISTINCT C.CLICODIGO,
  CLINOMEFANT NOMEFANTASIA, 
    C.GCLCODIGO CODGRUPO,
     GCLNOME GRUPO,
      E.EMPCODIGO,
       REPLACE(EMPNOMEFNT,'REPRO - ','') FILIAL,
        CLIDTCAD DATACADASTRO
         FROM CLIEN C
          LEFT JOIN CLIEMP E ON C.CLICODIGO=E.CLICODIGO 
           LEFT JOIN EMPRESA P ON E.EMPCODIGO=P.EMPCODIGO
            LEFT JOIN GRUPOCLI GC ON C.GCLCODIGO=GC.GCLCODIGO
             WHERE CLICLIENTE='S'")



inativos <- dbGetQuery(con2,"
 SELECT DISTINCT SITCLI.CLICODIGO 
                   FROM SITCLI
                    INNER JOIN (SELECT DISTINCT SITCLI.CLICODIGO,
                     MAX(SITDATA)ULTIMA 
                      FROM SITCLI GROUP BY 1)A ON SITCLI.CLICODIGO=A.CLICODIGO AND A.ULTIMA=SITCLI.SITDATA 
                       INNER JOIN (SELECT DISTINCT SITCLI.CLICODIGO,
                                           SITDATA,
                                            MAX(SITSEQ)USEQ 
                                             FROM SITCLI
                                              GROUP BY 1,2)MSEQ ON A.CLICODIGO=MSEQ.CLICODIGO AND MSEQ.SITDATA=A.ULTIMA AND MSEQ.USEQ=SITCLI.SITSEQ
                                               WHERE SITCODIGO=4")


pedid2022 <- dbGetQuery(con2,"
WITH FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','SR','R'))
 SELECT PEDDTBAIXA,
        P.CLICODIGO, 
         COUNT( DISTINCT ID_PEDIDO) QTD
          FROM PEDID P
           INNER JOIN FIS F ON P.FISCODIGO1=F.FISCODIGO
            WHERE PEDSITPED<>'C' AND PEDDTBAIXA BETWEEN '01.01.2022' AND '31.12.2022'
             GROUP BY 1,2") 

pedid2021 <- dbGetQuery(con2,"
WITH FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','SR','R'))


SELECT PEDDTBAIXA,
        P.CLICODIGO, 
         COUNT( DISTINCT ID_PEDIDO) QTD
          FROM PEDID P
           INNER JOIN FIS F ON P.FISCODIGO1=F.FISCODIGO
            WHERE PEDSITPED<>'C' AND PEDDTBAIXA BETWEEN '01.01.2021' AND '31.12.2021'
             GROUP BY 1,2") 


pedidos <- union_all(pedid2021,pedid2022)


npedidos <- pedidos %>% 
             group_by(CLICODIGO) %>% 
                summarize(
LASTMONTHLASTYEAR=sum(QTD[floor_date(PEDDTBAIXA,"day")>=floor_date((Sys.Date()-years(1)) %m-% months(1), 'month') & floor_date(PEDDTBAIXA,"day")<=ceiling_date((Sys.Date()-years(1)) %m-% months(1), 'month') %m-% days(1)],na.rm = TRUE),
                  
LASTMONTHTHISYEAR=sum(QTD[floor_date(PEDDTBAIXA,"day")>=floor_date(Sys.Date() %m-% months(1), 'month') & floor_date(PEDDTBAIXA,"day")<=ceiling_date(Sys.Date() %m-% months(1), 'month') %m-% days(1)],na.rm = TRUE),
                  
CURRENTMONTH=sum(QTD[floor_date(PEDDTBAIXA,"day")>=floor_date(Sys.Date(), "month") & floor_date(PEDDTBAIXA,"day")<=ceiling_date(Sys.Date(),'month') %m-% days(1)],na.rm = TRUE),
                  
YTD21=sum(QTD[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date()-years(1), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date()-years(1), "month")-1],na.rm = TRUE),
                  
YTD22=sum(QTD[floor_date(PEDDTBAIXA,"day") >= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date(), "month")-1],na.rm = TRUE),
                  
MEDIA21=sum(QTD[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date()-years(1), "year") & floor_date(PEDDTBAIXA,"day") <= floor_date(Sys.Date()-years(1), "month")-1]/as.numeric(length(seq(floor_date(Sys.Date(),"year"),floor_date(Sys.Date(),"month"),by="month"))-1)),
                  
MEDIA22=sum(QTD[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") < floor_date(Sys.Date(), "month")]/as.numeric(length(seq(floor_date(Sys.Date(),"year"),floor_date(Sys.Date(),"month"),by="month"))-1)),
                  
PAST12=sum(QTD[floor_date(PEDDTBAIXA,"day")< Sys.Date()],na.rm = TRUE),
                  
SEMRECEITA=sum(QTD[floor_date(PEDDTBAIXA,"day") >  ceiling_date((Sys.Date()-years(1)) %m-% months(1), 'month') %m-% days(1)])

) %>% mutate(VAR2022=ifelse(is.finite(YTD22/YTD21-1),YTD22/YTD21-1,0))

npedidos <- apply(npedidos,2,function(x) round(x,2)) %>% as.data.frame()


npedidos2 <- left_join(anti_join(clientes,inativos,by="CLICODIGO"),npedidos,by="CLICODIGO")%>% as.data.frame()

npedidos2 <- npedidos2 %>%  mutate(STATUS=case_when(
  DATACADASTRO>=floor_date(floor_date(Sys.Date() %m-% months(1), 'month')-years(1), "month") ~ 'CLIENTE NOVO',
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


npedidos3 <- npedidos2 %>% 
              arrange(desc(.$YTD22)) %>% 
               as.data.frame() %>% 
                rename_at(8:10,~ c(LASTMONTHLASTYEAR1,LASTMONTHTHISYEAR1,CURRENTMONTH1)) %>% 
                 .[,c(1:4,6,8:12,17:18,13:14)] %>% 
                  filter(!is.na(FILIAL))





##  GOOGLE ==========================================================================

range_write("1Jnaol2MHFUbCFwi5QsLkkcZYp92Bkgo8gSqvYjRtuIM",
            data=npedidos3,sheet = "RANKING",
            range = "A:N",reformat = FALSE)

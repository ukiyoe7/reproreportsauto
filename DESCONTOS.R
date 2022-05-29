## RELATORIO DE DESCONTOS COMERCIAIS
## SANDRO JAKOSKA 15_02_2022

library(tidyverse)
library(lubridate)
library(googlesheets4)
library(DBI)
library(gmailr)
library(xlsx)

con2 <- dbConnect(odbc::odbc(), "reproreplica")


gm_auth_configure(path = "C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\sendmail.json")

descontos_repro <-get(load("C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\BASES\\descontos_repro.RData"))


## BASE CLIENTES ==================================================================================

clientes <- dbGetQuery(con2,"
    SELECT DISTINCT 
    C.CLICODIGO,
    CLINOMEFANT NOMEFANTASIA, 
    C.GCLCODIGO CODGRUPO,
    GCLNOME GRUPO,
    ZODESCRICAO SETOR
    FROM CLIEN C
    INNER JOIN 
    (SELECT DISTINCT CLICODIGO,ZODESCRICAO FROM ENDCLI E
    INNER JOIN (SELECT ZOCODIGO,ZODESCRICAO FROM ZONA)Z
    ON E.ZOCODIGO=Z.ZOCODIGO 
    AND E.ZOCODIGO IN (20,21,22,23,24,25,28)
    AND ENDFAT='S')A ON C.CLICODIGO=A.CLICODIGO
    LEFT JOIN GRUPOCLI ON C.GCLCODIGO=GRUPOCLI.GCLCODIGO
    WHERE CLICLIENTE='S' 
    ")

inativos <- dbGetQuery(con2,"SELECT DISTINCT SITCLI.CLICODIGO  FROM SITCLI
    INNER JOIN (SELECT DISTINCT SITCLI.CLICODIGO,MAX(SITDATA)ULTIMA FROM SITCLI
    GROUP BY 1)A ON SITCLI.CLICODIGO=A.CLICODIGO AND A.ULTIMA=SITCLI.SITDATA 
    INNER JOIN (SELECT DISTINCT SITCLI.CLICODIGO,SITDATA,MAX(SITSEQ)USEQ FROM SITCLI
    GROUP BY 1,2)MSEQ ON A.CLICODIGO=MSEQ.CLICODIGO AND MSEQ.SITDATA=A.ULTIMA AND MSEQ.USEQ=SITCLI.SITSEQ
    WHERE SITCODIGO=4")

cli <- anti_join(clientes,inativos,by="CLICODIGO") 




## SALES =================================================================================================

## SALES VARILUX

sales_cli_vlx_2021 <- get(load("C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\BASES\\sales_cli_vlx_2021.RData"))


sales_cli_vlx_2022 <- dbGetQuery(con2,"
    WITH CLI AS (SELECT DISTINCT CLIEN.CLICODIGO,CLINOMEFANT
      FROM CLIEN
       WHERE CLICLIENTE='S'),
      
    FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','SR','R')),
      
    PED AS (SELECT ID_PEDIDO,PEDDTBAIXA,P.CLICODIGO FROM PEDID P
       INNER JOIN FIS F ON P.FISCODIGO1=F.FISCODIGO
        INNER JOIN CLI C ON P.CLICODIGO=C.CLICODIGO
         WHERE PEDSITPED<>'C' AND PEDDTBAIXA BETWEEN '01.01.2022' AND 'TODAY'),
         
    AUX AS (SELECT PROCODIGO FROM PRODU WHERE MARCODIGO=57)     
    
    SELECT PEDDTBAIXA,
    P.CLICODIGO,
     SUM(PDPQTDADE) QTD,
      SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA  
      FROM PDPRD PD
       INNER JOIN PED P ON PD.ID_PEDIDO=P.ID_PEDIDO
        INNER JOIN AUX ON PD.PROCODIGO= AUX.PROCODIGO
         GROUP BY 1,2")

sales_cli_vlx_2021_2022 <- union(sales_cli_vlx_2021,sales_cli_vlx_2022)


sales_cli_vlx <- sales_cli_vlx_2021_2022 %>% group_by(CLICODIGO) %>% 
  summarize(
    MEDIA21=sum(VRVENDA[floor_date(PEDDTBAIXA,"year")==as.Date("2021-01-01")]/12),
    
    MEDIA22=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") < floor_date(Sys.Date(), "month")]/as.numeric(length(seq(floor_date(Sys.Date(),"year"),floor_date(Sys.Date(),"month"),by="month"))-1)),
  ) 

sales_cli_vlx <- apply(sales_cli_vlx,2,function(x) round(x,2)) %>% as.data.frame()


## SALES KODAK

sales_cli_kdk_2021 <- get(load("C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\BASES\\sales_cli_kdk_2021.RData"))


sales_cli_kdk_2022 <- dbGetQuery(con2,"
    WITH CLI AS (SELECT DISTINCT CLIEN.CLICODIGO,CLINOMEFANT
      FROM CLIEN
       WHERE CLICLIENTE='S'),
      
    FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','SR','R')),
      
    PED AS (SELECT ID_PEDIDO,PEDDTBAIXA,P.CLICODIGO FROM PEDID P
       INNER JOIN FIS F ON P.FISCODIGO1=F.FISCODIGO
        INNER JOIN CLI C ON P.CLICODIGO=C.CLICODIGO
         WHERE PEDSITPED<>'C' AND PEDDTBAIXA BETWEEN '01.01.2022' AND 'TODAY'),
         
    AUX AS (SELECT PROCODIGO FROM PRODU WHERE MARCODIGO=24)     
    
    SELECT PEDDTBAIXA,
    P.CLICODIGO,
     SUM(PDPQTDADE) QTD,
      SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA  
      FROM PDPRD PD
       INNER JOIN PED P ON PD.ID_PEDIDO=P.ID_PEDIDO
        INNER JOIN AUX ON PD.PROCODIGO= AUX.PROCODIGO
         GROUP BY 1,2")


sales_cli_kdk_2021_2022 <- union(sales_cli_kdk_2021,sales_cli_kdk_2022)


sales_cli_kdk <- sales_cli_kdk_2021_2022 %>% group_by(CLICODIGO) %>% 
  summarize(
    MEDIA21=sum(VRVENDA[floor_date(PEDDTBAIXA,"year")==as.Date("2021-01-01")]/12),
    
    MEDIA22=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") < floor_date(Sys.Date(), "month")]/as.numeric(length(seq(floor_date(Sys.Date(),"year"),floor_date(Sys.Date(),"month"),by="month"))-1)),
  ) 

sales_cli_kdk <- apply(sales_cli_kdk,2,function(x) round(x,2)) %>% as.data.frame()


## SALES MREPRO

sales_cli_mrepro_2021 <- get(load("C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\BASES\\sales_cli_mrepro_2021.RData"))


sales_cli_mrepro_2022 <- dbGetQuery(con2,"
    WITH CLI AS (SELECT DISTINCT CLIEN.CLICODIGO,CLINOMEFANT
      FROM CLIEN
       WHERE CLICLIENTE='S'),
      
    FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','SR','R')),
      
    PED AS (SELECT ID_PEDIDO,PEDDTBAIXA,P.CLICODIGO FROM PEDID P
       INNER JOIN FIS F ON P.FISCODIGO1=F.FISCODIGO
        INNER JOIN CLI C ON P.CLICODIGO=C.CLICODIGO
         WHERE PEDSITPED<>'C' AND PEDDTBAIXA BETWEEN '01.01.2022' AND 'TODAY'),
         
    AUX AS (SELECT PROCODIGO FROM PRODU WHERE MARCODIGO IN (158,159,135,128,106,189))     
    
    SELECT PEDDTBAIXA,
    P.CLICODIGO,
     SUM(PDPQTDADE) QTD,
      SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA  
      FROM PDPRD PD
       INNER JOIN PED P ON PD.ID_PEDIDO=P.ID_PEDIDO
        INNER JOIN AUX ON PD.PROCODIGO= AUX.PROCODIGO
         GROUP BY 1,2")

sales_cli_mrepro_2021_2022 <- union(sales_cli_mrepro_2021,sales_cli_mrepro_2022)


sales_cli_mrepro <- sales_cli_mrepro_2021_2022 %>% group_by(CLICODIGO) %>% 
  summarize(
    MEDIA21=sum(VRVENDA[floor_date(PEDDTBAIXA,"year")==as.Date("2021-01-01")]/12),
    
    MEDIA22=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") < floor_date(Sys.Date(), "month")]/as.numeric(length(seq(floor_date(Sys.Date(),"year"),floor_date(Sys.Date(),"month"),by="month"))-1)),
  ) 

sales_cli_mrepro <- apply(sales_cli_mrepro,2,function(x) round(x,2)) %>% as.data.frame()


## SALES GERAL

sales_cli_geral_2021 <- get(load("C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\BASES\\sales_cli_geral_2021.RData"))


sales_cli_geral_2022 <- dbGetQuery(con2,"
    WITH CLI AS (SELECT DISTINCT CLIEN.CLICODIGO,CLINOMEFANT
      FROM CLIEN
       WHERE CLICLIENTE='S'),
      
    FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','SR','R')),
      
    PED AS (SELECT ID_PEDIDO,PEDDTBAIXA,P.CLICODIGO FROM PEDID P
       INNER JOIN FIS F ON P.FISCODIGO1=F.FISCODIGO
        INNER JOIN CLI C ON P.CLICODIGO=C.CLICODIGO
         WHERE PEDSITPED<>'C' AND PEDDTBAIXA BETWEEN '01.01.2022' AND 'TODAY')
    
    SELECT PEDDTBAIXA,
    P.CLICODIGO,
     SUM(PDPQTDADE) QTD,
      SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA  
      FROM PDPRD PD
       INNER JOIN PED P ON PD.ID_PEDIDO=P.ID_PEDIDO
         GROUP BY 1,2")

sales_cli_geral_2021_2022 <- union(sales_cli_geral_2021,sales_cli_geral_2022)


sales_cli_geral <- sales_cli_geral_2021_2022 %>% group_by(CLICODIGO) %>% 
  summarize(
    MEDIA21=sum(VRVENDA[floor_date(PEDDTBAIXA,"year")==as.Date("2021-01-01")]/12),
    
    MEDIA22=sum(VRVENDA[floor_date(PEDDTBAIXA,"day")>= floor_date(Sys.Date(), "year") & floor_date(PEDDTBAIXA,"day") < floor_date(Sys.Date(), "month")]/as.numeric(length(seq(floor_date(Sys.Date(),"year"),floor_date(Sys.Date(),"month"),by="month"))-1)),
  ) 

sales_cli_geral <- apply(sales_cli_geral,2,function(x) round(x,2)) %>% as.data.frame()



## DISCOUNTS ===========================================================================


desct_vlx <- dbGetQuery(con2,"
    SELECT CLICODIGO,
     TBPCODIGO,
      TBPDESC2,
       CASE 
        WHEN TBPCODIGO=101 THEN 'VARILUX'
        WHEN TBPCODIGO=102 THEN 'VARILUX' 
        WHEN TBPCODIGO=103 THEN 'VARILUX' 
        WHEN TBPCODIGO=104 THEN 'VARILUX' 
        WHEN TBPCODIGO=105 THEN 'VARILUX'
       ELSE '' END LINHA
      FROM CLITBP
      WHERE TBPCODIGO IN (101,102,103,104,105)
      ") 

desct_vlx2 <- desct_vlx %>% group_by(CLICODIGO) %>% 
  summarize(DESCTO_VLX=round(mean(TBPDESC2,na.rm = TRUE),1))


desct_kdk <- dbGetQuery(con2,"
    SELECT CLICODIGO,
     TBPCODIGO,
      TBPDESC2,
       CASE 
        WHEN TBPCODIGO=201 THEN 'KODAK'
        WHEN TBPCODIGO=202 THEN 'KODAK' 
       ELSE '' END LINHA
      FROM CLITBP
      WHERE TBPCODIGO IN (201,202)
      ") 

desct_kdk2 <- desct_kdk %>% group_by(CLICODIGO) %>% 
  summarize(DESCTO_KDK=round(mean(TBPDESC2,na.rm = TRUE),1)) 


desct_mrepro <- dbGetQuery(con2,"
    SELECT CLICODIGO,
     TBPCODIGO,
      TBPDESC2,
       CASE 
        WHEN TBPCODIGO=301 THEN 'REPRO'
        WHEN TBPCODIGO=302 THEN 'REPRO' 
        WHEN TBPCODIGO=303 THEN 'REPRO'
        WHEN TBPCODIGO=304 THEN 'REPRO'
        WHEN TBPCODIGO=305 THEN 'REPRO'
        WHEN TBPCODIGO=306 THEN 'REPRO'
        WHEN TBPCODIGO=308 THEN 'REPRO'
       ELSE '' END LINHA
      FROM CLITBP
      WHERE TBPCODIGO IN (301,302,303,304,305,306,308)
      ") 

desct_mrepro <- desct_mrepro %>% group_by(CLICODIGO) %>% 
  summarize(DESCTO_MREPRO=round(mean(TBPDESC2,na.rm = TRUE),1)) 


desct_geral <- dbGetQuery(con2,"
    SELECT DISTINCT 
    C.CLICODIGO,
    CLIPCDESCPRODU DESCTO_GERAL
    FROM CLIEN C
    WHERE CLICLIENTE='S'
    ") %>% anti_join(.,inativos,by="CLICODIGO")



descontos <- cli %>% mutate(DATA=format(Sys.Date(),"%d/%m%/%y")) %>% 
  left_join(.,desct_vlx2,by="CLICODIGO") %>% 
  left_join(.,desct_kdk2,by="CLICODIGO") %>% 
  left_join(.,desct_geral,by="CLICODIGO") %>% 
  left_join(.,desct_mrepro,by="CLICODIGO") 

descontos_repro <- union_all(desconto_repro,descontos)


save(descontos_repro,file = "C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\BASES\\descontos_repro.RData")


## TRANSFORM AND WRITE GG ======================================================================================


## VARILUX 

descontos_vlx <- descontos %>% mutate(row = row_number()) %>%
  .[,c(1:6,7)] %>% pivot_wider(names_from = DATA,values_from = DESCTO_VLX,values_fn = {mean}) %>% as.data.frame()

descontos_vlx <- left_join(descontos_vlx,sales_cli_vlx,by="CLICODIGO") %>%  as.data.frame() 

range_write("1JR1yosWO9uAP6olKYbDSRtUt2eDeNykaxOiIxRfCXT4",data=descontos_vlx,sheet = "VARILUX", range = "A1",reformat = FALSE)


## KODAK

descontos_kdk <- descontos %>% 
  .[,c(1:6,8)] %>% pivot_wider(names_from = DATA,values_from = DESCTO_KDK,values_fn = {mean}) %>% as.data.frame()

descontos_kdk <- left_join(descontos_kdk,sales_cli_kdk,by="CLICODIGO") %>%  as.data.frame() 

range_write("1JR1yosWO9uAP6olKYbDSRtUt2eDeNykaxOiIxRfCXT4",data=descontos_kdk,sheet = "KODAK", range = "A1",reformat = FALSE)


## MARCA REPRO

descontos_mrepro <- descontos %>% 
  .[,c(1:6,10)] %>% pivot_wider(names_from = DATA,values_from = DESCTO_MREPRO,values_fn = {mean}) %>% as.data.frame()

descontos_mrepro <- left_join(descontos_mrepro,sales_cli_mrepro,by="CLICODIGO") %>%  as.data.frame() 

range_write("1JR1yosWO9uAP6olKYbDSRtUt2eDeNykaxOiIxRfCXT4",data=descontos_mrepro,sheet = "MARCAREPRO", range = "A1",reformat = FALSE)


## DESCONTOS GERAL

descontos_geral <- descontos %>% 
  .[,c(1:6,9)] %>% pivot_wider(names_from = DATA,values_from = DESCTO_GERAL,values_fn = {mean}) %>% as.data.frame()

descontos_geral <- left_join(descontos_geral,sales_cli_geral,by="CLICODIGO") %>%  as.data.frame() 

range_write("1JR1yosWO9uAP6olKYbDSRtUt2eDeNykaxOiIxRfCXT4",data=descontos_geral,sheet = "GERAL", range = "A1",reformat = FALSE)




## WRITE XLS ==================================================================


filewd <-  paste0("C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\BASES\\descontos","_",format(Sys.Date(),"%d_%m_%y"),".xlsx")

write.xlsx(descontos_vlx, file = filewd,row.names=FALSE,sheetName = "VARILUX")

write.xlsx(descontos_kdk, file = filewd,row.names=FALSE,sheetName = "KODAK", append = TRUE)

write.xlsx(descontos_mrepro, file = filewd,row.names=FALSE,sheetName = "MARCAREPRO", append = TRUE)

write.xlsx(descontos_geral, file = filewd,row.names=FALSE,sheetName = "GERAL", append = TRUE)


## SEND MAIL ==================================================================


mymaildesconto <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO DESCONTOS RELREPRO") %>%
  gm_text_body("Segue anexo relatorio.Esse e um email automatico.") %>% 
  gm_attach_file(filewd)

gm_send_message(mymaildesconto)



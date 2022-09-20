## RESIDUAL PROMO EM DOBRO

library(DBI)
library(dplyr)
library(reshape2)
library(googlesheets4)
library(glue)
con2 <- dbConnect(odbc::odbc(), "reproreplica")

## GET IDS FROM GGSHEETS

residual <- range_read("15Or0FoTTDZBBSGuH_zPGYO9jD3QV97QNYtt9336u30w",sheet = "DADOS" , range = "B:C") %>% 
  mutate(HASH=paste0(ID1,ID2))


## QUERY ID1

residual_id1 <- residual %>% select(ID1) %>% rename("ID_PEDIDO"="ID1") %>% filter(!is.na(.))

residual_promo1_sql <- glue_sql("
    WITH PED AS (SELECT ID_PEDIDO,PEDVRTOTAL FROM PEDID WHERE 
    ID_PEDIDO IN ({residual_id1$ID_PEDIDO*})
    AND PEDSITPED<>'C'),
    
    LEN AS (SELECT PROCODIGO,
                    PRODESCRICAO,
                     IIF(PROCODIGO2 IS NULL,
                      PROCODIGO,PROCODIGO2)CHAVE FROM PRODU WHERE PROTIPO NOT IN ('T','M','C','S','X','K')),
    
    TRAT AS (SELECT PROCODIGO,PRODESCRICAO FROM PRODU WHERE PROTIPO='T'),
    
    COL AS (SELECT PROCODIGO,PRODESCRICAO FROM PRODU WHERE PROTIPO='C'),
  
    PED1 AS (SELECT DISTINCT P.ID_PEDIDO,
                      CHAVE LENTE_COD,
                       (SELECT PRODESCRICAO FROM PRODU WHERE PROCODIGO=CHAVE) LENTE
     FROM PDPRD P
      INNER JOIN PED ON P.ID_PEDIDO=PED.ID_PEDIDO
       INNER JOIN LEN L ON L.PROCODIGO=P.PROCODIGO),
       
    PED2 AS (SELECT DISTINCT P.ID_PEDIDO,
                      P.PROCODIGO TRAT_COD,
                        (SELECT PRODESCRICAO FROM PRODU WHERE P.PROCODIGO=PROCODIGO) TRAT
        FROM PDPRD P
         INNER JOIN PED ON P.ID_PEDIDO=PED.ID_PEDIDO
          INNER JOIN TRAT T ON T.PROCODIGO=P.PROCODIGO),
          
    PED3 AS (SELECT DISTINCT P.ID_PEDIDO,
                      P.PROCODIGO COL_COD,
                        (SELECT PRODESCRICAO FROM PRODU WHERE P.PROCODIGO=PROCODIGO) COL
        FROM PDPRD P
         INNER JOIN PED ON P.ID_PEDIDO=PED.ID_PEDIDO
          INNER JOIN COL C ON C.PROCODIGO=P.PROCODIGO)
    
    SELECT P.ID_PEDIDO,
             LENTE_COD,
              LENTE,
               TRAT_COD,
                TRAT,
                 COL_COD,
                  COL,
                   PEDVRTOTAL
                    FROM PED P
                     LEFT JOIN PED1 ON  PED1.ID_PEDIDO=P.ID_PEDIDO
                      LEFT JOIN PED2 ON  PED2.ID_PEDIDO=P.ID_PEDIDO
                       LEFT JOIN PED3 ON  PED3.ID_PEDIDO=P.ID_PEDIDO
                        WHERE LENTE_COD IS NOT NULL")  


residual_promo1 <-  dbGetQuery(con2,residual_promo1_sql)



## QUERY ID2

residual_id2 <- residual %>% select(ID2) %>% rename("ID_PEDIDO"="ID2") %>% filter(!is.na(.))


residual_promo2_sql <- glue_sql("
    WITH PED AS (SELECT ID_PEDIDO,PEDVRTOTAL FROM PEDID WHERE 
    ID_PEDIDO IN ({residual_id2$ID_PEDIDO*})
    AND PEDSITPED<>'C'),
    
    LEN AS (SELECT PROCODIGO,
                    PRODESCRICAO,
                     IIF(PROCODIGO2 IS NULL,
                      PROCODIGO,PROCODIGO2)CHAVE FROM PRODU WHERE PROTIPO NOT IN ('T','M','C','S','X','K')),
    
    TRAT AS (SELECT PROCODIGO,PRODESCRICAO FROM PRODU WHERE PROTIPO='T'),
    
    COL AS (SELECT PROCODIGO,PRODESCRICAO FROM PRODU WHERE PROTIPO='C'),
  
    PED1 AS (SELECT DISTINCT P.ID_PEDIDO,
                      CHAVE LENTE_COD,
                       (SELECT PRODESCRICAO FROM PRODU WHERE PROCODIGO=CHAVE) LENTE
     FROM PDPRD P
      INNER JOIN PED ON P.ID_PEDIDO=PED.ID_PEDIDO
       INNER JOIN LEN L ON L.PROCODIGO=P.PROCODIGO),
       
    PED2 AS (SELECT DISTINCT P.ID_PEDIDO,
                      P.PROCODIGO TRAT_COD,
                        (SELECT PRODESCRICAO FROM PRODU WHERE P.PROCODIGO=PROCODIGO) TRAT
        FROM PDPRD P
         INNER JOIN PED ON P.ID_PEDIDO=PED.ID_PEDIDO
          INNER JOIN TRAT T ON T.PROCODIGO=P.PROCODIGO),
          
    PED3 AS (SELECT DISTINCT P.ID_PEDIDO,
                      P.PROCODIGO COL_COD,
                        (SELECT PRODESCRICAO FROM PRODU WHERE P.PROCODIGO=PROCODIGO) COL
        FROM PDPRD P
         INNER JOIN PED ON P.ID_PEDIDO=PED.ID_PEDIDO
          INNER JOIN COL C ON C.PROCODIGO=P.PROCODIGO)
    
    SELECT P.ID_PEDIDO,
             LENTE_COD,
              LENTE,
               TRAT_COD,
                TRAT,
                 COL_COD,
                  COL,
                   PEDVRTOTAL
                    FROM PED P
                     LEFT JOIN PED1 ON  PED1.ID_PEDIDO=P.ID_PEDIDO
                      LEFT JOIN PED2 ON  PED2.ID_PEDIDO=P.ID_PEDIDO
                       LEFT JOIN PED3 ON  PED3.ID_PEDIDO=P.ID_PEDIDO
                        WHERE LENTE_COD IS NOT NULL")  


residual_promo2 <-  dbGetQuery(con2,residual_promo2_sql)

## FORMAT DTF


residual_id1_sheets <- inner_join(residual_promo1 %>% 
                                    mutate(ID_PEDIDO=as.character(ID_PEDIDO)),residual %>% 
                                    rename("ID_PEDIDO"="ID1") %>%
                                    mutate(ID_PEDIDO=trimws(ID_PEDIDO)),by="ID_PEDIDO")



residual_id2_sheets <- inner_join(residual_promo2 %>% 
                                    mutate(ID_PEDIDO=as.character(ID_PEDIDO)),residual %>% 
                                    rename("ID_PEDIDO"="ID2") %>%
                                    mutate(ID_PEDIDO=trimws(ID_PEDIDO)),by="ID_PEDIDO") 

## JOIN BY HASH




residual_sheets <- left_join(residual_id1_sheets,residual_id2_sheets, by="HASH") 


sheet_write(residual_sheets %>% .[,c(-9,-10,-19)],ss="15Or0FoTTDZBBSGuH_zPGYO9jD3QV97QNYtt9336u30w",sheet="RESIDUAL")



## IDS PEDIDOS CONTROL ==================================================================


## QUERY ID1 CONTROL



residual_id1_control <- anti_join(residual_id1,residual_promo1,by="ID_PEDIDO") 



residual_promo2_sql_A <- glue_sql("
    WITH PED AS (SELECT ID_PEDIDO,PEDVRTOTAL FROM PEDID WHERE 
    ID_PEDIDO IN ({residual_id1_control$ID_PEDIDO*})
    AND PEDSITPED<>'C'),
    
    LEN AS (SELECT PROCODIGO,
                    PRODESCRICAO,
                     IIF(PROCODIGO2 IS NULL,
                      PROCODIGO,PROCODIGO2)CHAVE FROM PRODU WHERE PROTIPO NOT IN ('T','M','C','S','X','K')),
    
    TRAT AS (SELECT PROCODIGO,PRODESCRICAO FROM PRODU WHERE PROTIPO='T'),
    
    COL AS (SELECT PROCODIGO,PRODESCRICAO FROM PRODU WHERE PROTIPO='C'),
  
    PED1 AS (SELECT DISTINCT P.ID_PEDIDO,
                      CHAVE LENTE_COD,
                       (SELECT PRODESCRICAO FROM PRODU WHERE PROCODIGO=CHAVE) LENTE
     FROM PDPRD P
      INNER JOIN PED ON P.ID_PEDIDO=PED.ID_PEDIDO
       INNER JOIN LEN L ON L.PROCODIGO=P.PROCODIGO),
       
    PED2 AS (SELECT DISTINCT P.ID_PEDIDO,
                      P.PROCODIGO TRAT_COD,
                        (SELECT PRODESCRICAO FROM PRODU WHERE P.PROCODIGO=PROCODIGO) TRAT
        FROM PDPRD P
         INNER JOIN PED ON P.ID_PEDIDO=PED.ID_PEDIDO
          INNER JOIN TRAT T ON T.PROCODIGO=P.PROCODIGO),
          
    PED3 AS (SELECT DISTINCT P.ID_PEDIDO,
                      P.PROCODIGO COL_COD,
                        (SELECT PRODESCRICAO FROM PRODU WHERE P.PROCODIGO=PROCODIGO) COL
        FROM PDPRD P
         INNER JOIN PED ON P.ID_PEDIDO=PED.ID_PEDIDO
          INNER JOIN COL C ON C.PROCODIGO=P.PROCODIGO)
    
    SELECT P.ID_PEDIDO,
             LENTE_COD,
              LENTE,
               TRAT_COD,
                TRAT,
                 COL_COD,
                  COL,
                   PEDVRTOTAL
                    FROM PED P
                     LEFT JOIN PED1 ON  PED1.ID_PEDIDO=P.ID_PEDIDO
                      LEFT JOIN PED2 ON  PED2.ID_PEDIDO=P.ID_PEDIDO
                       LEFT JOIN PED3 ON  PED3.ID_PEDIDO=P.ID_PEDIDO
                            ")  


residual_promo_control_A <-  dbGetQuery(con2,residual_promo2_sql_A) %>% 
  inner_join(.,residual,by=c("ID_PEDIDO"="ID1"))


View(residual_promo_control_A)



## QUERY ID2 CONTROL

residual_id1_A <- residual %>% 
  select(ID1,HASH) %>% 
  rename("ID_PEDIDO"="ID1") 


residual_id1_control <- anti_join(residual_id1,residual_promo1,by="ID_PEDIDO") %>% 
  inner_join(.,residual,by=c("ID_PEDIDO"="ID1"))



residual_id2_B <- residual_id1_control %>% select(ID2) %>% rename("ID_PEDIDO"="ID2") %>% filter(!is.na(.))


residual_promo2_sql_B <- glue_sql("
    WITH PED AS (SELECT ID_PEDIDO,PEDVRTOTAL FROM PEDID WHERE 
    ID_PEDIDO IN ({residual_id2_B$ID_PEDIDO*})
    AND PEDSITPED<>'C'),
    
    LEN AS (SELECT PROCODIGO,
                    PRODESCRICAO,
                     IIF(PROCODIGO2 IS NULL,
                      PROCODIGO,PROCODIGO2)CHAVE FROM PRODU WHERE PROTIPO NOT IN ('T','M','C','S','X','K')),
    
    TRAT AS (SELECT PROCODIGO,PRODESCRICAO FROM PRODU WHERE PROTIPO='T'),
    
    COL AS (SELECT PROCODIGO,PRODESCRICAO FROM PRODU WHERE PROTIPO='C'),
  
    PED1 AS (SELECT DISTINCT P.ID_PEDIDO,
                      CHAVE LENTE_COD,
                       (SELECT PRODESCRICAO FROM PRODU WHERE PROCODIGO=CHAVE) LENTE
     FROM PDPRD P
      INNER JOIN PED ON P.ID_PEDIDO=PED.ID_PEDIDO
       INNER JOIN LEN L ON L.PROCODIGO=P.PROCODIGO),
       
    PED2 AS (SELECT DISTINCT P.ID_PEDIDO,
                      P.PROCODIGO TRAT_COD,
                        (SELECT PRODESCRICAO FROM PRODU WHERE P.PROCODIGO=PROCODIGO) TRAT
        FROM PDPRD P
         INNER JOIN PED ON P.ID_PEDIDO=PED.ID_PEDIDO
          INNER JOIN TRAT T ON T.PROCODIGO=P.PROCODIGO),
          
    PED3 AS (SELECT DISTINCT P.ID_PEDIDO,
                      P.PROCODIGO COL_COD,
                        (SELECT PRODESCRICAO FROM PRODU WHERE P.PROCODIGO=PROCODIGO) COL
        FROM PDPRD P
         INNER JOIN PED ON P.ID_PEDIDO=PED.ID_PEDIDO
          INNER JOIN COL C ON C.PROCODIGO=P.PROCODIGO)
    
    SELECT P.ID_PEDIDO,
             LENTE_COD,
              LENTE,
               TRAT_COD,
                TRAT,
                 COL_COD,
                  COL,
                   PEDVRTOTAL
                    FROM PED P
                     LEFT JOIN PED1 ON  PED1.ID_PEDIDO=P.ID_PEDIDO
                      LEFT JOIN PED2 ON  PED2.ID_PEDIDO=P.ID_PEDIDO
                       LEFT JOIN PED3 ON  PED3.ID_PEDIDO=P.ID_PEDIDO
                        WHERE LENTE_COD IS NOT NULL")  


residual_promo_control_B <-  dbGetQuery(con2,residual_promo2_sql_B) %>% inner_join(.,residual,by=c("ID_PEDIDO"="ID2"))


View(residual_promo_control_B)




residual_promo_control_AB <- left_join(residual_promo_control_A,residual_promo_control_B,by="HASH") %>% 
  .[,c(-9,-10,-19)] 

## WRITE SHEET


range_AB <- paste0("A",range_read("15Or0FoTTDZBBSGuH_zPGYO9jD3QV97QNYtt9336u30w",sheet = "RESIDUAL" , range = "A:A") %>%  nrow()+1)



range_write("15Or0FoTTDZBBSGuH_zPGYO9jD3QV97QNYtt9336u30w",data=residual_promo_control_AB,sheet = "RESIDUAL",
            range = range_AB ,reformat = FALSE,col_names = FALSE)



## LENTES CONTROL ==================================================================================


## diferença residual pedido1 - somente pedido com lentes

residual_id1_control <- anti_join(residual_id1,residual_promo1,by="ID_PEDIDO") 


residual_promo_control_lentes_sql <- glue_sql("
    WITH PEDCONTROL AS (SELECT CLICODIGO, 
                          REPLACE(PEDCODIGO,'.000','.001') PEDCODIGO,
                           PEDDTEMIS FROM PEDID 
                            WHERE ID_PEDIDO IN ({residual_id1_control$ID_PEDIDO*})
                            ),
                   
    PED AS (SELECT ID_PEDIDO,PEDVRTOTAL FROM PEDID P 
                   INNER JOIN PEDCONTROL PE ON P.PEDCODIGO=PE.PEDCODIGO AND 
                    P.CLICODIGO=PE.CLICODIGO AND
                     P.PEDDTEMIS=PE.PEDDTEMIS),  
                        
    
    LEN AS (SELECT PROCODIGO,
                    PRODESCRICAO,
                     IIF(PROCODIGO2 IS NULL,
                      PROCODIGO,PROCODIGO2)CHAVE FROM PRODU WHERE PROTIPO NOT IN ('T','M','C','S','X','K')),
    
    TRAT AS (SELECT PROCODIGO,PRODESCRICAO FROM PRODU WHERE PROTIPO='T'),
    
    COL AS (SELECT PROCODIGO,PRODESCRICAO FROM PRODU WHERE PROTIPO='C'),
  
    PED1 AS (SELECT DISTINCT P.ID_PEDIDO,
                      CHAVE LENTE_COD,
                       (SELECT PRODESCRICAO FROM PRODU WHERE PROCODIGO=CHAVE) LENTE
     FROM PDPRD P
       INNER JOIN LEN L ON L.PROCODIGO=P.PROCODIGO),
       
    PED2 AS (SELECT DISTINCT P.ID_PEDIDO,
                      P.PROCODIGO TRAT_COD,
                        (SELECT PRODESCRICAO FROM PRODU WHERE P.PROCODIGO=PROCODIGO) TRAT
        FROM PDPRD P
          INNER JOIN TRAT T ON T.PROCODIGO=P.PROCODIGO),
          
    PED3 AS (SELECT DISTINCT P.ID_PEDIDO,
                      P.PROCODIGO COL_COD,
                        (SELECT PRODESCRICAO FROM PRODU WHERE P.PROCODIGO=PROCODIGO) COL
        FROM PDPRD P
          INNER JOIN COL C ON C.PROCODIGO=P.PROCODIGO)
    
    SELECT P.ID_PEDIDO,
             LENTE_COD,
              LENTE,
               TRAT_COD,
                TRAT,
                 COL_COD,
                  COL,
                   SUM(PEDVRTOTAL) PEDVRTOTAL
                    FROM PDPRD P
                     INNER JOIN PED PE ON P.ID_PEDIDO=PE.ID_PEDIDO
                      LEFT JOIN PED1 ON  PED1.ID_PEDIDO=P.ID_PEDIDO
                       LEFT JOIN PED2 ON  PED2.ID_PEDIDO=P.ID_PEDIDO
                        LEFT JOIN PED3 ON  PED3.ID_PEDIDO=P.ID_PEDIDO
                         GROUP BY 1,2,3,4,5,6,7
                        ")  


residual_promo_control_lentes<-  dbGetQuery(con2,residual_promo_control_lentes_sql)


residual_promo_control_servico_sql <- glue_sql("
    WITH PEDCONTROL AS (SELECT CLICODIGO, 
                          REPLACE(PEDCODIGO,'.000','.001') PEDCODIGO,ID_PEDIDO ID_PEDIDO2,
                           PEDDTEMIS FROM PEDID 
                            WHERE ID_PEDIDO IN ({residual_id1_control$ID_PEDIDO*})
                            )
                   
    SELECT P.ID_PEDIDO,ID_PEDIDO2 FROM PEDID P 
                   INNER JOIN PEDCONTROL PE ON P.PEDCODIGO=PE.PEDCODIGO AND 
                    P.CLICODIGO=PE.CLICODIGO AND
                     P.PEDDTEMIS=PE.PEDDTEMIS 
                        ")  


residual_promo_control_servico <-  dbGetQuery(con2,residual_promo_control_servico_sql)


residual_promo_control_lentes_servicos <- left_join(residual_promo_control_lentes,residual_promo_control_servico,by="ID_PEDIDO")

View(residual_promo_control_lentes_servicos)

sheet_write(residual_promo_control_lentes_servicos,ss="15Or0FoTTDZBBSGuH_zPGYO9jD3QV97QNYtt9336u30w",sheet="CONTROL")







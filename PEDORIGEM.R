
## PEDIDOS POR ORIGEM 
## Bibliotecas necessárias

library(DBI)
library(dplyr)
library(reshape2)
library(googlesheets4)
library(gmailr)


## conexão com banco replica 

con2 <- dbConnect(odbc::odbc(), "reproreplica")


pedorigem <- dbGetQuery(con2,"
  
  WITH FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','R','SR')),
  
  CLI AS (SELECT C.CLICODIGO,CLINOMEFANT, GCLCODIGO GRUPO, SETOR FROM CLIEN C
  INNER JOIN (SELECT CLICODIGO, ZODESCRICAO SETOR FROM ENDCLI E
  INNER JOIN (SELECT ZOCODIGO,ZODESCRICAO FROM ZONA WHERE ZOCODIGO IN(20,21,22,23,24,28))Z ON E.ZOCODIGO=Z.ZOCODIGO
  WHERE ENDFAT='S'
  ) ED ON C.CLICODIGO=ED.CLICODIGO
  WHERE CLICLIENTE='S')
  
  
  SELECT ID_PEDIDO,
          PEDDTEMIS,
            P.CLICODIGO,
             CLINOMEFANT,
              GRUPO,
             PEDORIGEM,
              SETOR FROM PEDID P
  INNER JOIN FIS ON P.FISCODIGO1=FIS.FISCODIGO
   INNER JOIN CLI C ON P.CLICODIGO=C.CLICODIGO
    WHERE PEDDTEMIS >= DATEADD(-30 DAY TO CURRENT_DATE) AND 
     PEDSITPED<>'C' AND PEDLCFINANC IN ('S', 'L','N') AND 
      PEDORIGEM IN ('D','W')

  ") 


## SETOR 1

setor1 <- pedorigem %>% group_by(CLICODIGO,CLINOMEFANT,SETOR,PEDORIGEM) %>% 
   summarize(QTD=n_distinct(ID_PEDIDO)) %>% dcast(CLICODIGO + CLINOMEFANT + SETOR ~ PEDORIGEM) %>%  
     as.data.frame() %>% mutate(W=coalesce(W,0)) %>% mutate(D=coalesce(D,0)) %>% 
  mutate(PERC_W=round((W)/(W+D)*100,2)) %>% 
  mutate(PERC_D=round((D)/(W+D)*100,2)) %>% arrange(desc(PERC_D)) %>% filter(SETOR=='SETOR 1 - GRANDE FLORIANOPOLIS')

range_write("1mOfE__m24rjZ6iyusDg2GjvJOdRbB0mNBcS_MClqSEA",data=setor1,sheet = "SETOR1",
            range = "A1",reformat = FALSE) 

## SETOR 2

setor2 <- pedorigem %>% group_by(CLICODIGO,CLINOMEFANT,SETOR,PEDORIGEM) %>% 
  summarize(QTD=n_distinct(ID_PEDIDO)) %>% dcast(CLICODIGO + CLINOMEFANT + SETOR ~ PEDORIGEM) %>%  
  as.data.frame() %>% mutate(W=coalesce(W,0)) %>% mutate(D=coalesce(D,0)) %>% 
  mutate(PERC_W=round((W)/(W+D)*100,2)) %>% 
  mutate(PERC_D=round((D)/(W+D)*100,2)) %>% arrange(desc(PERC_D)) %>% filter(SETOR=='SETOR 2 - CRICIUMA - SUL')

range_write("1mOfE__m24rjZ6iyusDg2GjvJOdRbB0mNBcS_MClqSEA",data=setor2,sheet = "SETOR2",
            range = "A1",reformat = FALSE) 

## SETOR 3

setor3 <- pedorigem %>% group_by(CLICODIGO,CLINOMEFANT,SETOR,PEDORIGEM) %>% 
  summarize(QTD=n_distinct(ID_PEDIDO)) %>% dcast(CLICODIGO + CLINOMEFANT + SETOR ~ PEDORIGEM) %>%  
  as.data.frame() %>% mutate(W=coalesce(W,0)) %>% mutate(D=coalesce(D,0)) %>% 
  mutate(PERC_W=round((W)/(W+D)*100,2)) %>% 
  mutate(PERC_D=round((D)/(W+D)*100,2)) %>% arrange(desc(PERC_D)) %>% filter(SETOR=='SETOR 3 - CHAPECO-PLANAL-OESTE')

range_write("1mOfE__m24rjZ6iyusDg2GjvJOdRbB0mNBcS_MClqSEA",data=setor3,sheet = "SETOR3",
            range = "A1",reformat = FALSE) 


## SETOR 4

setor4 <- pedorigem %>% group_by(CLICODIGO,CLINOMEFANT,SETOR,PEDORIGEM) %>% 
  summarize(QTD=n_distinct(ID_PEDIDO)) %>% dcast(CLICODIGO + CLINOMEFANT + SETOR ~ PEDORIGEM) %>%  
  as.data.frame() %>% mutate(W=coalesce(W,0)) %>% mutate(D=coalesce(D,0)) %>% 
  mutate(PERC_W=round((W)/(W+D)*100,2)) %>% 
  mutate(PERC_D=round((D)/(W+D)*100,2)) %>% arrange(desc(PERC_D)) %>% filter(SETOR=='SETOR 4 - JOINVILLE - NORTESC')

range_write("1mOfE__m24rjZ6iyusDg2GjvJOdRbB0mNBcS_MClqSEA",data=setor4,sheet = "SETOR4",
            range = "A1",reformat = FALSE)


## SETOR 5

setor5 <- pedorigem %>% group_by(CLICODIGO,CLINOMEFANT,SETOR,PEDORIGEM) %>% 
  summarize(QTD=n_distinct(ID_PEDIDO)) %>% dcast(CLICODIGO + CLINOMEFANT + SETOR ~ PEDORIGEM) %>%  
  as.data.frame() %>% mutate(W=coalesce(W,0)) %>% mutate(D=coalesce(D,0)) %>% 
  mutate(PERC_W=round((W)/(W+D)*100,2)) %>% 
  mutate(PERC_D=round((D)/(W+D)*100,2)) %>% arrange(desc(PERC_D)) %>% filter(SETOR=='SETOR 5 - BLUMENAU - VALE')

range_write("1mOfE__m24rjZ6iyusDg2GjvJOdRbB0mNBcS_MClqSEA",data=setor5,sheet = "SETOR5",
            range = "A1",reformat = FALSE)


## SETOR 6

setor6 <- pedorigem %>% group_by(CLICODIGO,CLINOMEFANT,SETOR,PEDORIGEM) %>% 
  summarize(QTD=n_distinct(ID_PEDIDO)) %>% dcast(CLICODIGO + CLINOMEFANT + SETOR ~ PEDORIGEM) %>%  
  as.data.frame() %>% mutate(W=coalesce(W,0)) %>% mutate(D=coalesce(D,0)) %>% 
  mutate(PERC_W=round((W)/(W+D)*100,2)) %>% 
  mutate(PERC_D=round((D)/(W+D)*100,2)) %>% arrange(desc(PERC_D)) %>% filter(SETOR=='SETOR 6 - BALNEARIO-LITORAL')

range_write("1mOfE__m24rjZ6iyusDg2GjvJOdRbB0mNBcS_MClqSEA",data=setor6,sheet = "SETOR6",
            range = "A1",reformat = FALSE) 



## RESUMO

pedorigem2 <- pedorigem %>% 
               group_by(SETOR,PEDORIGEM) %>% 
                summarize(QTD=n_distinct(ID_PEDIDO)) %>% 
                 dcast(SETOR ~ PEDORIGEM) %>%  
                  as.data.frame() %>% 
                   mutate(W=coalesce(W,0)) %>% 
                    mutate(D=coalesce(D,0)) %>% 
                     mutate(PERC_W=round((W)/(W+D)*100,2)) %>% 
                      mutate(PERC_D=round((D)/(W+D)*100,2)) %>% 
                       arrange(desc(PERC_D)) %>% 
                        mutate(TOT=W+D) %>% 
                         mutate(META=0.7) %>% 
                          mutate(META2=TOT*META) %>% 
                           mutate(FALTAM=W-META2)
  



range_write("1mOfE__m24rjZ6iyusDg2GjvJOdRbB0mNBcS_MClqSEA",data=pedorigem2,sheet = "RESUMO",
            range = "A1",reformat = FALSE) 


## TOTAL CLIENTES

conversion <- pedorigem %>% group_by(CLICODIGO,CLINOMEFANT,SETOR,PEDORIGEM) %>% 
  summarize(QTD=n_distinct(ID_PEDIDO)) %>% dcast(CLICODIGO + CLINOMEFANT + SETOR ~ PEDORIGEM) %>%  
  as.data.frame() %>% mutate(W=coalesce(W,0)) %>% mutate(D=coalesce(D,0)) %>% 
  mutate(PERC_W=round((W)/(W+D)*100,2)) %>% 
  mutate(PERC_D=round((D)/(W+D)*100,2)) %>% arrange(desc(PERC_D)) %>% filter(PERC_D==100) %>% 
  tally()

range_write("1mOfE__m24rjZ6iyusDg2GjvJOdRbB0mNBcS_MClqSEA",data=conversion,sheet = "RESUMO",
            range = "B10",reformat = FALSE)



## SEND MAIL ====================================================================


gm_auth_configure(path = "C:\\Users\\Repro\\Documents\\R\\ADM\\REPORTS_AUTO\\sendmail.json")

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,junior.lima@repro.com.br,cristiano.regis@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO ORIGEM DE PEDIDOS") %>%
  gm_text_body("RELATÓRIO ATUALIZADO.
ACESSE O LINK https://docs.google.com/spreadsheets/d/1mOfE__m24rjZ6iyusDg2GjvJOdRbB0mNBcS_MClqSEA/edit?usp=sharing.

DESCRIÇÃO: PEDIDOS ORIGEM WEB E DIGITADOS DOS ÚLTIMOS 30 DIAS POR EMISSÃO, POR SETOR.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)











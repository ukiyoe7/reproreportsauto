WITH 
                               
   FIS AS (SELECT FISCODIGO FROM TBFIS WHERE (FISTPNATOP IN ('V','R','SR') OR FISCODIGO IN ('5.91V','6.91V'))),
    
    PED AS (SELECT ID_PEDIDO,
                    CLICODIGO,
                         PEDDTBAIXA
                          FROM PEDID P
                           INNER JOIN FIS ON P.FISCODIGO1=FIS.FISCODIGO
                            WHERE (PEDDTBAIXA BETWEEN '01.01.2022' AND '31.12.2022')
                             AND PEDSITPED<>'C' ),
                               
      PROD AS (SELECT PROCODIGO FROM PRODU WHERE MARCODIGO=57)

    
      SELECT PD.ID_PEDIDO,
              PEDDTBAIXA,
                 CLICODIGO,
                    SUM(PDPQTDADE)QTD,
                     SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA 
                      FROM PDPRD PD
                       INNER JOIN PED P ON PD.ID_PEDIDO=P.ID_PEDIDO
                        INNER JOIN PROD PR ON PD.PROCODIGO=PR.PROCODIGO
                         GROUP BY 1,2,3 ORDER BY ID_PEDIDO DESC
                                   
                                   
                                   
                                   
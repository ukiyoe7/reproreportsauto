  WITH CLI AS (SELECT DISTINCT CLICODIGO,
                           CLINOMEFANT,
                             GCLCODIGO
                              FROM CLIEN 
                               WHERE CLICLIENTE='S' AND
                                CLICODIGO IN(4496,4483,4528,4536)),
                                 
  FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','R','SR')),
      
      PED AS (SELECT ID_PEDIDO,
                        PEDDTBAIXA,
                         P.CLICODIGO,
                          GCLCODIGO,
                            CLINOMEFANT
                              FROM PEDID P
                               INNER JOIN FIS ON P.FISCODIGO1=FIS.FISCODIGO
                                INNER JOIN CLI C ON P.CLICODIGO=C.CLICODIGO
                                 WHERE
                                  PEDDTBAIXA
                                   BETWEEN DATEADD(MONTH, -12, CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) + 1)
                                    AND CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) 
                                     AND PEDSITPED<>'C'
                                      AND PEDLCFINANC IN ('S', 'L','N')) 
      
      
        SELECT PD.ID_PEDIDO,
                  PEDDTBAIXA DATA,
                   CLICODIGO,
                    CLINOMEFANT NOMEFANTASIA,
                     GCLCODIGO GRUPO,
                         PROCODIGO,
                          PDPDESCRICAO DESCRICAO,
                            SUM(PDPQTDADE)QTD,
                             SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA 
                              FROM PDPRD PD
                               INNER JOIN PED P ON PD.ID_PEDIDO=P.ID_PEDIDO
                                GROUP BY 1,2,3,4,5,6,7 ORDER BY ID_PEDIDO DESC
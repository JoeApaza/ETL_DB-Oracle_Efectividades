DROP TABLE EFECTIVIDADES_CORP;
CREATE TABLE EFECTIVIDADES_CORP AS (
SELECT  
TO_CHAR(TO_DATE(CASE WHEN SUBSTR(A.NOMBRE_CARTERA,-6,6)='NTERNA' THEN 'ENE-22'
                     WHEN  SUBSTR(A.NOMBRE_CARTERA,-6,3)='SET' THEN 'SEP-'|| SUBSTR(A.NOMBRE_CARTERA,-2,2) 
                     ELSE REGEXP_REPLACE(SUBSTR(A.NOMBRE_CARTERA,-6,6),'_','-')END, 'MON-YY', 'NLS_DATE_LANGUAGE=SPANISH'), 'YYYYMM')PERIODO_CARTERA,
A.*,
C.TRAMO_PREF_CARTERA,C.CARTERA ,C.FECHA_INICIO INICIO_ETAPA,R.DIA_PAGO,R.FECHA_REGISTRO_APLIC_PAGO ,
TRUNC(A.FECHA-C.FECHA_INICIO) DIA_GESTION,
TRUNC(R.FECHA_REGISTRO_APLIC_PAGO-C.FECHA_INICIO) DIA_PAGO_GESTION,
(CASE WHEN CAE.RUC IS NOT NULL THEN 'CARTERIZADO'
      WHEN G.RUC IS NOT NULL THEN 'GOBIERNO' ELSE 'NO CARTERIZADO' END)TIPO_CORP,
CAE.EJECUTIVO,
CAE.SUPERVISOR,   
CAE.CARTERA CARTERA_CAE,
P.DIRECCION,
P.SEGMENTO,
P.SUB_SEGMENTO,
CU.REGION REGION2,  
CU.DEPARTAMENTO, 
CU.PROVINCIA,
CU.DISTRITO,
CU.ESTADO ESTADO_ACTUAL,
(CASE WHEN TRUNC(TO_DATE(FECHA_VENCIMIENTO,'DD/MM/YYYY'))>=TRUNC(SYSDATE) THEN 'DEUDA POR VENCER' ELSE 'DEUDA VENCIDA'END)CRITERIO,
TRUNC(A.FECHA)-TRUNC(TO_DATE(FECHA_VENCIMIENTO,'DD/MM/YYYY')) ANTIGUEDAD,
(CASE WHEN (TRUNC(A.FECHA)-TRUNC(TO_DATE(FECHA_VENCIMIENTO,'DD/MM/YYYY')))<1 THEN '0. CORRIENTE'
     WHEN (TRUNC(A.FECHA)-TRUNC(TO_DATE(FECHA_VENCIMIENTO,'DD/MM/YYYY')))<31 THEN '1. RANGO DE 1 A 30'
     WHEN (TRUNC(A.FECHA)-TRUNC(TO_DATE(FECHA_VENCIMIENTO,'DD/MM/YYYY')))<61 THEN '2. RANGO DE 31 A 60'
     WHEN (TRUNC(A.FECHA)-TRUNC(TO_DATE(FECHA_VENCIMIENTO,'DD/MM/YYYY')))<91 THEN '3. RANGO DE 61 A 90'
     WHEN (TRUNC(A.FECHA)-TRUNC(TO_DATE(FECHA_VENCIMIENTO,'DD/MM/YYYY')))<121 THEN '4. RANGO DE 91 A 120'
     WHEN (TRUNC(A.FECHA)-TRUNC(TO_DATE(FECHA_VENCIMIENTO,'DD/MM/YYYY')))<151 THEN '5. RANGO DE 121 A 150'
     WHEN (TRUNC(A.FECHA)-TRUNC(TO_DATE(FECHA_VENCIMIENTO,'DD/MM/YYYY')))<365 THEN '6. RANGO DE 151 A 364'
     WHEN (TRUNC(A.FECHA)-TRUNC(TO_DATE(FECHA_VENCIMIENTO,'DD/MM/YYYY')))>=365 THEN '7. RANGO DE 365 A MAS' END)TRAMO_ASIGNACION,
NVL(R.PAGO_EFECTIVIDAD,0)PAGO_EFECTIVIDAD,NVL(PAGO_APLICADO_SOLES,0)PAGO_APLICADO_SOLES, SYSDATE F_DUMP FROM ASIGNACION_HISTORICO A,
ASIGNACION_PAGOS R ,
(SELECT * FROM TABLA_CARTERIZADOS WHERE SECTOR NOT IN ('GOBIERNO'))CAE,
(SELECT * FROM TABLA_SEGMENTACION WHERE TIPO_CORPORATIVO='GOBIERNO')G,
CARTERAS_CORPORATIVAS C,
TABLA_SEGMENTACION_COMERCIAL P,
CUENTAS CU

WHERE A.FECHA=R.FECHA(+)
AND A.GESTOR=R.GESTOR(+)
AND A.CUST_ACCOUNT=R.NRO_CUENTA(+)
AND A.CUST_ACCOUNT=CU.CUENTA
AND A.NRO_DOCUMENTO=R.NRO_DOC(+)
AND A.RUC_DNI=CAE.RUC(+)
AND A.RUC_DNI=P.NUMDOC(+)
AND TO_CHAR(A.FECHA,'YYYYMM')=CAE.PERIODO(+)
AND A.RUC_DNI=G.RUC(+)
AND A.NOMBRE_CARTERA=C.NOMBRE_CARTERA(+));


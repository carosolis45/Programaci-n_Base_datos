--Se realiza la conexion y crea el usuario PRY2206_P4--
--se crea _pobla_tablas_bd_ALL_THE_BEST.SQL--

-- CASO 1--
DECLARE
    -- VARIABLES BIND (parámetros) 
    v_tramo1_inf   NUMBER := 500000;  -- $500.000
    v_tramo1_sup   NUMBER := 700000;  -- $700.000
    v_tramo2_inf   NUMBER := 700001;  -- $700.001
    v_tramo2_sup   NUMBER := 900000;  -- $900.000
    v_tramo3_inf   NUMBER := 900001;  -- Más de $900.000

    -- VARRAY
    TYPE t_puntos IS VARRAY(4) OF NUMBER;
    v_puntos t_puntos := t_puntos(250, 300, 550, 700); -- [normales, extras1, extras2, extras3]

    -- Variables para cursores
    CURSOR c_resumen IS -- Cursor explícito con parámetro 
        SELECT 
            TO_CHAR(ttc.fecha_transaccion, 'MMYYYY') AS mes_anno,
            SUM(CASE WHEN ttt.nombre_tptran_tarjeta = 'Compras Tiendas Retail o Asociadas' THEN ttc.monto_transaccion ELSE 0 END) AS total_compras,
            COUNT(CASE WHEN ttt.nombre_tptran_tarjeta = 'Compras Tiendas Retail o Asociadas' THEN 1 END) AS cant_compras,
            SUM(CASE WHEN ttt.nombre_tptran_tarjeta = 'Avance en Efectivo' THEN ttc.monto_transaccion ELSE 0 END) AS total_avances,
            COUNT(CASE WHEN ttt.nombre_tptran_tarjeta = 'Avance en Efectivo' THEN 1 END) AS cant_avances,
            SUM(CASE WHEN ttt.nombre_tptran_tarjeta = 'Súper Avance en Efectivo' THEN ttc.monto_transaccion ELSE 0 END) AS total_savances,
            COUNT(CASE WHEN ttt.nombre_tptran_tarjeta = 'Súper Avance en Efectivo' THEN 1 END) AS cant_savances
        FROM TRANSACCION_TARJETA_CLIENTE ttc
        JOIN TIPO_TRANSACCION_TARJETA ttt ON ttc.cod_tptran_tarjeta = ttt.cod_tptran_tarjeta
        WHERE EXTRACT(YEAR FROM ttc.fecha_transaccion) = EXTRACT(YEAR FROM SYSDATE) - 1
        GROUP BY TO_CHAR(ttc.fecha_transaccion, 'MMYYYY')
        ORDER BY TO_CHAR(ttc.fecha_transaccion, 'MMYYYY');

    -- Cursor con variable de cursor 
    TYPE c_detalle_type IS REF CURSOR;
    c_detalle c_detalle_type;

    -- Registro PL/SQL 
    TYPE r_detalle IS RECORD (
        numrun              CLIENTE.numrun%TYPE,
        dvrun               CLIENTE.dvrun%TYPE,
        nro_tarjeta         TARJETA_CLIENTE.nro_tarjeta%TYPE,
        nro_transaccion     TRANSACCION_TARJETA_CLIENTE.nro_transaccion%TYPE,
        fecha_transaccion   TRANSACCION_TARJETA_CLIENTE.fecha_transaccion%TYPE,
        tipo_transaccion    TIPO_TRANSACCION_TARJETA.nombre_tptran_tarjeta%TYPE,
        monto_transaccion   TRANSACCION_TARJETA_CLIENTE.monto_transaccion%TYPE,
        puntos              NUMBER
    );
    v_reg_detalle r_detalle;

    -- Variables para cálculos
    v_anio_anterior        NUMBER;
    v_puntos_normales      NUMBER := v_puntos(1); -- 250 puntos por cada $100.000
    v_puntos_extras        NUMBER;
    v_monto_anual_cliente  NUMBER;
    v_tipo_cliente         TIPO_CLIENTE.nombre_tipo_cliente%TYPE;

    -- Variables para resumen
    v_mes_anno             VARCHAR2(6);
    v_total_compras        NUMBER;
    v_total_puntos_comp    NUMBER;
    v_total_avances        NUMBER;
    v_total_puntos_av      NUMBER;
    v_total_savances       NUMBER;
    v_total_puntos_sav     NUMBER;

BEGIN
    -- Punto 8: Truncar tablas
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_PUNTOS_TARJETA_CATB';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_PUNTOS_TARJETA_CATB';

    -- Obtener año anterior dinámicamente (punto 6)
    v_anio_anterior := EXTRACT(YEAR FROM SYSDATE) - 1;

    -- Cursor para detalles (con variable de cursor)
    OPEN c_detalle FOR
        SELECT 
            c.numrun,
            c.dvrun,
            tc.nro_tarjeta,
            ttc.nro_transaccion,
            ttc.fecha_transaccion,
            ttt.nombre_tptran_tarjeta,
            ttc.monto_transaccion
        FROM TRANSACCION_TARJETA_CLIENTE ttc
        JOIN TARJETA_CLIENTE tc ON ttc.nro_tarjeta = tc.nro_tarjeta
        JOIN CLIENTE c ON tc.numrun = c.numrun
        JOIN TIPO_TRANSACCION_TARJETA ttt ON ttc.cod_tptran_tarjeta = ttt.cod_tptran_tarjeta
        WHERE EXTRACT(YEAR FROM ttc.fecha_transaccion) = v_anio_anterior
        ORDER BY ttc.fecha_transaccion, c.numrun, ttc.nro_transaccion;

    -- Procesar detalle
    LOOP
        FETCH c_detalle INTO 
            v_reg_detalle.numrun,
            v_reg_detalle.dvrun,
            v_reg_detalle.nro_tarjeta,
            v_reg_detalle.nro_transaccion,
            v_reg_detalle.fecha_transaccion,
            v_reg_detalle.tipo_transaccion,
            v_reg_detalle.monto_transaccion;
        EXIT WHEN c_detalle%NOTFOUND;

        -- Inicializar puntos
        v_reg_detalle.puntos := 0;

        -- Puntos normales: 250 por cada $100.000 (punto 12, con estructura de control)
        v_reg_detalle.puntos := TRUNC(v_reg_detalle.monto_transaccion / 100000) * v_puntos_normales;

        -- Determinar tipo de cliente para puntos extras
        BEGIN
            SELECT tc.nombre_tipo_cliente 
            INTO v_tipo_cliente
            FROM CLIENTE c
            JOIN TIPO_CLIENTE tc ON c.cod_tipo_cliente = tc.cod_tipo_cliente
            WHERE c.numrun = v_reg_detalle.numrun;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_tipo_cliente := NULL;
        END;

        -- Solo dueñas de casa y pensionados/tercera edad tienen puntos extras
        IF v_tipo_cliente IN ('Dueña(o) de Casa', 'Pensionados y Tercera Edad') THEN
            -- Calcular monto anual acumulado por cliente (simplificado: sumar transacciones del año)
            SELECT NVL(SUM(ttc.monto_transaccion), 0)
            INTO v_monto_anual_cliente
            FROM TRANSACCION_TARJETA_CLIENTE ttc
            JOIN TARJETA_CLIENTE tc ON ttc.nro_tarjeta = tc.nro_tarjeta
            WHERE tc.numrun = v_reg_detalle.numrun
            AND EXTRACT(YEAR FROM ttc.fecha_transaccion) = v_anio_anterior;

            -- se determinar puntos extras según tramo (de acuerdo al punto 12, estructura condicional)
            IF v_monto_anual_cliente BETWEEN v_tramo1_inf AND v_tramo1_sup THEN
                v_puntos_extras := v_puntos(2); -- 300
            ELSIF v_monto_anual_cliente BETWEEN v_tramo2_inf AND v_tramo2_sup THEN
                v_puntos_extras := v_puntos(3); -- 550
            ELSIF v_monto_anual_cliente > v_tramo3_inf THEN
                v_puntos_extras := v_puntos(4); -- 700
            ELSE
                v_puntos_extras := 0;
            END IF;

            -- se Suman puntos extras por cada $100.000 del monto de esta transacción
            v_reg_detalle.puntos := v_reg_detalle.puntos + 
                (TRUNC(v_reg_detalle.monto_transaccion / 100000) * v_puntos_extras);
        END IF;

        -- Insertar en DETALLE_PUNTOS_TARJETA_CATB
        INSERT INTO DETALLE_PUNTOS_TARJETA_CATB VALUES (
            v_reg_detalle.numrun,
            v_reg_detalle.dvrun,
            v_reg_detalle.nro_tarjeta,
            v_reg_detalle.nro_transaccion,
            v_reg_detalle.fecha_transaccion,
            v_reg_detalle.tipo_transaccion,
            v_reg_detalle.monto_transaccion,
            v_reg_detalle.puntos
        );
    END LOOP;
    CLOSE c_detalle;

    -- Procesar resumen con cursor explícito
    FOR rec IN c_resumen LOOP
        v_mes_anno := rec.mes_anno;
        v_total_compras := rec.total_compras;
        v_total_avances := rec.total_avances;
        v_total_savances := rec.total_savances;

        -- Calcular puntos para cada tipo 
        v_total_puntos_comp := TRUNC(v_total_compras / 100000) * v_puntos_normales;
        v_total_puntos_av := TRUNC(v_total_avances / 100000) * v_puntos_normales;
        v_total_puntos_sav := TRUNC(v_total_savances / 100000) * v_puntos_normales;

        -- Insertar en RESUMEN_PUNTOS_TARJETA_CATB (orden ascendente por mes/año)
        INSERT INTO RESUMEN_PUNTOS_TARJETA_CATB VALUES (
            v_mes_anno,
            v_total_compras,
            v_total_puntos_comp,
            v_total_avances,
            v_total_puntos_av,
            v_total_savances,
            v_total_puntos_sav
        );
    END LOOP;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Proceso completado para el año ' || v_anio_anterior);
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/

-- Ver los detalles de puntos
SELECT * FROM DETALLE_PUNTOS_TARJETA_CATB 
ORDER BY fecha_transaccion, numrun, nro_transaccion;

-- Ver el resumen por mes
SELECT * FROM RESUMEN_PUNTOS_TARJETA_CATB 
ORDER BY mes_anno;


--CASO 2--

DECLARE
    -- Cursor para el detalle (con parámetro para el año)
    CURSOR c_detalle(p_anio NUMBER) IS
        SELECT 
            c.numrun,
            c.dvrun,
            tt.nro_tarjeta,
            tt.nro_transaccion,
            tt.fecha_transaccion,
            ttt.nombre_tptran_tarjeta AS tipo_transaccion,
            tt.monto_total_transaccion
        FROM TRANSACCION_TARJETA_CLIENTE tt
        JOIN TIPO_TRANSACCION_TARJETA ttt ON tt.cod_tptran_tarjeta = ttt.cod_tptran_tarjeta
        JOIN TARJETA_CLIENTE tc ON tt.nro_tarjeta = tc.nro_tarjeta
        JOIN CLIENTE c ON tc.numrun = c.numrun
        WHERE EXTRACT(YEAR FROM tt.fecha_transaccion) = p_anio
          AND ttt.nombre_tptran_tarjeta IN ('Avance en Efectivo', 'Súper Avance en Efectivo')
        ORDER BY tt.fecha_transaccion, c.numrun;
    
    -- Cursor para el resumen (optimizado - ya incluye el aporte calculado)
    CURSOR c_resumen(p_anio NUMBER) IS
        SELECT 
            TO_CHAR(d.fecha_transaccion, 'MMYYYY') AS mes_anno,
            d.tipo_transaccion,
            SUM(d.monto_transaccion) AS monto_total_transacciones,
            SUM(d.aporte_sbif) AS aporte_total_abif
        FROM DETALLE_APORTE_SBIF d
        WHERE EXTRACT(YEAR FROM d.fecha_transaccion) = p_anio
        GROUP BY 
            TO_CHAR(d.fecha_transaccion, 'MMYYYY'),
            d.tipo_transaccion
        ORDER BY 
            TO_CHAR(d.fecha_transaccion, 'MMYYYY'),
            d.tipo_transaccion;
    
    -- Variables para el cursor de detalle
    v_num_run         CLIENTE.numrun%TYPE;
    v_dv_run          CLIENTE.dvrun%TYPE;
    v_nro_tarjeta     TARJETA_CLIENTE.nro_tarjeta%TYPE;
    v_nro_transaccion TRANSACCION_TARJETA_CLIENTE.nro_transaccion%TYPE;
    v_fecha_trans     TRANSACCION_TARJETA_CLIENTE.fecha_transaccion%TYPE;
    v_tipo_trans      TIPO_TRANSACCION_TARJETA.nombre_tptran_tarjeta%TYPE;
    v_monto_total     TRANSACCION_TARJETA_CLIENTE.monto_total_transaccion%TYPE;
    v_aporte_sbif     NUMBER;
    
    -- Variables para el cursor de resumen
    v_mes_anno               VARCHAR2(6);
    v_tipo_trans_res         VARCHAR2(50);
    v_monto_total_res        NUMBER;
    v_aporte_total_res       NUMBER;
    
    -- Variables para procesamiento
    v_anio_procesar          NUMBER;
    v_contador_detalle       NUMBER := 0;
    
    -- Función para calcular el aporte SBIF
    FUNCTION calcular_aporte_sbif(p_monto NUMBER) RETURN NUMBER IS
        v_porcentaje TRAMO_APORTE_SBIF.porc_aporte_sbif%TYPE;
    BEGIN
        SELECT porc_aporte_sbif 
        INTO v_porcentaje
        FROM TRAMO_APORTE_SBIF
        WHERE p_monto BETWEEN tramo_inf_av_sav AND tramo_sup_av_sav
          AND ROWNUM = 1;
        
        RETURN ROUND(p_monto * v_porcentaje / 100);
        
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 0;
    END calcular_aporte_sbif;

BEGIN
    -- Documentación: Bloque PL/SQL para generar reportes SBIF
    -- Procesa avances del año ANTERIOR según especificación del caso
    
    -- Truncar las tablas destino
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_APORTE_SBIF';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_APORTE_SBIF';
    
    -- Documentación: Determinar año a procesar
    -- Según especificación: información enviada en enero = transacciones del año anterior
    -- Según punto 1: procesar mismo año de ejecución
    -- procesamos el año anterior
    
    -- Procesar el año ANTERIOR al actual 
    v_anio_procesar := EXTRACT(YEAR FROM SYSDATE) - 1;
    
    -- En producción (ejecución 31 diciembre), sería: v_anio_procesar := EXTRACT(YEAR FROM SYSDATE);
    -- Pero para el ejemplo y pruebas, usamos año anterior
    
    DBMS_OUTPUT.PUT_LINE('Inicio proceso SBIF - Año procesado: ' || v_anio_procesar);
    DBMS_OUTPUT.PUT_LINE('Fecha de ejecución del proceso: ' || TO_CHAR(SYSDATE, 'DD/MM/YYYY'));
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- ============================================
    -- PROCESAMIENTO DEL DETALLE
    -- ============================================
    
    DBMS_OUTPUT.PUT_LINE('1. Procesando detalles individuales...');
    v_contador_detalle := 0;
    
    OPEN c_detalle(v_anio_procesar);
    
    LOOP
        FETCH c_detalle INTO v_num_run, v_dv_run, v_nro_tarjeta, v_nro_transaccion, 
                            v_fecha_trans, v_tipo_trans, v_monto_total;
        EXIT WHEN c_detalle%NOTFOUND;
        
        -- Cálculo del aporte en PL/SQL (no en SELECT)
        v_aporte_sbif := calcular_aporte_sbif(v_monto_total);
        
        INSERT INTO DETALLE_APORTE_SBIF (
            numrun,
            dvrun,
            nro_tarjeta,
            nro_transaccion,
            fecha_transaccion,
            tipo_transaccion,
            monto_transaccion,
            aporte_sbif
        ) VALUES (
            v_num_run,
            v_dv_run,
            v_nro_tarjeta,
            v_nro_transaccion,
            v_fecha_trans,
            v_tipo_trans,
            v_monto_total,
            v_aporte_sbif
        );
        
        v_contador_detalle := v_contador_detalle + 1;
    END LOOP;
    
    CLOSE c_detalle;
    
    DBMS_OUTPUT.PUT_LINE('   Detalles procesados: ' || v_contador_detalle || ' registros');
    
    -- ============================================
    -- PROCESAMIENTO DEL RESUMEN (OPTIMIZADO)
    -- ============================================
    
    DBMS_OUTPUT.PUT_LINE('2. Procesando resúmenes mensuales...');
    
    -- Insertar directamente desde el detalle ya calculado
    INSERT INTO RESUMEN_APORTE_SBIF (
        mes_anno,
        tipo_transaccion,
        monto_total_transacciones,
        aporte_total_abif
    )
    SELECT 
        TO_CHAR(fecha_transaccion, 'MMYYYY'),
        tipo_transaccion,
        SUM(monto_transaccion),
        SUM(aporte_sbif)
    FROM DETALLE_APORTE_SBIF
    GROUP BY 
        TO_CHAR(fecha_transaccion, 'MMYYYY'),
        tipo_transaccion
    ORDER BY 
        TO_CHAR(fecha_transaccion, 'MMYYYY'),  -- Orden ascendente por mes/año
        tipo_transaccion;                      -- Orden ascendente por tipo
    
    DBMS_OUTPUT.PUT_LINE('   Resúmenes procesados: ' || SQL%ROWCOUNT || ' registros');
    
    -- Documentación: Confirmar todos los cambios en base de datos
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('PROCESO COMPLETADO EXITOSAMENTE');
    DBMS_OUTPUT.PUT_LINE('Año procesado: ' || v_anio_procesar);
    DBMS_OUTPUT.PUT_LINE('Nota: Este proceso se ejecuta normalmente el 31/12 de cada año');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Mostrar resultados
    IF v_contador_detalle > 0 THEN
        DBMS_OUTPUT.PUT_LINE(CHR(10) || 'RESUMEN DE RESULTADOS:');
        DBMS_OUTPUT.PUT_LINE('Mes/Año | Tipo Transacción | Monto Total | Aporte SBIF');
        
        FOR r IN (SELECT * FROM RESUMEN_APORTE_SBIF ORDER BY mes_anno, tipo_transaccion) LOOP
            DBMS_OUTPUT.PUT_LINE(
                SUBSTR(r.mes_anno, 1, 2) || '/' || SUBSTR(r.mes_anno, 3, 4) || ' | ' || 
                RPAD(r.tipo_transaccion, 25) || ' | ' || 
                TO_CHAR(r.monto_total_transacciones, '999,999,999') || ' | ' || 
                TO_CHAR(r.aporte_total_abif, '999,999,999')
            );
        END LOOP;
    ELSE
        DBMS_OUTPUT.PUT_LINE(CHR(10) || 'ADVERTENCIA: No se procesaron transacciones para el año ' || v_anio_procesar);
        DBMS_OUTPUT.PUT_LINE('Verifique que existan transacciones de Avance/Súper Avance para ese año.');
    END IF;
    
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: No se encontraron datos para procesar.');
        ROLLBACK;
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        ROLLBACK;
        RAISE;
END;
/

-- Consultas para verificar resultados 
SELECT 
    ROWNUM AS NUMR,
    numrun AS DIVR,
    nro_tarjeta AS NRO_TARJETA,
    nro_transaccion || '/' || TO_CHAR(fecha_transaccion, 'DD/MM/YYYY') AS "NRO_TRANSACCION/FECHA",
    tipo_transaccion AS TIPO_TRANSACCION,
    monto_transaccion AS MONTO_TOTAL_TRANSACCION,
    aporte_sbif AS APORTE_SBIF
FROM DETALLE_APORTE_SBIF
ORDER BY fecha_transaccion, numrun;

SELECT 
    mes_anno,
    tipo_transaccion,
    monto_total_transacciones,
    aporte_total_abif
FROM RESUMEN_APORTE_SBIF
ORDER BY mes_anno, tipo_transaccion;


---OPCION 2 del CASO 2 Simula ejecución en 2021 (procesando 2020)--
--sale vacio los datos--

DECLARE
    -- Cursor para el detalle (con parámetro para el año)
    CURSOR c_detalle(p_anio NUMBER) IS
        SELECT 
            c.numrun,
            c.dvrun,
            tt.nro_tarjeta,
            tt.nro_transaccion,
            tt.fecha_transaccion,
            ttt.nombre_tptran_tarjeta AS tipo_transaccion,
            tt.monto_total_transaccion
        FROM TRANSACCION_TARJETA_CLIENTE tt
        JOIN TIPO_TRANSACCION_TARJETA ttt ON tt.cod_tptran_tarjeta = ttt.cod_tptran_tarjeta
        JOIN TARJETA_CLIENTE tc ON tt.nro_tarjeta = tc.nro_tarjeta
        JOIN CLIENTE c ON tc.numrun = c.numrun
        WHERE EXTRACT(YEAR FROM tt.fecha_transaccion) = p_anio
          AND ttt.nombre_tptran_tarjeta IN ('Avance en Efectivo', 'Súper Avance en Efectivo')
        ORDER BY tt.fecha_transaccion, c.numrun;
    
    -- Cursor para el resumen (optimizado)
    CURSOR c_resumen(p_anio NUMBER) IS
        SELECT 
            TO_CHAR(d.fecha_transaccion, 'MMYYYY') AS mes_anno,
            d.tipo_transaccion,
            SUM(d.monto_transaccion) AS monto_total_transacciones,
            SUM(d.aporte_sbif) AS aporte_total_abif
        FROM DETALLE_APORTE_SBIF d
        WHERE EXTRACT(YEAR FROM d.fecha_transaccion) = p_anio
        GROUP BY 
            TO_CHAR(d.fecha_transaccion, 'MMYYYY'),
            d.tipo_transaccion
        ORDER BY 
            TO_CHAR(d.fecha_transaccion, 'MMYYYY'),
            d.tipo_transaccion;
    
    -- Variables para el cursor de detalle
    v_num_run         CLIENTE.numrun%TYPE;
    v_dv_run          CLIENTE.dvrun%TYPE;
    v_nro_tarjeta     TARJETA_CLIENTE.nro_tarjeta%TYPE;
    v_nro_transaccion TRANSACCION_TARJETA_CLIENTE.nro_transaccion%TYPE;
    v_fecha_trans     TRANSACCION_TARJETA_CLIENTE.fecha_transaccion%TYPE;
    v_tipo_trans      TIPO_TRANSACCION_TARJETA.nombre_tptran_tarjeta%TYPE;
    v_monto_total     TRANSACCION_TARJETA_CLIENTE.monto_total_transaccion%TYPE;
    v_aporte_sbif     NUMBER;
    
    -- Variables para el cursor de resumen
    v_mes_anno               VARCHAR2(6);
    v_tipo_trans_res         VARCHAR2(50);
    v_monto_total_res        NUMBER;
    v_aporte_total_res       NUMBER;
    
    -- Variables para procesamiento
    v_anio_procesar          NUMBER;
    v_contador_detalle       NUMBER := 0;
    v_existe_datos           BOOLEAN := FALSE;
    
    -- Función para calcular el aporte SBIF
    FUNCTION calcular_aporte_sbif(p_monto NUMBER) RETURN NUMBER IS
        v_porcentaje TRAMO_APORTE_SBIF.porc_aporte_sbif%TYPE;
    BEGIN
        SELECT porc_aporte_sbif 
        INTO v_porcentaje
        FROM TRAMO_APORTE_SBIF
        WHERE p_monto BETWEEN tramo_inf_av_sav AND tramo_sup_av_sav
          AND ROWNUM = 1;
        
        RETURN ROUND(p_monto * v_porcentaje / 100);
        
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 0;
    END calcular_aporte_sbif;

BEGIN
    -- SIMULACIÓN: Ejecución en año 2021
    -- Según el caso: "el bloque PL/SQL se ejecutó el año 2021"
    -- Esto significa que procesa transacciones del año ANTERIOR (2020)
    
    DBMS_OUTPUT.PUT_LINE('=================================================');
    DBMS_OUTPUT.PUT_LINE('SIMULACIÓN: PROCESO EJECUTADO EN ENERO 2021');
    DBMS_OUTPUT.PUT_LINE('(Procesando transacciones del año anterior: 2020)');
    DBMS_OUTPUT.PUT_LINE('=================================================');
    
    -- Truncar las tablas destino
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_APORTE_SBIF';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_APORTE_SBIF';
    
    -- Procesar año 2020 (porque se ejecuta en 2021)
    v_anio_procesar := 2020;
    
    -- Verificar si hay datos de 2020
    DBMS_OUTPUT.PUT_LINE('Buscando transacciones del año: ' || v_anio_procesar);
    
    SELECT COUNT(*) INTO v_contador_detalle
    FROM TRANSACCION_TARJETA_CLIENTE tt
    JOIN TIPO_TRANSACCION_TARJETA ttt ON tt.cod_tptran_tarjeta = ttt.cod_tptran_tarjeta
    WHERE EXTRACT(YEAR FROM tt.fecha_transaccion) = v_anio_procesar
      AND ttt.nombre_tptran_tarjeta IN ('Avance en Efectivo', 'Súper Avance en Efectivo');
    
    IF v_contador_detalle = 0 THEN
        DBMS_OUTPUT.PUT_LINE('? No hay transacciones de 2020. Probando con 2021...');
        
        -- Si no hay de 2020, intentar con 2021
        v_anio_procesar := 2021;
        
        SELECT COUNT(*) INTO v_contador_detalle
        FROM TRANSACCION_TARJETA_CLIENTE tt
        JOIN TIPO_TRANSACCION_TARJETA ttt ON tt.cod_tptran_tarjeta = ttt.cod_tptran_tarjeta
        WHERE EXTRACT(YEAR FROM tt.fecha_transaccion) = v_anio_procesar
          AND ttt.nombre_tptran_tarjeta IN ('Avance en Efectivo', 'Súper Avance en Efectivo');
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('Año finalmente procesado: ' || v_anio_procesar);
    DBMS_OUTPUT.PUT_LINE('Transacciones encontradas: ' || v_contador_detalle);
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- ============================================
    -- PROCESAMIENTO DEL DETALLE
    -- ============================================
    
    IF v_contador_detalle > 0 THEN
        DBMS_OUTPUT.PUT_LINE('1. Procesando detalles individuales...');
        v_contador_detalle := 0;
        
        OPEN c_detalle(v_anio_procesar);
        
        LOOP
            FETCH c_detalle INTO v_num_run, v_dv_run, v_nro_tarjeta, v_nro_transaccion, 
                                v_fecha_trans, v_tipo_trans, v_monto_total;
            EXIT WHEN c_detalle%NOTFOUND;
            
            v_aporte_sbif := calcular_aporte_sbif(v_monto_total);
            
            INSERT INTO DETALLE_APORTE_SBIF (
                numrun, dvrun, nro_tarjeta, nro_transaccion,
                fecha_transaccion, tipo_transaccion, monto_transaccion, aporte_sbif
            ) VALUES (
                v_num_run, v_dv_run, v_nro_tarjeta, v_nro_transaccion,
                v_fecha_trans, v_tipo_trans, v_monto_total, v_aporte_sbif
            );
            
            v_contador_detalle := v_contador_detalle + 1;
        END LOOP;
        
        CLOSE c_detalle;
        
        DBMS_OUTPUT.PUT_LINE('   Detalles procesados: ' || v_contador_detalle || ' registros');
        
        -- ============================================
        -- PROCESAMIENTO DEL RESUMEN
        -- ============================================
        
        DBMS_OUTPUT.PUT_LINE('2. Procesando resúmenes mensuales...');
        
        INSERT INTO RESUMEN_APORTE_SBIF (
            mes_anno, tipo_transaccion, monto_total_transacciones, aporte_total_abif
        )
        SELECT 
            TO_CHAR(fecha_transaccion, 'MMYYYY'),
            tipo_transaccion,
            SUM(monto_transaccion),
            SUM(aporte_sbif)
        FROM DETALLE_APORTE_SBIF
        GROUP BY 
            TO_CHAR(fecha_transaccion, 'MMYYYY'),
            tipo_transaccion
        ORDER BY 
            TO_CHAR(fecha_transaccion, 'MMYYYY'),
            tipo_transaccion;
        
        DBMS_OUTPUT.PUT_LINE('   Resúmenes procesados: ' || SQL%ROWCOUNT || ' registros');
        
        COMMIT;
        
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('PROCESO COMPLETADO EXITOSAMENTE');
        DBMS_OUTPUT.PUT_LINE('Año procesado: ' || v_anio_procesar);
        DBMS_OUTPUT.PUT_LINE('(Simulando ejecución en enero 2021)');
        DBMS_OUTPUT.PUT_LINE('========================================');
    ELSE
        DBMS_OUTPUT.PUT_LINE(' No se encontraron transacciones para procesar.');
        DBMS_OUTPUT.PUT_LINE('   Para ver años disponibles ejecuta:');
        DBMS_OUTPUT.PUT_LINE('   SELECT DISTINCT EXTRACT(YEAR FROM fecha_transaccion)');
        DBMS_OUTPUT.PUT_LINE('   FROM TRANSACCION_TARJETA_CLIENTE;');
    END IF;
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        ROLLBACK;
        RAISE;
END;
/

-- CONSULTAS ESPECÍFICAS PARA SIMULACIÓN 2021
-- DETALLE_APORTE_SBIF (Año procesado debería ser 2020):

SELECT 
    ROWNUM AS NUMR,
    numrun AS DIVR,
    nro_tarjeta AS NRO_TARJETA,
    nro_transaccion || '/' || TO_CHAR(fecha_transaccion, 'DD/MM/YYYY') AS "NRO_TRANSACCION/FECHA",
    tipo_transaccion AS TIPO_TRANSACCION,
    monto_transaccion AS MONTO_TOTAL_TRANSACCION,
    aporte_sbif AS APORTE_SBIF,
    'Año: ' || EXTRACT(YEAR FROM fecha_transaccion) AS VERIFICACION_AÑO
FROM DETALLE_APORTE_SBIF
ORDER BY fecha_transaccion, numrun;

-- RESUMEN_APORTE_SBIF

SELECT 
    mes_anno,
    tipo_transaccion,
    monto_total_transacciones,
    aporte_total_abif,
    'Mes: ' || SUBSTR(mes_anno, 1, 2) || ' Año: ' || SUBSTR(mes_anno, 3, 4) AS DESGLOSE
FROM RESUMEN_APORTE_SBIF
ORDER BY mes_anno, tipo_transaccion;

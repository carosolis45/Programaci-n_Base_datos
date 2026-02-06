-- se realiza y se crea la conexion con el usuario SUMATIVA_2206_P2
-- se realiza y se ejecuta el script Script_Sumativa2.sql para crear y poblar las tablas del Modelo

-- tablas  creadas en la base de datos
-- Tabla DETALLE_APORTE_SBIF (YA EXISTE)
-- Tabla RESUMEN_APORTE_SBIF (YA EXISTE)

-- TABLA NUEVA QUE DEBEMOS CREAR
-- Tabla para registrar errores (Nueva tabla que se debe cear)

CREATE TABLE LOG_ERRORES_SBIF (
    id_log NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fecha_error DATE DEFAULT SYSDATE,
    proceso VARCHAR2(100),
    error_msg VARCHAR2(1000),
    anio_procesado NUMBER
);


-- 1. ACTIVAR SALIDA DE MENSAJES
SET SERVEROUTPUT ON;
SET VERIFY OFF;

-- 2. BLOQUE PL/SQL ANÓNIMO CON CURSORES EXPLÍCITOS
DECLARE
    -- ============================================
    -- DECLARACIÓN DE VARIABLES
    -- ============================================
    
    -- Variable para el año a procesar 
    v_anio_procesar NUMBER := 2025;  -- <-- esto se puede ir modificando, el año 2021 no hay informacion lo hice 2025
    
    -- Variables de control
    v_contador_detalle NUMBER := 0;
    v_contador_resumen NUMBER := 0;
    v_total_aporte NUMBER := 0;
    
    -- Variables para cálculo
    v_porcentaje_aporte NUMBER;
    v_aporte_calculado NUMBER;
    
    -- Variables para el cursor de detalle
    v_run CLIENTE.numrun%TYPE;
    v_dv CLIENTE.dvrun%TYPE;
    v_tarjeta TARJETA_CLIENTE.nro_tarjeta%TYPE;
    v_transaccion TRANSACCION_TARJETA_CLIENTE.nro_transaccion%TYPE;
    v_fecha TRANSACCION_TARJETA_CLIENTE.fecha_transaccion%TYPE;
    v_tipo TIPO_TRANSACCION_TARJETA.nombre_tptran_tarjeta%TYPE;
    v_monto TRANSACCION_TARJETA_CLIENTE.monto_transaccion%TYPE;
    v_monto_total TRANSACCION_TARJETA_CLIENTE.monto_total_transaccion%TYPE;
    
    -- Variables para el cursor de resumen
    v_mes_anno VARCHAR2(6);
    v_tipo_resumen VARCHAR2(40);
    v_monto_mes NUMBER;
    v_aporte_mes NUMBER;
    
    -- ============================================
    -- DECLARACIÓN DE EXCEPCIONES
    -- ============================================
    
    -- Excepciones personalizadas según requerimiento
    e_sin_datos EXCEPTION;                      -- Excepción que fue definida 
    e_error_proceso EXCEPTION;                  -- Excepción que fue definida 
    PRAGMA EXCEPTION_INIT(e_error_proceso, -20001);
    
    -- ============================================
    -- DECLARACIÓN DE CURSORES EXPLÍCITOS
    -- ============================================
    
    -- CURSOR 1: Para detalle de transacciones (con parámetro)
    CURSOR c_detalle_transacciones(p_anio NUMBER) IS
        SELECT c.numrun, c.dvrun, tc.nro_tarjeta,
               ttc.nro_transaccion, ttc.fecha_transaccion,
               ttt.nombre_tptran_tarjeta as tipo_transaccion,
               ttc.monto_transaccion, ttc.monto_total_transaccion
        FROM cliente c
        JOIN tarjeta_cliente tc ON c.numrun = tc.numrun
        JOIN transaccion_tarjeta_cliente ttc ON tc.nro_tarjeta = ttc.nro_tarjeta
        JOIN tipo_transaccion_tarjeta ttt ON ttc.cod_tptran_tarjeta = ttt.cod_tptran_tarjeta
        WHERE EXTRACT(YEAR FROM ttc.fecha_transaccion) = p_anio
          AND ttt.nombre_tptran_tarjeta IN ('Avance en Efectivo', 'Súper Avance en Efectivo')
        ORDER BY ttc.fecha_transaccion, c.numrun;
    
    -- CURSOR 2: Para resumen mensual (con parámetro) - AJUSTE 1: LPAD para formato de mes
    CURSOR c_resumen_mensual(p_anio NUMBER) IS
        SELECT LPAD(TO_CHAR(ttc.fecha_transaccion, 'MM'), 2, '0') || p_anio as mes_anno,
               ttt.nombre_tptran_tarjeta as tipo_transaccion,
               SUM(ttc.monto_total_transaccion) as monto_total
        FROM transaccion_tarjeta_cliente ttc
        JOIN tipo_transaccion_tarjeta ttt ON ttc.cod_tptran_tarjeta = ttt.cod_tptran_tarjeta
        WHERE EXTRACT(YEAR FROM ttc.fecha_transaccion) = p_anio
          AND ttt.nombre_tptran_tarjeta IN ('Avance en Efectivo', 'Súper Avance en Efectivo')
        GROUP BY LPAD(TO_CHAR(ttc.fecha_transaccion, 'MM'), 2, '0') || p_anio, ttt.nombre_tptran_tarjeta
        ORDER BY mes_anno, ttt.nombre_tptran_tarjeta;
    
    -- ============================================
    -- PROCEDIMIENTO PARA REGISTRAR ERRORES
    -- ============================================
    
    PROCEDURE registrar_error(
        p_proceso IN VARCHAR2,
        p_error_msg IN VARCHAR2
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;  -- Para que el COMMIT no afecte el bloque principal
    BEGIN
        INSERT INTO LOG_ERRORES_SBIF (proceso, error_msg, anio_procesado)
        VALUES (p_proceso, p_error_msg, v_anio_procesar);
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            NULL;  -- Si falla el log, continuamos
    END registrar_error;
    
    -- ============================================
    -- FUNCIÓN PARA CALCULAR APORTE SBIF - AJUSTE 2: Incluir montos menores a 30.000
    -- ============================================
    
    FUNCTION calcular_aporte_sbif(p_monto_total NUMBER) RETURN NUMBER IS
        v_porcentaje NUMBER;
    BEGIN
        -- Determinar porcentaje según tabla TRAMO_APORTE_SBIF
        IF p_monto_total < 30000 THEN
            v_porcentaje := 0;  -- Montos menores a 30.000 no pagan aporte
        ELSIF p_monto_total BETWEEN 30000 AND 100000 THEN
            v_porcentaje := 1;
        ELSIF p_monto_total BETWEEN 100001 AND 200000 THEN
            v_porcentaje := 2;
        ELSIF p_monto_total BETWEEN 200001 AND 400000 THEN
            v_porcentaje := 3;
        ELSIF p_monto_total BETWEEN 400001 AND 600000 THEN
            v_porcentaje := 4;
        ELSE
            v_porcentaje := 7;  -- Para montos mayores a 600.000
        END IF;
        
        -- Calcular y redondear aporte
        RETURN ROUND(p_monto_total * v_porcentaje / 100);
    END calcular_aporte_sbif;

BEGIN
    -- ============================================
    -- INICIO DEL PROCESO
    -- ============================================
    
    DBMS_OUTPUT.PUT_LINE('=============================================');
    DBMS_OUTPUT.PUT_LINE('PROCESO DE APORTES SBIF - SUMATIVA 2');
    DBMS_OUTPUT.PUT_LINE('Usuario: ' || USER);
    DBMS_OUTPUT.PUT_LINE('Año a procesar: ' || v_anio_procesar);
    DBMS_OUTPUT.PUT_LINE('Fecha/hora inicio: ' || TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('=============================================');
    
    -- ============================================
    -- PASO 1: TRUNCAR TABLAS DESTINO
    -- ============================================
    
    DBMS_OUTPUT.PUT_LINE('PASO 1: Limpieza de tablas destino...');
    
    BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_APORTE_SBIF';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_APORTE_SBIF';
        DBMS_OUTPUT.PUT_LINE('? Tablas DETALLE_APORTE_SBIF y RESUMEN_APORTE_SBIF truncadas');
    EXCEPTION
        WHEN OTHERS THEN
            registrar_error('TRUNCATE_TABLAS', 'Error al truncar tablas: ' || SQLERRM);
            RAISE e_error_proceso;
    END;
    
    -- ============================================
    -- PASO 2: PROCESAR TRANSACCIONES (DETALLE)
    -- ============================================
    
    DBMS_OUTPUT.PUT_LINE('PASO 2: Procesando transacciones de detalle...');
    
    BEGIN
        -- Abrir cursor con parámetro
        OPEN c_detalle_transacciones(v_anio_procesar);
        
        LOOP
            FETCH c_detalle_transacciones INTO v_run, v_dv, v_tarjeta, v_transaccion,
                                                v_fecha, v_tipo, v_monto, v_monto_total;
            EXIT WHEN c_detalle_transacciones%NOTFOUND;
            
            v_contador_detalle := v_contador_detalle + 1;
            
            -- Calcular aporte usando función
            v_aporte_calculado := calcular_aporte_sbif(v_monto_total);
            v_total_aporte := v_total_aporte + v_aporte_calculado;
            
            -- Insertar en tabla DETALLE_APORTE_SBIF
            INSERT INTO DETALLE_APORTE_SBIF (
                numrun, dvrun, nro_tarjeta, nro_transaccion,
                fecha_transaccion, tipo_transaccion,
                monto_transaccion, aporte_sbif
            ) VALUES (
                v_run, v_dv, v_tarjeta, v_transaccion,
                v_fecha, v_tipo, v_monto, v_aporte_calculado
            );
            
            -- Mostrar progreso cada 20 registros
            IF MOD(v_contador_detalle, 20) = 0 THEN
                DBMS_OUTPUT.PUT_LINE('  Procesados ' || v_contador_detalle || ' registros...');
            END IF;
            
        END LOOP;
        
        CLOSE c_detalle_transacciones;
        
        -- Verificar si se procesaron datos
        IF v_contador_detalle = 0 THEN
            RAISE e_sin_datos;
        END IF;
        
        DBMS_OUTPUT.PUT_LINE('? ' || v_contador_detalle || ' transacciones procesadas');
        DBMS_OUTPUT.PUT_LINE('? Aporte total acumulado: $' || TO_CHAR(v_total_aporte, '999,999,999'));
        
    EXCEPTION
        WHEN e_sin_datos THEN
            DBMS_OUTPUT.PUT_LINE('? ERROR: No hay transacciones para el año ' || v_anio_procesar);
            registrar_error('PROCESO_DETALLE', 'Sin datos para año ' || v_anio_procesar);
            RAISE;
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('? ERROR en procesamiento detalle: ' || SQLERRM);
            registrar_error('PROCESO_DETALLE', SQLERRM);
            RAISE e_error_proceso;
    END;
    
    -- ============================================
    -- PASO 3: GENERAR RESUMEN MENSUAL
    -- ============================================
    
    DBMS_OUTPUT.PUT_LINE('PASO 3: Generando resumen mensual...');
    
    DECLARE
        v_aporte_total_mes NUMBER;
        v_monto_individual NUMBER;
        
        -- Cursor para calcular aporte por transacción en cada mes
        CURSOR c_transacciones_mes(p_mes VARCHAR2, p_tipo VARCHAR2) IS
            SELECT ttc.monto_total_transaccion
            FROM transaccion_tarjeta_cliente ttc
            JOIN tipo_transaccion_tarjeta ttt ON ttc.cod_tptran_tarjeta = ttt.cod_tptran_tarjeta
            WHERE TO_CHAR(ttc.fecha_transaccion, 'MM') || v_anio_procesar = p_mes
              AND ttt.nombre_tptran_tarjeta = p_tipo
              AND EXTRACT(YEAR FROM ttc.fecha_transaccion) = v_anio_procesar;
    
    BEGIN
        -- Abrir cursor de resumen
        OPEN c_resumen_mensual(v_anio_procesar);
        
        LOOP
            FETCH c_resumen_mensual INTO v_mes_anno, v_tipo_resumen, v_monto_mes;
            EXIT WHEN c_resumen_mensual%NOTFOUND;
            
            v_contador_resumen := v_contador_resumen + 1;
            v_aporte_total_mes := 0;
            
            -- Calcular aporte total para este mes/tipo
            FOR reg_mes IN c_transacciones_mes(v_mes_anno, v_tipo_resumen) LOOP
                v_aporte_total_mes := v_aporte_total_mes + calcular_aporte_sbif(reg_mes.monto_total_transaccion);
            END LOOP;
            
            -- Insertar en tabla RESUMEN_APORTE_SBIF
            INSERT INTO RESUMEN_APORTE_SBIF (
                mes_anno, tipo_transaccion,
                monto_total_transacciones, aporte_total_abif
            ) VALUES (
                v_mes_anno, v_tipo_resumen,
                v_monto_mes, v_aporte_total_mes
            );
            
            DBMS_OUTPUT.PUT_LINE('  Mes ' || v_mes_anno || ' - ' || v_tipo_resumen || 
                               ': $' || TO_CHAR(v_monto_mes, '999,999,999'));
            
        END LOOP;
        
        CLOSE c_resumen_mensual;
        
        DBMS_OUTPUT.PUT_LINE('? ' || v_contador_resumen || ' registros de resumen generados');
        
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('? ERROR en generación de resumen: ' || SQLERRM);
            registrar_error('PROCESO_RESUMEN', SQLERRM);
            RAISE e_error_proceso;
    END;
    
    -- ============================================
    -- PASO 4: CONFIRMAR TRANSACCIÓN
    -- ============================================
    
    DBMS_OUTPUT.PUT_LINE('PASO 4: Confirmando transacción...');
    
    -- Verificar integridad (contador vs registros insertados)
    DECLARE
        v_total_detalle NUMBER;
        v_total_resumen NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_total_detalle FROM DETALLE_APORTE_SBIF;
        SELECT COUNT(*) INTO v_total_resumen FROM RESUMEN_APORTE_SBIF;
        
        IF v_contador_detalle = v_total_detalle AND v_contador_detalle > 0 THEN
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('? Transacción confirmada exitosamente');
            DBMS_OUTPUT.PUT_LINE('? Verificación OK: ' || v_contador_detalle || ' = ' || v_total_detalle);
        ELSE
            RAISE e_error_proceso;
        END IF;
        
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            registrar_error('VERIFICACION', 'Error en verificación de datos');
            RAISE e_error_proceso;
    END;
    
    -- ============================================
    -- RESULTADO FINAL
    -- ============================================
    
    DBMS_OUTPUT.PUT_LINE('=============================================');
    DBMS_OUTPUT.PUT_LINE('¡PROCESO COMPLETADO EXITOSAMENTE!');
    DBMS_OUTPUT.PUT_LINE('RESULTADOS:');
    DBMS_OUTPUT.PUT_LINE('  • Año procesado: ' || v_anio_procesar);
    DBMS_OUTPUT.PUT_LINE('  • Transacciones procesadas: ' || v_contador_detalle);
    DBMS_OUTPUT.PUT_LINE('  • Registros de resumen: ' || v_contador_resumen);
    DBMS_OUTPUT.PUT_LINE('  • Aporte total SBIF: $' || TO_CHAR(v_total_aporte, '999,999,999'));
    DBMS_OUTPUT.PUT_LINE('  • Usuario ejecutor: ' || USER);
    DBMS_OUTPUT.PUT_LINE('  • Hora finalización: ' || TO_CHAR(SYSDATE, 'HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('=============================================');
    
EXCEPTION
    -- ============================================
    -- MANEJO DE EXCEPCIONES
    -- ============================================
    
    WHEN e_sin_datos THEN
        DBMS_OUTPUT.PUT_LINE('EXCEPCIÓN DEFINIDA POR USUARIO: Sin datos para procesar');
        ROLLBACK;
        
    WHEN e_error_proceso THEN
        DBMS_OUTPUT.PUT_LINE('EXCEPCIÓN DEFINIDA POR USUARIO: Error en el proceso');
        ROLLBACK;
        
    WHEN NO_DATA_FOUND THEN  -- Excepción predefinida
        DBMS_OUTPUT.PUT_LINE('EXCEPCIÓN PREDEFINIDA: No se encontraron datos');
        registrar_error('NO_DATA_FOUND', 'Error NO_DATA_FOUND: ' || SQLERRM);
        ROLLBACK;
        
    WHEN TOO_MANY_ROWS THEN  -- Excepción predefinida
        DBMS_OUTPUT.PUT_LINE('EXCEPCIÓN PREDEFINIDA: Múltiples filas encontradas');
        registrar_error('TOO_MANY_ROWS', 'Error TOO_MANY_ROWS: ' || SQLERRM);
        ROLLBACK;
        
    WHEN OTHERS THEN  -- Excepción no predefinida
        DBMS_OUTPUT.PUT_LINE('EXCEPCIÓN NO PREDEFINIDA: ' || SQLERRM);
        registrar_error('OTHERS', 'Error OTHERS: ' || SQLERRM);
        ROLLBACK;
        
END;
/

-- ============================================
-- CONSULTAS PARA VERIFICAR RESULTADOS
-- ============================================

-- Conteo general
SELECT 'DETALLE_APORTE_SBIF' as tabla, COUNT(*) as registros FROM DETALLE_APORTE_SBIF
UNION ALL
SELECT 'RESUMEN_APORTE_SBIF', COUNT(*) FROM RESUMEN_APORTE_SBIF
UNION ALL
SELECT 'APORTE TOTAL', SUM(aporte_sbif) FROM DETALLE_APORTE_SBIF;

-- Ver primeros 5 registros de detalle
---Primeros 5 registros de DETALLE_APORTE_SBIF:
SELECT * FROM (
    SELECT numrun, dvrun, tipo_transaccion, 
           TO_CHAR(fecha_transaccion, 'DD/MM/YYYY') as fecha,
           TO_CHAR(monto_transaccion, '999,999,999') as monto,
           TO_CHAR(aporte_sbif, '999,999,999') as aporte
    FROM DETALLE_APORTE_SBIF
    ORDER BY fecha_transaccion, numrun
) WHERE ROWNUM <= 5;

-- Ver resumen completo
-- Resumen mensual completo (RESUMEN_APORTE_SBIF):
SELECT mes_anno, tipo_transaccion,
       TO_CHAR(monto_total_transacciones, '999,999,999') as monto_total,
       TO_CHAR(aporte_total_abif, '999,999,999') as aporte_total
FROM RESUMEN_APORTE_SBIF
ORDER BY mes_anno, tipo_transaccion;

-- Ver si hubo errores
-- Registro de errores (LOG_ERRORES_SBIF):
SELECT TO_CHAR(fecha_error, 'DD/MM/YYYY HH24:MI:SS') as fecha,
       proceso, error_msg
FROM LOG_ERRORES_SBIF
ORDER BY fecha_error DESC;


-- Ver datos de muestra de RESUMEN_APORTE_SBIF
SELECT * FROM RESUMEN_APORTE_SBIF
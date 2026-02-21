-- se crea el usuario  SUMATIVA_2206_P3
-- una vez conectado con la conexión SUMATIVA_2206_P3, se realiza el script_prueba3_FC.sql que creará y poblará las tablas 

--SOLUCIÓN INTEGRAL - SISTEMA DE GESTIÓN HOTEL "LA ÚLTIMA OPORTUNIDAD"
 

SET SERVEROUTPUT ON;
SET FEEDBACK ON;

-- =============================================================================
-- ELIMINAR OBJETOS EXISTENTES
-- =============================================================================
BEGIN
    FOR obj IN (SELECT object_name, object_type FROM user_objects 
                WHERE object_name IN ('TRG_ACTUALIZA_TOTAL_CONSUMOS',
                                      'PKG_GESTION_COBRANZA',
                                      'FN_OBTEN_AGENCIA',
                                      'FN_OBTEN_TOTAL_CONSUMOS',
                                      'SP_CALCULO_PAGOS_DIARIOS'))
    LOOP
        IF obj.object_type = 'PACKAGE' THEN
            EXECUTE IMMEDIATE 'DROP PACKAGE ' || obj.object_name;
        ELSIF obj.object_type = 'PROCEDURE' THEN
            EXECUTE IMMEDIATE 'DROP PROCEDURE ' || obj.object_name;
        ELSIF obj.object_type = 'FUNCTION' THEN
            EXECUTE IMMEDIATE 'DROP FUNCTION ' || obj.object_name;
        ELSIF obj.object_type = 'TRIGGER' THEN
            EXECUTE IMMEDIATE 'DROP TRIGGER ' || obj.object_name;
        END IF;
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

-- =============================================================================
-- CASO 1: TRIGGER PARA ACTUALIZACIÓN DE TOTAL_CONSUMOS
-- se activa automáticamente cada vez que alguien modifica la tabla CONSUMO
-- Mantiene siempre actualizada la tabla TOTAL_CONSUMOS que guarda el monto total por huésped
-- =============================================================================
CREATE OR REPLACE TRIGGER trg_actualiza_total_consumos
    FOR INSERT OR UPDATE OR DELETE ON consumo
    COMPOUND TRIGGER

    TYPE r_consumo_change IS RECORD (
        id_huesped consumo.id_huesped%TYPE,
        monto      consumo.monto%TYPE,
        operacion  VARCHAR2(1)
    );
    TYPE t_consumo_changes IS TABLE OF r_consumo_change INDEX BY PLS_INTEGER;
    g_changes t_consumo_changes;
    g_idx     PLS_INTEGER := 0;

    BEFORE EACH ROW IS
    BEGIN
        CASE
            WHEN INSERTING THEN
                g_idx := g_idx + 1;
                g_changes(g_idx).id_huesped := :NEW.id_huesped;
                g_changes(g_idx).monto := :NEW.monto;
                g_changes(g_idx).operacion := 'I';

            WHEN UPDATING THEN
                g_idx := g_idx + 1;
                g_changes(g_idx).id_huesped := :OLD.id_huesped;
                g_changes(g_idx).monto := :OLD.monto;
                g_changes(g_idx).operacion := 'O';

                g_idx := g_idx + 1;
                g_changes(g_idx).id_huesped := :NEW.id_huesped;
                g_changes(g_idx).monto := :NEW.monto;
                g_changes(g_idx).operacion := 'U';

            WHEN DELETING THEN
                g_idx := g_idx + 1;
                g_changes(g_idx).id_huesped := :OLD.id_huesped;
                g_changes(g_idx).monto := :OLD.monto;
                g_changes(g_idx).operacion := 'D';
        END CASE;
    END BEFORE EACH ROW;

    AFTER STATEMENT IS
        v_current_total NUMBER;
    BEGIN
        FOR i IN 1..g_changes.COUNT LOOP
            IF g_changes(i).operacion IN ('I', 'U') THEN
                BEGIN
                    SELECT monto_consumos INTO v_current_total
                    FROM total_consumos
                    WHERE id_huesped = g_changes(i).id_huesped
                    FOR UPDATE;

                    UPDATE total_consumos
                    SET monto_consumos = v_current_total + g_changes(i).monto
                    WHERE id_huesped = g_changes(i).id_huesped;

                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        INSERT INTO total_consumos (id_huesped, monto_consumos)
                        VALUES (g_changes(i).id_huesped, g_changes(i).monto);
                END;

            ELSIF g_changes(i).operacion IN ('D', 'O') THEN
                BEGIN
                    SELECT monto_consumos INTO v_current_total
                    FROM total_consumos
                    WHERE id_huesped = g_changes(i).id_huesped
                    FOR UPDATE;

                    UPDATE total_consumos
                    SET monto_consumos = v_current_total - g_changes(i).monto
                    WHERE id_huesped = g_changes(i).id_huesped;

                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        NULL;
                END;
            END IF;
        END LOOP;
    END AFTER STATEMENT;

END trg_actualiza_total_consumos;
/

-- =============================================================================
-- PRUEBAS DEL CASO 1
-- =============================================================================
DECLARE
    CURSOR c_cons_huesped (p_id_huesped NUMBER) IS
        SELECT id_consumo, id_reserva, id_huesped, monto
        FROM consumo
        WHERE id_huesped = p_id_huesped
        ORDER BY id_consumo;

    v_total_340006 NUMBER;
    v_total_340004 NUMBER;
    v_total_340008 NUMBER;
    v_nuevo_id NUMBER;
    
BEGIN
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '--- [CASO 1] PRUEBAS DEL TRIGGER ---');
    DBMS_OUTPUT.PUT_LINE('--- ESTADO INICIAL ---');

    -- Mostrar datos iniciales para huésped 340006
    DBMS_OUTPUT.PUT_LINE('Consumos de 340006 (inicial):');
    FOR rec IN c_cons_huesped(340006) LOOP
        DBMS_OUTPUT.PUT_LINE('  ID: ' || rec.id_consumo || ', Monto: ' || rec.monto);
    END LOOP;
    
    SELECT NVL(SUM(monto), 0) INTO v_total_340006 FROM consumo WHERE id_huesped = 340006;
    DBMS_OUTPUT.PUT_LINE('Total consumos 340006 (inicial - calculado): ' || v_total_340006);

    -- Mostrar datos iniciales para huésped 340004
    DBMS_OUTPUT.PUT_LINE(CHR(10) || 'Consumos de 340004 (inicial):');
    FOR rec IN c_cons_huesped(340004) LOOP
        DBMS_OUTPUT.PUT_LINE('  ID: ' || rec.id_consumo || ', Monto: ' || rec.monto);
    END LOOP;
    
    SELECT NVL(SUM(monto), 0) INTO v_total_340004 FROM consumo WHERE id_huesped = 340004;
    DBMS_OUTPUT.PUT_LINE('Total consumos 340004 (inicial - calculado): ' || v_total_340004);

    -- Mostrar datos iniciales para huésped 340008
    DBMS_OUTPUT.PUT_LINE(CHR(10) || 'Consumos de 340008 (inicial):');
    FOR rec IN c_cons_huesped(340008) LOOP
        DBMS_OUTPUT.PUT_LINE('  ID: ' || rec.id_consumo || ', Monto: ' || rec.monto);
    END LOOP;
    
    SELECT NVL(SUM(monto), 0) INTO v_total_340008 FROM consumo WHERE id_huesped = 340008;
    DBMS_OUTPUT.PUT_LINE('Total consumos 340008 (inicial - calculado): ' || v_total_340008);

    -- 1. INSERTAR un nuevo consumo para 340006
    SELECT NVL(MAX(id_consumo), 0) + 1 INTO v_nuevo_id FROM consumo;
    
    INSERT INTO consumo (id_consumo, id_reserva, id_huesped, monto)
    VALUES (v_nuevo_id, 1587, 340006, 150);
    DBMS_OUTPUT.PUT_LINE(CHR(10) || 'Operación 1: INSERT (340006, +150) ejecutada.');

    -- 2. ELIMINAR el consumo con ID 11473
    DELETE FROM consumo WHERE id_consumo = 11473;
    DBMS_OUTPUT.PUT_LINE('Operación 2: DELETE (ID 11473) ejecutada.');

    -- 3. ACTUALIZAR el consumo con ID 10688 a monto 95
    UPDATE consumo SET monto = 95 WHERE id_consumo = 10688;
    DBMS_OUTPUT.PUT_LINE('Operación 3: UPDATE (ID 10688, nuevo monto 95) ejecutada.');

    COMMIT;

    DBMS_OUTPUT.PUT_LINE(CHR(10) || '--- ESTADO FINAL ---');

    -- Mostrar datos finales
    DBMS_OUTPUT.PUT_LINE('Consumos de 340006 (final):');
    FOR rec IN c_cons_huesped(340006) LOOP
        DBMS_OUTPUT.PUT_LINE('  ID: ' || rec.id_consumo || ', Monto: ' || rec.monto);
    END LOOP;
    
    SELECT NVL(SUM(monto), 0) INTO v_total_340006 FROM consumo WHERE id_huesped = 340006;
    DBMS_OUTPUT.PUT_LINE('Total consumos 340006 (final - calculado): ' || v_total_340006);

    DBMS_OUTPUT.PUT_LINE(CHR(10) || 'Consumos de 340004 (final):');
    FOR rec IN c_cons_huesped(340004) LOOP
        DBMS_OUTPUT.PUT_LINE('  ID: ' || rec.id_consumo || ', Monto: ' || rec.monto);
    END LOOP;
    
    SELECT NVL(SUM(monto), 0) INTO v_total_340004 FROM consumo WHERE id_huesped = 340004;
    DBMS_OUTPUT.PUT_LINE('Total consumos 340004 (final - calculado): ' || v_total_340004);

    DBMS_OUTPUT.PUT_LINE(CHR(10) || 'Consumos de 340008 (final):');
    FOR rec IN c_cons_huesped(340008) LOOP
        DBMS_OUTPUT.PUT_LINE('  ID: ' || rec.id_consumo || ', Monto: ' || rec.monto);
    END LOOP;
    
    SELECT NVL(SUM(monto), 0) INTO v_total_340008 FROM consumo WHERE id_huesped = 340008;
    DBMS_OUTPUT.PUT_LINE('Total consumos 340008 (final - calculado): ' || v_total_340008);
    
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '--- PRUEBAS CASO 1 FINALIZADAS ---');
    
END;
/

-- =============================================================================
-- CASO 2: PACKAGE, FUNCIONES Y PROCEDIMIENTO
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. PACKAGE PARA TOURS
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE pkg_gestion_cobranza IS
    v_monto_tours NUMBER := 0;
    FUNCTION fn_monto_tours(p_id_huesped NUMBER) RETURN NUMBER;
END pkg_gestion_cobranza;
/

CREATE OR REPLACE PACKAGE BODY pkg_gestion_cobranza IS
    FUNCTION fn_monto_tours(p_id_huesped NUMBER) RETURN NUMBER IS
        v_total_tours NUMBER := 0;
    BEGIN
        SELECT NVL(SUM(ht.num_personas * t.valor_tour), 0)
        INTO v_total_tours
        FROM huesped_tour ht
        JOIN tour t ON ht.id_tour = t.id_tour
        WHERE ht.id_huesped = p_id_huesped;

        v_monto_tours := v_total_tours;
        RETURN v_total_tours;

    EXCEPTION
        WHEN OTHERS THEN
            v_monto_tours := 0;
            RETURN 0;
    END fn_monto_tours;
END pkg_gestion_cobranza;
/

-- -----------------------------------------------------------------------------
-- 2. FUNCIÓN PARA OBTENER AGENCIA
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_obten_agencia(p_id_huesped NUMBER) 
RETURN VARCHAR2 IS
    v_nom_agencia agencia.nom_agencia%TYPE;
    v_error_msg VARCHAR2(4000);
BEGIN
    BEGIN
        SELECT a.nom_agencia
        INTO v_nom_agencia
        FROM huesped h
        LEFT JOIN agencia a ON h.id_agencia = a.id_agencia
        WHERE h.id_huesped = p_id_huesped;

        IF v_nom_agencia IS NULL THEN
            RETURN 'NO REGISTRA AGENCIA';
        END IF;

        RETURN v_nom_agencia;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_error_msg := SQLERRM;
            INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
            VALUES (sq_error.NEXTVAL, 'fn_obten_agencia', 'Cliente ID ' || p_id_huesped || ': ' || v_error_msg);
            COMMIT;
            RETURN 'NO REGISTRA AGENCIA';

        WHEN OTHERS THEN
            v_error_msg := SQLERRM;
            INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
            VALUES (sq_error.NEXTVAL, 'fn_obten_agencia', 'Error inesperado: ' || v_error_msg);
            COMMIT;
            RETURN 'NO REGISTRA AGENCIA';
    END;
END fn_obten_agencia;
/

-- -----------------------------------------------------------------------------
-- 3. FUNCIÓN PARA OBTENER TOTAL CONSUMOS
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_obten_total_consumos(p_id_huesped NUMBER) 
RETURN NUMBER IS
    v_monto_consumos NUMBER;
    v_error_msg VARCHAR2(4000);
BEGIN
    BEGIN
        SELECT monto_consumos
        INTO v_monto_consumos
        FROM total_consumos
        WHERE id_huesped = p_id_huesped;
        
        RETURN v_monto_consumos;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 0;
        WHEN OTHERS THEN
            v_error_msg := SQLERRM;
            INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
            VALUES (sq_error.NEXTVAL, 'fn_obten_total_consumos', v_error_msg);
            COMMIT;
            RETURN 0;
    END;
END fn_obten_total_consumos;
/

-- -----------------------------------------------------------------------------
-- 4. PROCEDIMIENTO PRINCIPAL 
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sp_calculo_pagos_diarios (
    p_fecha_salida DATE,
    p_valor_dolar NUMBER
) IS
    CURSOR c_huespedes_salida IS
        SELECT DISTINCT h.id_huesped,
               h.appat_huesped || ' ' || h.apmat_huesped || ', ' || h.nom_huesped AS nombre_completo,
               r.id_reserva,
               r.ingreso,
               r.estadia
        FROM huesped h
        JOIN reserva r ON h.id_huesped = r.id_huesped
        WHERE (r.ingreso + r.estadia) = TRUNC(p_fecha_salida);

    v_alojamiento_usd NUMBER := 0;
    v_subtotal_usd NUMBER := 0;
    v_cargo_personas_clp CONSTANT NUMBER := 35000;
    v_cargo_personas_usd NUMBER := 0;
    v_consumos_usd NUMBER;
    v_tours_usd NUMBER;
    v_agencia_nom VARCHAR2(100);
    v_descuento_agencia_usd NUMBER := 0;
    v_total_pagar_usd NUMBER := 0;
    v_total_pagar_clp NUMBER := 0;
    v_cognomos_clp NUMBER := 0;
    v_base_cognomos_usd NUMBER := 0;
    v_pct_cognomos NUMBER := 0;
    v_error_msg VARCHAR2(4000);

BEGIN
    -- Limpieza de tablas
    EXECUTE IMMEDIATE 'TRUNCATE TABLE detalle_diario_huespedes';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE reg_errores';

    DBMS_OUTPUT.PUT_LINE(CHR(10) || '--- [CASO 2] INICIANDO PROCESO DE COBRANZA ---');
    DBMS_OUTPUT.PUT_LINE('Fecha de Salida: ' || TO_CHAR(p_fecha_salida, 'DD/MM/YYYY'));
    DBMS_OUTPUT.PUT_LINE('Valor Dólar: $' || p_valor_dolar);

    FOR rec_huesped IN c_huespedes_salida LOOP
        -- 1. Calcular ALOJAMIENTO
        BEGIN
            SELECT NVL(SUM(h.valor_habitacion + h.valor_minibar), 0)
            INTO v_alojamiento_usd
            FROM detalle_reserva dr
            JOIN habitacion h ON dr.id_habitacion = h.id_habitacion
            WHERE dr.id_reserva = rec_huesped.id_reserva;

            v_alojamiento_usd := v_alojamiento_usd * rec_huesped.estadia;
        EXCEPTION
            WHEN OTHERS THEN
                v_alojamiento_usd := 0;
        END;

        -- 2. Obtener CONSUMOS
        v_consumos_usd := fn_obten_total_consumos(rec_huesped.id_huesped);

        -- 3. Obtener TOURS
        v_tours_usd := pkg_gestion_cobranza.fn_monto_tours(rec_huesped.id_huesped);

        -- 4. Calcular CARGO POR PERSONAS
        v_cargo_personas_usd := ROUND(v_cargo_personas_clp / p_valor_dolar);

        -- 5. Calcular SUBTOTAL
        v_subtotal_usd := v_alojamiento_usd + v_consumos_usd + v_tours_usd + v_cargo_personas_usd;

        -- 6. Obtener AGENCIA
        v_agencia_nom := fn_obten_agencia(rec_huesped.id_huesped);
        
        -- 7. Calcular descuento agencia (12% solo para VIAJES ALBERTI)
        IF v_agencia_nom = 'VIAJES ALBERTI' THEN
            v_descuento_agencia_usd := ROUND(v_subtotal_usd * 12 / 100);
        ELSE
            v_descuento_agencia_usd := 0;
        END IF;

        -- 8. Calcular DESCUENTO POR CONSUMOS 
        v_base_cognomos_usd := v_alojamiento_usd + v_consumos_usd + v_cargo_personas_usd;
        
        BEGIN
            SELECT NVL(MAX(pct), 0)
            INTO v_pct_cognomos
            FROM tramos_consumos
            WHERE v_consumos_usd BETWEEN vmin_tramo AND vmax_tramo;
        EXCEPTION
            WHEN OTHERS THEN
                v_pct_cognomos := 0;
        END;
        
        v_cognomos_clp := ROUND(v_base_cognomos_usd * v_pct_cognomos * p_valor_dolar);

        -- 9. Calcular TOTAL
        v_total_pagar_usd := v_subtotal_usd - v_descuento_agencia_usd;
        v_total_pagar_clp := ROUND(v_total_pagar_usd * p_valor_dolar) - v_cognomos_clp;

        -- 10. Insertar en DETALLE_DIARIO_HUESPEDES
        INSERT INTO detalle_diario_huespedes (
            id_huesped, nombre, agencia, alojamiento, consumos, tours, 
            subtotal_pago, descuento_consumos, descuentos_agencia, total
        ) VALUES (
            rec_huesped.id_huesped,
            rec_huesped.nombre_completo,
            v_agencia_nom,
            ROUND(v_alojamiento_usd * p_valor_dolar),
            ROUND(v_consumos_usd * p_valor_dolar),
            ROUND(v_tours_usd * p_valor_dolar),
            ROUND(v_subtotal_usd * p_valor_dolar),
            v_cognomos_clp,
            ROUND(v_descuento_agencia_usd * p_valor_dolar),
            v_total_pagar_clp
        );

        DBMS_OUTPUT.PUT_LINE('Procesado: Huésped ' || rec_huesped.id_huesped || 
                           ' - Total a pagar: $' || v_total_pagar_clp);

    END LOOP;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('--- PROCESO FINALIZADO ---');

EXCEPTION
    WHEN OTHERS THEN
        v_error_msg := SQLERRM;
        DBMS_OUTPUT.PUT_LINE('Error en el procedimiento principal: ' || v_error_msg);
        INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
        VALUES (sq_error.NEXTVAL, 'sp_calculo_pagos_diarios', v_error_msg);
        ROLLBACK;
        RAISE;
END sp_calculo_pagos_diarios;
/

-- =============================================================================
-- PRUEBAS FINALES
-- =============================================================================
DECLARE
    v_consumos NUMBER;
    v_agencia VARCHAR2(100);
    v_tours NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '=== PRUEBAS DE FUNCIONES ===');
    
    -- Probar fn_obten_total_consumos
    v_consumos := fn_obten_total_consumos(340006);
    DBMS_OUTPUT.PUT_LINE('fn_obten_total_consumos(340006): ' || v_consumos);
    
    -- Probar fn_obten_agencia
    v_agencia := fn_obten_agencia(340006);
    DBMS_OUTPUT.PUT_LINE('fn_obten_agencia(340006): ' || v_agencia);
    
    -- Probar fn_monto_tours
    v_tours := pkg_gestion_cobranza.fn_monto_tours(340006);
    DBMS_OUTPUT.PUT_LINE('pkg_gestion_cobranza.fn_monto_tours(340006): ' || v_tours);
    
    -- Ejecutar el procedimiento principal
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '=== EJECUTANDO PROCEDIMIENTO PRINCIPAL ===');
    sp_calculo_pagos_diarios(
        p_fecha_salida => TO_DATE('18/08/2021', 'DD/MM/YYYY'),
        p_valor_dolar  => 915
    );
END;
/

-- =============================================================================
-- VERIFICACIÓN DE RESULTADOS
-- =============================================================================
SELECT * FROM detalle_diario_huespedes;
SELECT * FROM reg_errores;

SET SERVEROUTPUT ON;

DECLARE
    ------------------------------------------------------------------
    --  Variable para fecha de proceso 
    ------------------------------------------------------------------
    v_fecha_proceso DATE;
    
    ------------------------------------------------------------------
    -- Variables con %TYPE (mínimo 3)
    ------------------------------------------------------------------
    v_id_emp empleado.id_emp%TYPE;
    v_primer_nombre empleado.pnombre_emp%TYPE;
    v_estado_civil estado_civil.nombre_estado_civil%TYPE;
    v_run empleado.numrun_emp%TYPE;
    v_dv_run empleado.dvrun_emp%TYPE;
    v_fecha_contrato empleado.fecha_contrato%TYPE;
    v_sueldo_base empleado.sueldo_base%TYPE;
    v_apellido_paterno empleado.appaterno_emp%TYPE;
    v_fecha_nacimiento empleado.fecha_nac%TYPE;
    v_nombre_completo VARCHAR2(60);

    ------------------------------------------------------------------
    -- Otras variables escalares
    ------------------------------------------------------------------
    v_nombre_usuario VARCHAR2(20);
    v_clave_usuario VARCHAR2(20);
    v_anos_trabajando NUMBER;
    v_contador_iteraciones NUMBER := 0;
    v_total_empleados NUMBER := 0;

    ------------------------------------------------------------------
    -- Variables para calculos
    ------------------------------------------------------------------
    v_primera_letra_estado CHAR(1);
    v_tres_letras_nombre VARCHAR2(3);
    v_largo_nombre NUMBER;
    v_ultimo_digito_sueldo NUMBER;
    v_tercer_digito_run CHAR(1);
    v_ano_nacimiento_mas2 NUMBER;
    v_ultimos_tres_sueldo VARCHAR2(3);
    v_dos_letras_apellido VARCHAR2(2);
    v_mes_ano_db VARCHAR2(6);

    ------------------------------------------------------------------
    -- Cursor para procesar empleados
    ------------------------------------------------------------------
    CURSOR c_empleados IS
        SELECT e.id_emp, e.numrun_emp, e.dvrun_emp,
               e.appaterno_emp, e.apmaterno_emp,
               e.pnombre_emp, e.snombre_emp,
               e.fecha_nac, e.fecha_contrato, e.sueldo_base,
               ec.nombre_estado_civil
        FROM empleado e
        JOIN estado_civil ec
            ON e.id_estado_civil = ec.id_estado_civil
        WHERE e.id_emp BETWEEN 100 AND 320
        ORDER BY e.id_emp;

BEGIN
    ------------------------------------------------------------------
    -- Asignar fecha de proceso     
    ------------------------------------------------------------------
    v_fecha_proceso := SYSDATE;  -- se puede cambiar por fecha específica si se necesita
    
    DBMS_OUTPUT.PUT_LINE('INICIANDO PROCESO DE GENERACIÓN DE USUARIOS');
    DBMS_OUTPUT.PUT_LINE('Fecha de proceso: ' || TO_CHAR(v_fecha_proceso, 'DD/MM/YYYY'));
    
    ------------------------------------------------------------------
    --  Truncar tabla antes de ejecutar
    ------------------------------------------------------------------
    EXECUTE IMMEDIATE 'TRUNCATE TABLE USUARIO_CLAVE';
    DBMS_OUTPUT.PUT_LINE('Tabla USUARIO_CLAVE truncada correctamente.');
    
    ------------------------------------------------------------------
    -- Total de empleados para validación final
    ------------------------------------------------------------------
    SELECT COUNT(*)
    INTO v_total_empleados
    FROM empleado
    WHERE id_emp BETWEEN 100 AND 320;
    
    DBMS_OUTPUT.PUT_LINE('Empleados a procesar (ID 100-320): ' || v_total_empleados);
    DBMS_OUTPUT.PUT_LINE('-----------------------------------------');

    ------------------------------------------------------------------
    -- Procesar empleados con cursor
    ------------------------------------------------------------------
    FOR emp_rec IN c_empleados LOOP

        -- Asignación de variables %TYPE
        v_id_emp := emp_rec.id_emp;
        v_primer_nombre := emp_rec.pnombre_emp;
        v_estado_civil := emp_rec.nombre_estado_civil;
        v_run := emp_rec.numrun_emp;
        v_dv_run := emp_rec.dvrun_emp;
        v_fecha_contrato := emp_rec.fecha_contrato;
        v_sueldo_base := emp_rec.sueldo_base;
        v_apellido_paterno := emp_rec.appaterno_emp;
        v_fecha_nacimiento := emp_rec.fecha_nac;

        ------------------------------------------------------------------
        -- Construcción del nombre completo
        ------------------------------------------------------------------
        v_nombre_completo :=
            TRIM(emp_rec.appaterno_emp) || ' ' ||
            TRIM(emp_rec.apmaterno_emp) || ' ' ||
            TRIM(emp_rec.pnombre_emp) ||
            NVL2(emp_rec.snombre_emp, ' ' || emp_rec.snombre_emp, '');

        ------------------------------------------------------------------
        -- GENERACIÓN NOMBRE DE USUARIO (PL/SQL)
        ------------------------------------------------------------------
        v_primera_letra_estado := LOWER(SUBSTR(v_estado_civil, 1, 1));
        v_tres_letras_nombre   := LOWER(SUBSTR(v_primer_nombre, 1, 3));
        v_largo_nombre         := LENGTH(v_primer_nombre);
        v_ultimo_digito_sueldo := MOD(TRUNC(v_sueldo_base), 10);

        v_anos_trabajando :=
            TRUNC(MONTHS_BETWEEN(v_fecha_proceso, v_fecha_contrato) / 12);

        v_nombre_usuario :=
              v_primera_letra_estado
           || v_tres_letras_nombre
           || v_largo_nombre
           || '*'
           || v_ultimo_digito_sueldo
           || v_dv_run
           || v_anos_trabajando;

        IF v_anos_trabajando < 10 THEN
            v_nombre_usuario := v_nombre_usuario || 'X';
        END IF;
        
        IF LENGTH(v_nombre_usuario) > 20 THEN
            v_nombre_usuario := SUBSTR(v_nombre_usuario, 1, 20);
        END IF;

        ------------------------------------------------------------------
        -- GENERACIÓN CLAVE (PL/SQL)
        ------------------------------------------------------------------
        v_tercer_digito_run := SUBSTR(TO_CHAR(v_run), 3, 1);
        v_ano_nacimiento_mas2 := EXTRACT(YEAR FROM v_fecha_nacimiento) + 2;
        v_ultimos_tres_sueldo :=
            LPAD(MOD(TRUNC(v_sueldo_base) - 1, 1000), 3, '0');

        CASE
            WHEN UPPER(v_estado_civil) IN ('CASADO', 'ACUERDO DE UNION CIVIL') THEN
                v_dos_letras_apellido := LOWER(SUBSTR(v_apellido_paterno, 1, 2));
            WHEN UPPER(v_estado_civil) IN ('DIVORCIADO', 'SOLTERO') THEN
                v_dos_letras_apellido :=
                    LOWER(SUBSTR(v_apellido_paterno, 1, 1) ||
                          SUBSTR(v_apellido_paterno, -1, 1));
            WHEN UPPER(v_estado_civil) = 'VIUDO' THEN
                v_dos_letras_apellido :=
                    LOWER(SUBSTR(v_apellido_paterno, -3, 1) ||
                          SUBSTR(v_apellido_paterno, -2, 1));
            WHEN UPPER(v_estado_civil) = 'SEPARADO' THEN
                v_dos_letras_apellido := LOWER(SUBSTR(v_apellido_paterno, -2, 2));
            ELSE
                v_dos_letras_apellido := 'xx';
        END CASE;

        v_mes_ano_db := TO_CHAR(v_fecha_proceso, 'MMYYYY');

        v_clave_usuario :=
              v_tercer_digito_run
           || v_ano_nacimiento_mas2
           || v_ultimos_tres_sueldo
           || v_dos_letras_apellido
           || v_id_emp
           || v_mes_ano_db;
        
        IF LENGTH(v_clave_usuario) > 20 THEN
            v_clave_usuario := SUBSTR(v_clave_usuario, 1, 20);
        END IF;

        ------------------------------------------------------------------
        -- Inserción en tabla destino
        ------------------------------------------------------------------
        INSERT INTO USUARIO_CLAVE (
            id_emp,
            numrun_emp,
            dvrun_emp,
            nombre_empleado,
            nombre_usuario,
            clave_usuario
        )
        VALUES (
            v_id_emp,
            v_run,
            v_dv_run,
            v_nombre_completo,
            v_nombre_usuario,
            v_clave_usuario
        );

        v_contador_iteraciones := v_contador_iteraciones + 1;

    END LOOP;

    ------------------------------------------------------------------
    -- Confirmación de transacciones
    ------------------------------------------------------------------
    IF v_contador_iteraciones = v_total_empleados THEN
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('==========================================');
        DBMS_OUTPUT.PUT_LINE('PROCESO COMPLETADO EXITOSAMENTE');
        DBMS_OUTPUT.PUT_LINE('Registros procesados: ' || v_contador_iteraciones);
        DBMS_OUTPUT.PUT_LINE('Transacción confirmada ');
        DBMS_OUTPUT.PUT_LINE('==========================================');
    ELSE
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('==========================================');
        DBMS_OUTPUT.PUT_LINE('ERROR: PROCESO INCOMPLETO');
        DBMS_OUTPUT.PUT_LINE('Procesados: ' || v_contador_iteraciones);
        DBMS_OUTPUT.PUT_LINE('Esperados: ' || v_total_empleados);
        DBMS_OUTPUT.PUT_LINE('Transacción revertida ');
        DBMS_OUTPUT.PUT_LINE('==========================================');
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('==========================================');
        DBMS_OUTPUT.PUT_LINE('ERROR EN EL PROCESO');
        DBMS_OUTPUT.PUT_LINE('Código: ' || SQLCODE);
        DBMS_OUTPUT.PUT_LINE('Mensaje: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('Empleado ID: ' || NVL(TO_CHAR(v_id_emp), 'No identificado'));
        DBMS_OUTPUT.PUT_LINE('Transacción revertida ');
        DBMS_OUTPUT.PUT_LINE('==========================================');
END;
/

-- Ver todos los registros generados
SELECT * FROM USUARIO_CLAVE ORDER BY id_emp;

-- Ver con formato resumido
SELECT 
    id_emp,
    numrun_emp || '-' || dvrun_emp AS RUN,
    nombre_empleado,
    nombre_usuario,
    clave_usuario
FROM USUARIO_CLAVE 
ORDER BY id_emp;

# 🚀 Instrucciones para Ejecutar DataComPy

## 📁 Archivos Añadidos al Proyecto

He añadido los siguientes archivos a tu proyecto:

1. **`demo_datacompy.py`** - Demo independiente con ejemplos completos
2. **`GUIA_DATACOMPY.md`** - Guía detallada de uso 
3. **`INSTRUCCIONES_EJECUTAR.md`** - Este archivo con instrucciones paso a paso
4. **Modificaciones en `compare_domo_snowflake.py`** - Función `test_datacompy` integrada
5. **Modificaciones en `requirements.txt`** - Dependencia `datacompy>=0.8.5` añadida

---

## 🔧 Paso 1: Instalación

### Instalar DataComPy
```bash
pip install datacompy
```

### O instalar todas las dependencias
```bash
pip install -r requirements.txt
```

### Verificar instalación
```bash
python -c "import datacompy; print('✅ DataComPy instalado correctamente!')"
```

---

## 🎯 Paso 2: Ejecutar el Demo

### Opción A: Demo independiente (RECOMENDADO para empezar)
```bash
python demo_datacompy.py
```

**¿Qué verás?**
- 📊 Comparación básica con diferencias
- 🔢 Ejemplos de tolerancia numérica  
- 🔑 Múltiples columnas clave
- 📄 Reportes detallados
- 💡 Casos de uso prácticos
- 🔗 Código de integración

### Opción B: Función integrada en el proyecto principal
```bash
python compare_domo_snowflake.py --test-datacompy
```

---

## 📊 Paso 3: Ejemplo de Salida del Demo

Cuando ejecutes el demo, verás algo así:

```
================================================================================
🚀 DEMO COMPLETO DEL PAQUETE DATACOMPY
================================================================================
📖 Este demo muestra cómo usar datacompy para comparar DataFrames
🎯 Ideal para validar migraciones de datos entre sistemas

🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹
📊 EJEMPLO 1: Comparación básica con diferencias
🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹🔹

📋 Datos de origen (Domo):
 producto_id             nombre   precio    categoria  stock activo
           1        Laptop Dell   999.99 Computadoras     50   True
           2     Mouse Logitech    25.50   Accesorios    200   True
           3   Teclado Mecánico    89.99   Accesorios     75   True
           4          Monitor LG   299.99     Monitores     30   True
           5   Auriculares Sony   199.99        Audio    100   True

📋 Datos de destino (Snowflake):
 producto_id                 nombre   precio  categoria  stock activo  descuento
           1            Laptop Dell   999.99   Computadoras     50   True        0.0
           2   Mouse Logitech Pro    25.50     Accesorios    200   True        0.1
           3       Teclado Mecánico    89.99     Accesorios     75   True        0.0
           4              Monitor LG   319.99     Monitores     25   True       0.15
           6             Webcam HD    79.99       Cámaras    150   True       0.05

🔍 Ejecutando comparación con datacompy...

📊 RESULTADOS:
   ✅ ¿Coinciden completamente? False
   📈 Filas en Domo: 5
   📈 Filas en Snowflake: 5
   🔗 Filas en común: 4
   ➖ Solo en Domo: 1
   ➕ Solo en Snowflake: 1
   📝 Columnas solo en Snowflake: descuento
```

---

## 🔗 Paso 4: Usar en Tu Proyecto

### Integración básica en tu código:

```python
import datacompy
import pandas as pd

# Supongamos que ya tienes tus DataFrames
# domo_df = ... (tu código para obtener datos de Domo)
# snowflake_df = ... (tu código para obtener datos de Snowflake)

# Comparar con DataComPy
compare = datacompy.Compare(
    domo_df,
    snowflake_df,
    join_columns=['id'],  # Tus columnas clave
    abs_tol=0.01,         # Tolerancia para decimales
    df1_name='Domo',
    df2_name='Snowflake'
)

# Verificar resultados
if compare.matches():
    print("✅ Los datos coinciden perfectamente!")
else:
    print("❌ Se encontraron diferencias:")
    print(compare.report())
```

### Integración con el comparador existente:

```python
from compare_domo_snowflake import DatasetComparator

# En tu función de comparación, puedes añadir:
def enhanced_comparison_with_datacompy(self, domo_df, snowflake_df, key_columns):
    """
    Comparación mejorada usando DataComPy
    """
    compare = datacompy.Compare(
        domo_df,
        snowflake_df,
        join_columns=key_columns,
        abs_tol=0.01,
        rel_tol=0.001,
        df1_name='Domo',
        df2_name='Snowflake',
        ignore_spaces=True,
        ignore_case=True
    )
    
    return {
        'datacompy_matches': compare.matches(),
        'datacompy_report': compare.report(),
        'missing_in_snowflake': len(compare.df1_unq_rows),
        'extra_in_snowflake': len(compare.df2_unq_rows),
        'unique_columns_domo': compare.df1_unq_columns,
        'unique_columns_snowflake': compare.df2_unq_columns
    }
```

---

## 🛠️ Paso 5: Casos de Uso Específicos

### Para validación de migración completa:
```bash
# 1. Configura tu dataset en compare_domo_snowflake.py
# 2. Ejecuta:
python compare_domo_snowflake.py \
    --domo-dataset-id "tu-dataset-id" \
    --snowflake-table "tu_tabla" \
    --key-columns "id" \
    --transform-columns \
    --interactive
```

### Para tolerancia con datos financieros:
```python
compare = datacompy.Compare(
    domo_df,
    snowflake_df,
    join_columns=['transaction_id'],
    abs_tol=0.01,  # 1 centavo de tolerancia
    rel_tol=0.001, # 0.1% de tolerancia relativa
    df1_name='Domo',
    df2_name='Snowflake'
)
```

---

## ❓ Solución de Problemas

### Si obtienes: "ModuleNotFoundError: No module named 'datacompy'"
```bash
pip install datacompy
```

### Si obtienes errores de memoria con datasets grandes:
```python
# Usar muestreo
sample_size = 10000
domo_sample = domo_df.sample(n=sample_size)
snowflake_sample = snowflake_df.sample(n=sample_size)
```

### Si hay problemas con nombres de columnas:
```python
# Normalizar nombres
domo_df.columns = [col.lower().replace(' ', '_') for col in domo_df.columns]
snowflake_df.columns = [col.lower().replace(' ', '_') for col in snowflake_df.columns]
```

---

## 📋 Checklist de Verificación

- [ ] ✅ Instalé DataComPy (`pip install datacompy`)
- [ ] ✅ Ejecuté el demo independiente (`python demo_datacompy.py`)
- [ ] ✅ Probé la función integrada (`python compare_domo_snowflake.py --test-datacompy`)
- [ ] ✅ Leí la guía completa (`GUIA_DATACOMPY.md`)
- [ ] ✅ Integré DataComPy en mi código de validación
- [ ] ✅ Configuré tolerancias apropiadas para mis datos
- [ ] ✅ Probé con datos reales de mi proyecto

---

## 🎯 Próximos Pasos

1. **Ejecuta el demo ahora**: `python demo_datacompy.py`
2. **Experimenta con tolerancias**: Ajusta `abs_tol` y `rel_tol` según tus datos
3. **Integra en tu flujo**: Añade validaciones DataComPy a tus comparaciones existentes
4. **Automatiza**: Crea scripts que corran DataComPy automáticamente
5. **Monitorea**: Configura alertas basadas en los resultados de DataComPy

---

¡Ahora tienes todo listo para usar DataComPy en tu migración Domo→Snowflake! 🚀

**Comando rápido para empezar:**
```bash
python demo_datacompy.py
``` 
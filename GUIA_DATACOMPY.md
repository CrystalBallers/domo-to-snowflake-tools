# 🚀 Guía Completa: DataComPy en el Proyecto Domo→Snowflake

Esta guía te enseñará cómo usar **DataComPy** para validar tus migraciones de datos entre Domo y Snowflake.

## 📋 Tabla de Contenidos

1. [Instalación](#-instalación)
2. [Demo Rápido](#-demo-rápido)
3. [Uso en el Proyecto Principal](#-uso-en-el-proyecto-principal)
4. [Ejemplos Prácticos](#-ejemplos-prácticos)
5. [Configuraciones Avanzadas](#-configuraciones-avanzadas)

---

## 🔧 Instalación

### Opción 1: Instalar solo DataComPy
```bash
pip install datacompy
```

### Opción 2: Instalar todas las dependencias del proyecto
```bash
pip install -r requirements.txt
```

### Verificar instalación
```bash
python -c "import datacompy; print('✅ DataComPy instalado correctamente!')"
```

---

## 🎯 Demo Rápido

### 1. Ejecutar el demo independiente
```bash
# Ejecutar el demo completo con ejemplos
python demo_datacompy.py
```

### 2. Ejecutar la función de prueba integrada
```bash
# Ejecutar la función test_datacompy del archivo principal
python compare_domo_snowflake.py --test-datacompy
```

**¿Qué verás en el demo?**
- ✅ Comparación de DataFrames con diferencias
- ✅ Ejemplos de tolerancia numérica
- ✅ Uso de múltiples columnas clave
- ✅ Reportes detallados
- ✅ Casos de uso prácticos

---

## 🔗 Uso en el Proyecto Principal

### Integración básica en tu código:

```python
from compare_domo_snowflake import DatasetComparator
import datacompy

# Crear instancia del comparador
comparator = DatasetComparator()

# Obtener datos de Domo y Snowflake
domo_df = # ... tu código para obtener datos de Domo
snowflake_df = # ... tu código para obtener datos de Snowflake

# Comparar con DataComPy
compare = datacompy.Compare(
    domo_df,
    snowflake_df,
    join_columns=['id'],  # Tus columnas clave
    abs_tol=0.01,         # Tolerancia para decimales
    df1_name='Domo',
    df2_name='Snowflake'
)

# Verificar si coinciden
if compare.matches():
    print("✅ Los datos coinciden perfectamente!")
else:
    print("❌ Se encontraron diferencias:")
    print(compare.report())
```

### Función auxiliar recomendada:

```python
def validar_migracion_con_datacompy(domo_df, snowflake_df, key_columns, tolerancia=0.01):
    """
    Valida la migración usando DataComPy
    
    Args:
        domo_df: DataFrame de Domo
        snowflake_df: DataFrame de Snowflake  
        key_columns: Lista de columnas clave
        tolerancia: Tolerancia para valores numéricos
    
    Returns:
        Dict con resultados de la validación
    """
    compare = datacompy.Compare(
        domo_df,
        snowflake_df,
        join_columns=key_columns,
        abs_tol=tolerancia,
        rel_tol=0.001,
        df1_name='Domo',
        df2_name='Snowflake',
        ignore_spaces=True,
        ignore_case=True
    )
    
    return {
        'coinciden': compare.matches(),
        'resumen': compare.report(),
        'filas_solo_en_domo': len(compare.df1_unq_rows),
        'filas_solo_en_snowflake': len(compare.df2_unq_rows),
        'columnas_solo_en_domo': compare.df1_unq_columns,
        'columnas_solo_en_snowflake': compare.df2_unq_columns,
        'estadisticas_columnas': compare.column_stats
    }
```

---

## 💡 Ejemplos Prácticos

### Ejemplo 1: Validación básica de migración
```bash
# 1. Configurar tu dataset en compare_domo_snowflake.py
DEFAULT_CONFIG = {
    'domo_dataset_id': 'tu-dataset-id-aqui',
    'snowflake_table': 'tu_tabla_snowflake',
    'key_columns': ['id', 'fecha'],
    'sample_size': None,
    'interactive': True,
    'transform_column_names': True
}

# 2. Ejecutar comparación principal
python compare_domo_snowflake.py

# 3. Para casos específicos
python compare_domo_snowflake.py \
    --domo-dataset-id "383336aa-ba94-4eb8-be9b-bccc94ffff40" \
    --snowflake-table "int_view_of_upcs_w_categories_v2" \
    --key-columns "id" "loreal_media_categories" \
    --transform-columns
```

### Ejemplo 2: Validación con tolerancia numérica
```python
# Para datos financieros con decimales
compare = datacompy.Compare(
    domo_df,
    snowflake_df,
    join_columns=['transaction_id'],
    abs_tol=0.01,  # Tolerancia de 1 centavo
    rel_tol=0.001, # Tolerancia relativa del 0.1%
    df1_name='Domo',
    df2_name='Snowflake'
)
```

### Ejemplo 3: Múltiples columnas clave
```python
# Para datos con clave compuesta
compare = datacompy.Compare(
    domo_df,
    snowflake_df,
    join_columns=['fecha', 'producto_id', 'region'],
    df1_name='Domo',
    df2_name='Snowflake'
)
```

---

## ⚙️ Configuraciones Avanzadas

### Parámetros principales de DataComPy:

| Parámetro | Descripción | Ejemplo |
|-----------|-------------|---------|
| `join_columns` | Columnas para hacer JOIN | `['id']` o `['fecha', 'producto']` |
| `abs_tol` | Tolerancia absoluta para números | `0.01` (1 centavo) |
| `rel_tol` | Tolerancia relativa (porcentual) | `0.001` (0.1%) |
| `df1_name` / `df2_name` | Nombres para los DataFrames | `'Domo'`, `'Snowflake'` |
| `ignore_spaces` | Ignorar espacios en texto | `True` |
| `ignore_case` | Ignorar mayús/minús | `True` |
| `cast_column_names_lower` | Convertir nombres a minúsculas | `True` |

### Configuración recomendada para migraciones:
```python
compare = datacompy.Compare(
    domo_df,
    snowflake_df,
    join_columns=key_columns,
    abs_tol=0.01,           # Tolerancia de 1 centavo
    rel_tol=0.001,          # 0.1% de tolerancia relativa
    df1_name='Domo',
    df2_name='Snowflake',
    ignore_spaces=True,     # Ignorar espacios extra
    ignore_case=True,       # Ignorar diferencias de mayúsculas
    cast_column_names_lower=True  # Normalizar nombres de columnas
)
```

---

## 🐛 Solución de Problemas

### Error: "ModuleNotFoundError: No module named 'datacompy'"
```bash
# Solución:
pip install datacompy
```

### Error de memoria con datasets grandes
```python
# Usar muestreo para datasets grandes
sample_size = 10000
domo_sample = domo_df.sample(n=sample_size)
snowflake_sample = snowflake_df.sample(n=sample_size)

compare = datacompy.Compare(domo_sample, snowflake_sample, ...)
```

### Problemas con nombres de columnas
```python
# Normalizar nombres antes de la comparación
domo_df.columns = [col.lower().replace(' ', '_') for col in domo_df.columns]
snowflake_df.columns = [col.lower().replace(' ', '_') for col in snowflake_df.columns]
```

---

## 📊 Interpretando los Resultados

### Reporte básico:
```
DataComPy Comparison
--------------------
DataFrame Summary
-----------------
  DataFrame  Columns  Rows
0       Domo        6     5
1  Snowflake        7     5

Column Summary
--------------
Number of columns in common: 6
Number of columns in Domo but not in Snowflake: 0
Number of columns in Snowflake but not in Domo: 1

Row Summary
-----------
Matched on: producto_id
Any duplicates on match values: No
Absolute Tolerance: 0
Relative Tolerance: 0
Number of rows in common: 4
Number of rows in Domo but not in Snowflake: 1
Number of rows in Snowflake but not in Domo: 1

Column Comparison
-----------------
Number of columns compared with some values unequal: 2
Number of columns compared with all values equal: 4
```

### ¿Qué significa cada sección?

- **DataFrame Summary**: Resumen básico de filas y columnas
- **Column Summary**: Diferencias en el esquema (columnas)
- **Row Summary**: Diferencias en los datos (filas)
- **Column Comparison**: Diferencias en valores específicos

---

## 🎯 Casos de Uso Típicos

### ✅ Validación post-migración
```python
# Verificar que la migración fue exitosa
resultado = validar_migracion_con_datacompy(domo_df, snowflake_df, ['id'])
if resultado['coinciden']:
    print("🎉 Migración exitosa!")
else:
    print(f"⚠️ Se encontraron {resultado['filas_solo_en_domo']} filas faltantes")
```

### ✅ Monitoreo continuo
```python
# Configurar alertas automáticas
def validar_y_alertar():
    resultado = validar_migracion_con_datacompy(domo_df, snowflake_df, ['id'])
    if not resultado['coinciden']:
        enviar_alerta(f"Discrepancias encontradas: {resultado['resumen']}")
```

### ✅ Reportes de calidad de datos
```python
# Generar reporte detallado
def generar_reporte_calidad():
    resultado = validar_migracion_con_datacompy(domo_df, snowflake_df, ['id'])
    
    with open('reporte_calidad.txt', 'w') as f:
        f.write(f"Fecha: {datetime.now()}\n")
        f.write(f"Coincidencia: {resultado['coinciden']}\n")
        f.write(f"Resumen detallado:\n{resultado['resumen']}")
```

---

## 🔄 Próximos Pasos

1. **Ejecuta el demo**: `python demo_datacompy.py`
2. **Integra en tu flujo**: Usa las funciones en tu código de migración
3. **Personaliza**: Ajusta tolerancias según tus necesidades
4. **Automatiza**: Crea scripts de validación automática
5. **Monitorea**: Configura alertas para discrepancias

---

¡Ahora tienes todo lo necesario para validar tus migraciones Domo→Snowflake con DataComPy! 🚀 
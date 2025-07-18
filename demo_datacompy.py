#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Demostración independiente del paquete datacompy.

Este archivo demuestra las capacidades de comparación de DataFrames usando datacompy,
ideal para validar la migración de datos entre diferentes sistemas como Domo y Snowflake.
"""

import pandas as pd
import datacompy
import traceback

def imprimir_encabezado(titulo: str, caracter: str = "="):
    """Imprime un encabezado formateado para las secciones del demo."""
    print(f"\n{caracter * 80}")
    print(f" {titulo.upper()} ".center(80, caracter))
    print(f"{caracter * 80}\n")

def demo_datacompy():
    """
    Demostración completa del paquete datacompy con ejemplos prácticos.
    """
    imprimir_encabezado("🚀 DEMO COMPLETO DEL PAQUETE DATACOMPY 🚀")
    print("📖 Este demo muestra cómo usar datacompy para comparar DataFrames.")
    print("🎯 Ideal para validar migraciones de datos y asegurar la integridad de la información.")
    
    # ===========================================================================
    # EJEMPLO 1: Comparación básica con diferencias
    # ===========================================================================
    imprimir_encabezado("📊 EJEMPLO 1: Comparación básica con diferencias", "🔹")
    
    # Crear datos de ejemplo (simulando origen, ej: Domo)
    df_origen = pd.DataFrame({
        'producto_id': [1, 2, 3, 4, 5],
        'nombre': ['Laptop Dell', 'Mouse Logitech', 'Teclado Mecánico', 'Monitor LG', 'Auriculares Sony'],
        'precio': [999.99, 25.50, 89.99, 299.99, 199.99],
        'categoria': ['Computadoras', 'Accesorios', 'Accesorios', 'Monitores', 'Audio'],
        'stock': [50, 200, 75, 30, 100],
        'activo': [True, True, True, True, True]
    })
    
    # Crear datos modificados (simulando destino, ej: Snowflake)
    df_destino = pd.DataFrame({
        'producto_id': [1, 2, 3, 4, 6],  # ID 5 eliminado, ID 6 agregado
        'nombre': ['Laptop Dell', 'Mouse Logitech Pro', 'Teclado Mecánico', 'Monitor LG', 'Webcam HD'],  # Cambio en nombre
        'precio': [999.99, 25.50, 89.99, 319.99, 79.99],  # Precio del monitor cambiado
        'categoria': ['Computadoras', 'Accesorios', 'Accesorios', 'Monitores', 'Cámaras'],
        'stock': [50, 200, 75, 25, 150],  # Stock del monitor cambiado
        'activo': [True, True, True, True, True],
        'descuento': [0.0, 0.1, 0.0, 0.15, 0.05]  # Nueva columna en destino
    })
    
    print("📋 Datos de Origen (Domo):")
    print(df_origen.to_string(index=False))
    
    print("\n📋 Datos de Destino (Snowflake):")
    print(df_destino.to_string(index=False))
    
    # Realizar comparación
    print("\n🔍 Ejecutando comparación con datacompy...")
    compare1 = datacompy.Compare(
        df_origen,
        df_destino,
        join_columns='producto_id',  # Columna clave para unir los dataframes
        df1_name='Domo',             # Nombre para el reporte
        df2_name='Snowflake'         # Nombre para el reporte
    )
    
    print("\n📊 RESUMEN DE LA COMPARACIÓN:")
    print(f"   - ¿Coinciden perfectamente? {'✅ Sí' if compare1.matches() else '❌ No'}")
    print(f"   - Filas totales en Origen (Domo): {len(df_origen)}")
    print(f"   - Filas totales en Destino (Snowflake): {len(df_destino)}")
    print(f"   - Filas en común (basado en 'producto_id'): {len(compare1.intersect_rows)}")
    print(f"   - Filas que solo existen en Origen: {len(compare1.df1_unq_rows)}")
    print(f"   - Filas que solo existen en Destino: {len(compare1.df2_unq_rows)}")
    
    # Mostrar columnas únicas si existen (manejo de error específico)
    try:
        if compare1.df1_unq_columns():
            print(f"   - Columnas que solo existen en Origen: {', '.join(compare1.df1_unq_columns())}")
        if compare1.df2_unq_columns():
            print(f"   - Columnas que solo existen en Destino: {', '.join(compare1.df2_unq_columns())}")
    except AttributeError:
        pass  # Versiones más antiguas de datacompy podrían no tener estos métodos
    
    # ===========================================================================
    # EJEMPLO 2: Tolerancia numérica
    # ===========================================================================
    imprimir_encabezado("🔢 EJEMPLO 2: Tolerancia numérica para valores decimales", "🔹")
    
    df_num_original = pd.DataFrame({'id': [1, 2, 3], 'valor': [100.00, 250.50, 75.25]})
    df_num_modificado = pd.DataFrame({'id': [1, 2, 3], 'valor': [100.01, 250.49, 75.26]})
    
    print("📋 Datos Originales:\n", df_num_original.to_string(index=False))
    print("\n📋 Datos con Pequeñas Diferencias:\n", df_num_modificado.to_string(index=False))
    
    print("\n🚫 Comparación SIN tolerancia:")
    compare_sin_tol = datacompy.Compare(df_num_original, df_num_modificado, join_columns='id')
    print(f"   Resultado: {'✅ Coinciden' if compare_sin_tol.matches() else '❌ No coinciden'}")
    
    print("\n✅ Comparación CON tolerancia (abs_tol=0.1):")
    compare_con_tol = datacompy.Compare(
        df_num_original, df_num_modificado, join_columns='id', abs_tol=0.1
    )
    print(f"   Resultado: {'✅ Coinciden' if compare_con_tol.matches() else '❌ No coinciden'}")

    # ===========================================================================
    # EJEMPLO 3: Múltiples columnas clave
    # ===========================================================================
    imprimir_encabezado("🔑 EJEMPLO 3: Comparación con múltiples columnas clave", "🔹")

    df_ventas_orig = pd.DataFrame({
        'fecha': ['2024-01-01', '2024-01-01', '2024-01-02'],
        'producto': ['A', 'B', 'A'],
        'ventas': [100, 150, 120]
    })
    df_ventas_dest = pd.DataFrame({
        'fecha': ['2024-01-01', '2024-01-01', '2024-01-02'],
        'producto': ['A', 'B', 'A'],
        'ventas': [100, 155, 120]  # Cambio en una venta
    })

    print("📋 Datos de Ventas Originales:\n", df_ventas_orig.to_string(index=False))
    print("\n📋 Datos de Ventas Modificados:\n", df_ventas_dest.to_string(index=False))

    print("\n🔍 Comparación usando clave compuesta ['fecha', 'producto']:")
    compare_ventas = datacompy.Compare(
        df_ventas_orig, df_ventas_dest, join_columns=['fecha', 'producto']
    )
    
    print(f"   ¿Coinciden? {'✅ Sí' if compare_ventas.matches() else '❌ No'}")
    if not compare_ventas.matches():
        print("   Resumen de columnas con diferencias:")
        print(compare_ventas.column_stats)

    # ===========================================================================
    # EJEMPLO 4: Reporte detallado y guardado en archivo
    # ===========================================================================
    imprimir_encabezado("📄 EJEMPLO 4: Reporte detallado completo", "🔹")
    
    print("📊 Generando reporte detallado del primer ejemplo...")
    reporte_texto = compare1.report()
    
    print("\n--- INICIO DEL REPORTE ---\n")
    print(reporte_texto)
    print("--- FIN DEL REPORTE ---\n")
    
    # Guardar el reporte en un archivo
    try:
        with open("reporte_comparacion.txt", "w", encoding="utf-8") as f:
            f.write(reporte_texto)
        print("✅ Reporte guardado exitosamente en 'reporte_comparacion.txt'")
    except IOError as e:
        print(f"❌ Error al guardar el archivo: {e}")

    # ===========================================================================
    # CASOS DE USO Y CONFIGURACIONES
    # ===========================================================================
    imprimir_encabezado("💡 Casos de Uso y Configuraciones Avanzadas", "🔹")
    
    print("Casos de Uso Prácticos para Migraciones:")
    print("- ✅ Validar que todos los registros se migraron correctamente.")
    print("- ✅ Identificar diferencias en valores después de transformaciones (ETL).")
    print("- ✅ Verificar integridad con tolerancia para campos numéricos y de punto flotante.")
    print("- ✅ Detectar registros duplicados o faltantes en el destino.")
    print("- ✅ Comparar esquemas (columnas adicionales o faltantes).")
    
    print("\nConfiguraciones Avanzadas Comunes:")
    print("- 🔧 `abs_tol`: Tolerancia absoluta para números (ej: 0.01).")
    print("- 🔧 `rel_tol`: Tolerancia relativa para números (ej: 0.001 para 0.1%).")
    print("- 🔧 `ignore_spaces`: Ignorar espacios en blanco al inicio y final en strings.")
    print("- 🔧 `ignore_case`: Ignorar mayúsculas/minúsculas en strings.")
    print("- 🔧 `cast_column_names_lower`: Convertir nombres de columnas a minúsculas antes de comparar.")

    imprimir_encabezado("✅ DEMO COMPLETADO EXITOSAMENTE ✅")
    print("💡 ¡Ahora puedes usar datacompy en tu proyecto de migración Domo -> Snowflake!")


def ejemplo_integracion_con_proyecto():
    """
    Ejemplo de cómo se podría integrar datacompy en una función reutilizable
    dentro de un proyecto más grande.
    """
    imprimir_encabezado("🔗 EJEMPLO DE INTEGRACIÓN EN UN PROYECTO REAL", "🔹")
    
    codigo_ejemplo = '''
# En un módulo de utilidades de tu proyecto (ej: utils/comparator.py)
import pandas as pd
import datacompy

def run_comparison(df_source: pd.DataFrame, df_target: pd.DataFrame, 
                   join_keys: list[str], df1_name: str, df2_name: str) -> dict:
    """
    Función encapsulada para ejecutar una comparación con datacompy y devolver un resumen.

    Args:
        df_source: DataFrame de origen.
        df_target: DataFrame de destino.
        join_keys: Lista de columnas clave para la unión.
        df1_name: Nombre del DataFrame de origen.
        df2_name: Nombre del DataFrame de destino.

    Returns:
        Un diccionario con los resultados de la comparación.
    """
    print(f"Iniciando comparación entre '{df1_name}' y '{df2_name}'...")
    
    compare = datacompy.Compare(
        df_source,
        df_target,
        join_columns=join_keys,
        abs_tol=0.001,           # Tolerancia para decimales
        rel_tol=0.0,             # Sin tolerancia relativa
        df1_name=df1_name,
        df2_name=df2_name,
        ignore_spaces=True,      # Ignorar espacios en blanco
        ignore_case=True         # Ignorar mayúsculas/minúsculas
    )
    
    # Retornar un diccionario con la información clave
    return {
        'are_matching': compare.matches(),
        'report': compare.report(),
        'rows_missing_in_target': len(compare.df1_unq_rows),
        'rows_extra_in_target': len(compare.df2_unq_rows),
        'columns_only_in_source': compare.df1_unq_columns(),
        'columns_only_in_target': compare.df2_unq_columns()
    }
'''
    
    print("📝 Código de ejemplo para una función de integración reutilizable:")
    print(codigo_ejemplo)


if __name__ == "__main__":
    try:
        demo_datacompy()
        ejemplo_integracion_con_proyecto()
        
    except ImportError:
        print("\n❌ Error de importación: El paquete 'datacompy' no está instalado.")
        print("\n💡 Para resolver este error, ejecuta uno de los siguientes comandos:")
        print("   1. pip install datacompy")
        print("   2. Si tienes un archivo de requerimientos: pip install -r requirements.txt")
        
    except Exception as e:
        print(f"\n❌ Ha ocurrido un error inesperado: {e}")
        print(f"   Tipo de error: {type(e).__name__}")
        print("\n--- Traceback ---")
        print(traceback.format_exc())
        print("-----------------")
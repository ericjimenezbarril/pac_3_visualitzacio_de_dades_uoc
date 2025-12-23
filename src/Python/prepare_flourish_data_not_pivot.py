#!/usr/bin/env python3
"""
Script per preparar dades per Flourish.studio (format LLARG, sense pivotar)
Genera múltiples CSVs amb Status com a files en lloc de columnes
"""

import pandas as pd
import numpy as np
from pathlib import Path

# Configuració
INPUT_FILE = "../../output/hotel_booking_final.csv"
OUTPUT_DIR = Path("flourish_csv_not_pivot")
OUTPUT_DIR.mkdir(exist_ok=True)

print("Carregant dades...")
df = pd.read_csv(INPUT_FILE)

# Neteja bàsica: assegurar que is_canceled és numèric
df['is_canceled'] = pd.to_numeric(df['is_canceled'], errors='coerce')
df['is_canceled'] = df['is_canceled'].fillna(0).astype(int)

# Crear columna Status per a les visualitzacions
df['Status'] = df['is_canceled'].map({0: 'Not canceled', 1: 'Canceled'})

print(f"Dades carregades: {len(df)} files, {len(df.columns)} columnes")
print(f"Taxa de cancel·lació global: {df['is_canceled'].mean():.2%}")

# ============================================================================
# FUNCIÓ PER GENERAR CSVs DE CANCEL·LACIONS (FORMAT LLARG)
# ============================================================================

def generate_cancellation_csv_long(df, groupby_cols, output_name):
    """
    Genera un CSV amb cancel·lacions en format llarg (Status com a files)
    
    Args:
        df: DataFrame
        groupby_cols: llista de columnes per agrupar (pot ser buida)
        output_name: nom del fitxer de sortida
    """
    if not groupby_cols:
        # Cas bàsic: només Status i count
        result = df.groupby('Status', observed=True).size().reset_index(name='count')
        # Calcular percentatge respecte al total
        total = result['count'].sum()
        result['percentage'] = (result['count'] / total * 100).round(2)
        result = result[['Status', 'count', 'percentage']]
        result = result.sort_values('Status')
    else:
        # Agrupar per Status i les columnes especificades
        cols = ['Status'] + groupby_cols
        grouped = df.groupby(cols, observed=True).size().reset_index(name='count')
        
        # Calcular totals per cada grup (sense Status) per calcular percentatges
        totals = df.groupby(groupby_cols, observed=True).size().reset_index(name='Total')
        grouped = grouped.merge(totals, on=groupby_cols, how='left')
        grouped['percentage'] = (grouped['count'] / grouped['Total'] * 100).round(2)
        grouped = grouped.drop(columns=['Total'])
        
        # Reordenar columnes: groupby_cols, Status, count, percentage
        result = grouped[groupby_cols + ['Status', 'count', 'percentage']]
        
        # Si 'origin' està a les columnes d'agrupació, afegir files "Total"
        if 'origin' in groupby_cols:
            # Agrupar per totes les columnes excepte 'origin' i 'Status' per calcular Total
            other_cols = [col for col in groupby_cols if col != 'origin']
            
            # Per cada combinació d'altres columnes i Status, sumar els counts
            if other_cols:
                total_grouped = df.groupby(other_cols + ['Status'], observed=True).size().reset_index(name='count')
                # Calcular totals per cada grup (sense Status) per recalcular percentatges
                totals = df.groupby(other_cols, observed=True).size().reset_index(name='Total')
                total_grouped = total_grouped.merge(totals, on=other_cols, how='left')
                total_grouped['percentage'] = (total_grouped['count'] / total_grouped['Total'] * 100).round(2)
                total_grouped = total_grouped.drop(columns=['Total'])
            else:
                # Si només hi ha 'origin', sumar tot per Status
                total_grouped = df.groupby('Status', observed=True).size().reset_index(name='count')
                total = total_grouped['count'].sum()
                total_grouped['percentage'] = (total_grouped['count'] / total * 100).round(2)
            
            # Afegir columna origin='Total'
            total_grouped['origin'] = 'Total'
            
            # Reordenar columnes per coincidir amb result
            total_grouped = total_grouped[groupby_cols + ['Status', 'count', 'percentage']]
            
            # Afegir al resultat
            result = pd.concat([result, total_grouped], ignore_index=True)
        
        # Ordenar per les columnes d'agrupació i després per Status (Total al final si existeix)
        if 'origin' in groupby_cols:
            # Crear una columna temporal per ordenar (Total al final)
            result['_sort_origin'] = result['origin'].apply(lambda x: 1 if x == 'Total' else 0)
            sort_cols = ['_sort_origin'] + [col for col in groupby_cols if col != 'origin'] + ['origin', 'Status']
            result = result.sort_values(sort_cols)
            result = result.drop(columns=['_sort_origin'])
        else:
            result = result.sort_values(groupby_cols + ['Status'])
    
    # Mapping de noms de columnes
    column_mapping = {
        'country': 'Country',
        'hotel': 'Hotel',
        'tipo': 'Trip Type',
        'origin': 'Origin',
        'Status': 'Status',
        'count': 'Count',
        'percentage': 'Percentage'
    }
    result = result.rename(columns=column_mapping)
    
    output_path = OUTPUT_DIR / f"{output_name}.csv"
    result.to_csv(output_path, index=False)
    print(f"  ✓ Generat: {output_name}.csv ({len(result)} files)")
    return result

# ============================================================================
# GENERAR TOTS ELS CSVs DE CANCEL·LACIONS (FORMAT LLARG)
# ============================================================================

print("\n" + "="*70)
print("GENERANT CSVs DE CANCEL·LACIONS (FORMAT LLARG)")
print("="*70)

# 1. Bàsic: Status, count
generate_cancellation_csv_long(df, [], "01_cancellation_basic")

# 2. Per hotel
generate_cancellation_csv_long(df, ['hotel'], "02_cancellation_by_hotel")

# 3. Per origen
generate_cancellation_csv_long(df, ['origin'], "03_cancellation_by_origin")

# 4. Per país (només països especificats: PRT, GBR, FRA, ESP, DEU)
top_countries_list = ['PRT', 'GBR', 'FRA', 'ESP', 'DEU']
df_country_filtered = df[df['country'].isin(top_countries_list)]
generate_cancellation_csv_long(df_country_filtered, ['country'], "04_cancellation_by_country")

# 5. Per first_time_visitor
generate_cancellation_csv_long(df, ['first_time_visitor'], "05_cancellation_by_first_time_visitor")

# 6. Per previous_cancellations_group
generate_cancellation_csv_long(df, ['previous_cancellations_group'], "06_cancellation_by_previous_cancellations")

# 7. Per tipo
generate_cancellation_csv_long(df, ['tipo'], "07_cancellation_by_tipo")

# 8. Combinacions de 2 variables
print("\nGenerant combinacions de 2 variables...")

# hotel + origin
generate_cancellation_csv_long(df, ['hotel', 'origin'], "08_cancellation_by_hotel_origin")

# hotel + country (filtrat)
generate_cancellation_csv_long(df_country_filtered, ['hotel', 'country'], "09_cancellation_by_hotel_country")

# origin + country (filtrat)
generate_cancellation_csv_long(df_country_filtered, ['origin', 'country'], "10_cancellation_by_origin_country")

# hotel + first_time_visitor
generate_cancellation_csv_long(df, ['hotel', 'first_time_visitor'], "11_cancellation_by_hotel_first_time_visitor")

# origin + first_time_visitor
generate_cancellation_csv_long(df, ['origin', 'first_time_visitor'], "12_cancellation_by_origin_first_time_visitor")

# hotel + previous_cancellations_group
generate_cancellation_csv_long(df, ['hotel', 'previous_cancellations_group'], "13_cancellation_by_hotel_previous_cancellations")

# origin + previous_cancellations_group
generate_cancellation_csv_long(df, ['origin', 'previous_cancellations_group'], "14_cancellation_by_origin_previous_cancellations")

# hotel + tipo
generate_cancellation_csv_long(df, ['hotel', 'tipo'], "15_cancellation_by_hotel_tipo")

# origin + tipo
generate_cancellation_csv_long(df, ['origin', 'tipo'], "16_cancellation_by_origin_tipo")

# first_time_visitor + previous_cancellations_group
generate_cancellation_csv_long(df, ['first_time_visitor', 'previous_cancellations_group'], "17_cancellation_by_first_time_previous_cancellations")

# 9. Combinacions de 3 variables
print("\nGenerant combinacions de 3 variables...")

# hotel + origin + tipo
generate_cancellation_csv_long(df, ['hotel', 'origin', 'tipo'], "18_cancellation_by_hotel_origin_tipo")

# hotel + country + tipo (filtrat)
generate_cancellation_csv_long(df_country_filtered, ['hotel', 'country', 'tipo'], "18b_cancellation_by_hotel_country_tipo")

# hotel + origin + first_time_visitor
generate_cancellation_csv_long(df, ['hotel', 'origin', 'first_time_visitor'], "19_cancellation_by_hotel_origin_first_time_visitor")

# hotel + origin + previous_cancellations_group
generate_cancellation_csv_long(df, ['hotel', 'origin', 'previous_cancellations_group'], "20_cancellation_by_hotel_origin_previous_cancellations")

# hotel + origin + country (filtrat)
generate_cancellation_csv_long(df_country_filtered, ['hotel', 'origin', 'country'], "21_cancellation_by_hotel_origin_country")

# hotel + tipo + previous_cancellations_group
generate_cancellation_csv_long(df, ['hotel', 'tipo', 'previous_cancellations_group'], "22_cancellation_by_hotel_tipo_previous_cancellations")

# origin + tipo + previous_cancellations_group
generate_cancellation_csv_long(df, ['origin', 'tipo', 'previous_cancellations_group'], "23_cancellation_by_origin_tipo_previous_cancellations")

# 10. Combinacions de 4 variables
print("\nGenerant combinacions de 4 variables...")

# hotel + origin + tipo + previous_cancellations_group
generate_cancellation_csv_long(df, ['hotel', 'origin', 'tipo', 'previous_cancellations_group'], "24_cancellation_by_hotel_origin_tipo_previous_cancellations")

# ============================================================================
# RESUM FINAL
# ============================================================================

print("\n" + "="*70)
print("RESUM")
print("="*70)
print(f"✓ Tots els CSVs generats a: {OUTPUT_DIR.absolute()}")
print(f"✓ Total de fitxers generats: {len(list(OUTPUT_DIR.glob('*.csv')))}")
print("\nFitxers generats:")
for csv_file in sorted(OUTPUT_DIR.glob('*.csv')):
    size_kb = csv_file.stat().st_size / 1024
    print(f"  - {csv_file.name} ({size_kb:.1f} KB)")

print("\n" + "="*70)
print("COMPLETAT!")
print("="*70)


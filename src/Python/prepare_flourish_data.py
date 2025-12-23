#!/usr/bin/env python3
"""
Script per preparar dades per Flourish.studio
Genera múltiples CSVs amb diferents combinacions de variables per visualitzacions
"""

import pandas as pd
import numpy as np
from pathlib import Path
from itertools import combinations

# Configuració
INPUT_FILE = "../../output/hotel_booking_final.csv"
OUTPUT_DIR = Path("flourish_csv")
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
# FUNCIÓ PER GENERAR CSVs DE CANCEL·LACIONS
# ============================================================================

def generate_cancellation_csv(df, groupby_cols, output_name):
    """
    Genera un CSV amb cancel·lacions agrupades per les columnes especificades
    Amb Status com a columnes (Canceled_value, Not_canceled_value, etc.)
    
    Args:
        df: DataFrame
        groupby_cols: llista de columnes per agrupar (pot ser buida)
        output_name: nom del fitxer de sortida
    """
    if not groupby_cols:
        # Cas bàsic: només Status i Value (format original amb files)
        result = df.groupby('Status', observed=True).size().reset_index(name='Value')
        # Calcular percentatge respecte al total
        total = result['Value'].sum()
        result['Percentage'] = (result['Value'] / total * 100).round(2)
        result = result[['Status', 'Value', 'Percentage']]
        result = result.sort_values('Status')
    else:
        # Agrupar per Status i les columnes especificades
        cols = ['Status'] + groupby_cols
        grouped = df.groupby(cols, observed=True).size().reset_index(name='Value')
        
        # Calcular totals per cada grup (sense Status)
        totals = df.groupby(groupby_cols, observed=True).size().reset_index(name='Total')
        grouped = grouped.merge(totals, on=groupby_cols, how='left')
        grouped['Percentage'] = (grouped['Value'] / grouped['Total'] * 100).round(2)
        grouped = grouped.drop(columns=['Total'])
        
        # Pivotar: Status com a columnes
        result = grouped.pivot_table(
            index=groupby_cols,
            columns='Status',
            values=['Value', 'Percentage'],
            aggfunc='first',
            fill_value=0
        )
        
        # Aplanar noms de columnes multi-index (només les columnes pivotades, no les d'agrupació)
        new_columns = []
        for col in result.columns:
            if isinstance(col, tuple):
                # Columna pivotada (Value o Percentage)
                status_name = col[1].replace(' ', '_')  # "Not canceled" -> "Not_canceled"
                # Per Value: minúscula, per Percentage: capitalitzar
                if col[0] == 'Value':
                    metric = 'value'  # Minúscula per consistència
                else:  # Percentage
                    metric = 'Percentage'  # Capitalitzat
                new_columns.append(f"{status_name}_{metric}")
            else:
                # Columna normal (no hauria de passar després del pivot, però per seguretat)
                new_columns.append(col)
        
        result.columns = new_columns
        result = result.reset_index()  # Ara sí, reset_index() per tenir les columnes d'agrupació com a columnes normals
        
        # Reordenar columnes: groupby_cols, Canceled_value, Not_canceled_value, Canceled_Percentage, Not_canceled_Percentage
        cols_order = groupby_cols + [
            'Canceled_value', 'Not_canceled_value', 
            'Canceled_Percentage', 'Not_canceled_Percentage'
        ]
        # Només agafar columnes que existeixen
        result = result[[col for col in cols_order if col in result.columns]]
        
        # Si 'origin' està a les columnes d'agrupació, afegir fila "Total"
        if 'origin' in groupby_cols:
            # Agrupar per totes les columnes excepte 'origin' per calcular Total
            other_cols = [col for col in groupby_cols if col != 'origin']
            
            if other_cols:
                # Agrupar per les altres columnes i sumar
                total_rows = result.groupby(other_cols, observed=True)[
                    ['Canceled_value', 'Not_canceled_value']
                ].sum().reset_index()
            else:
                # Si només hi ha 'origin', sumar tot
                total_rows = pd.DataFrame({
                    'Canceled_value': [result['Canceled_value'].sum()],
                    'Not_canceled_value': [result['Not_canceled_value'].sum()]
                })
            
            # Recalcular percentatges per Total (no mitjana, sinó del total real)
            total_rows['total_bookings'] = total_rows['Canceled_value'] + total_rows['Not_canceled_value']
            total_rows['Canceled_Percentage'] = (total_rows['Canceled_value'] / total_rows['total_bookings'] * 100).round(2)
            total_rows['Not_canceled_Percentage'] = (total_rows['Not_canceled_value'] / total_rows['total_bookings'] * 100).round(2)
            total_rows = total_rows.drop(columns=['total_bookings'])
            
            # Afegir columna origin='Total'
            total_rows['origin'] = 'Total'
            
            # Reordenar columnes per coincidir amb result
            total_rows = total_rows[groupby_cols + [col for col in cols_order if col not in groupby_cols and col in total_rows.columns]]
            
            # Afegir al resultat
            result = pd.concat([result, total_rows], ignore_index=True)
        
        # Ordenar per les columnes d'agrupació (Total al final si existeix)
        if 'origin' in groupby_cols:
            # Crear una columna temporal per ordenar (Total al final)
            result['_sort_origin'] = result['origin'].apply(lambda x: 1 if x == 'Total' else 0)
            sort_cols = ['_sort_origin'] + [col for col in groupby_cols if col != 'origin'] + ['origin']
            result = result.sort_values(sort_cols)
            result = result.drop(columns=['_sort_origin'])
        else:
            result = result.sort_values(groupby_cols)
    
    # Mapping de noms de columnes
    column_mapping = {
        'country': 'Country',
        'hotel': 'Hotel',
        'tipo': 'Trip Type',
        'origin': 'Origin',
        'Canceled_value': 'Canceled',
        'Not_canceled_value': 'Not Canceled',
        'Canceled_Percentage': 'Canceled %',
        'Not_canceled_Percentage': 'Not Canceled %'
    }
    result = result.rename(columns=column_mapping)
    
    output_path = OUTPUT_DIR / f"{output_name}.csv"
    result.to_csv(output_path, index=False)
    print(f"  ✓ Generat: {output_name}.csv ({len(result)} files)")
    return result

# ============================================================================
# GENERAR TOTS ELS CSVs DE CANCEL·LACIONS
# ============================================================================

print("\n" + "="*70)
print("GENERANT CSVs DE CANCEL·LACIONS")
print("="*70)

# 1. Bàsic: Status, Value
generate_cancellation_csv(df, [], "01_cancellation_basic")

# 2. Per hotel
generate_cancellation_csv(df, ['hotel'], "02_cancellation_by_hotel")

# 3. Per origen
generate_cancellation_csv(df, ['origin'], "03_cancellation_by_origin")

# 4. Per país (només països especificats: PRT, GBR, FRA, ESP, DEU)
top_countries_list = ['PRT', 'GBR', 'FRA', 'ESP', 'DEU']
df_country_filtered = df[df['country'].isin(top_countries_list)]
generate_cancellation_csv(df_country_filtered, ['country'], "04_cancellation_by_country")

# 5. Per first_time_visitor
generate_cancellation_csv(df, ['first_time_visitor'], "05_cancellation_by_first_time_visitor")

# 6. Per previous_cancellations_group
generate_cancellation_csv(df, ['previous_cancellations_group'], "06_cancellation_by_previous_cancellations")

# 7. Per tipo
generate_cancellation_csv(df, ['tipo'], "07_cancellation_by_tipo")

# 8. Combinacions de 2 variables
print("\nGenerant combinacions de 2 variables...")

# hotel + origin
generate_cancellation_csv(df, ['hotel', 'origin'], "08_cancellation_by_hotel_origin")

# hotel + country (filtrat)
generate_cancellation_csv(df_country_filtered, ['hotel', 'country'], "09_cancellation_by_hotel_country")

# origin + country (filtrat)
generate_cancellation_csv(df_country_filtered, ['origin', 'country'], "10_cancellation_by_origin_country")

# hotel + first_time_visitor
generate_cancellation_csv(df, ['hotel', 'first_time_visitor'], "11_cancellation_by_hotel_first_time_visitor")

# origin + first_time_visitor
generate_cancellation_csv(df, ['origin', 'first_time_visitor'], "12_cancellation_by_origin_first_time_visitor")

# hotel + previous_cancellations_group
generate_cancellation_csv(df, ['hotel', 'previous_cancellations_group'], "13_cancellation_by_hotel_previous_cancellations")

# origin + previous_cancellations_group
generate_cancellation_csv(df, ['origin', 'previous_cancellations_group'], "14_cancellation_by_origin_previous_cancellations")

# hotel + tipo
generate_cancellation_csv(df, ['hotel', 'tipo'], "15_cancellation_by_hotel_tipo")

# origin + tipo
generate_cancellation_csv(df, ['origin', 'tipo'], "16_cancellation_by_origin_tipo")

# first_time_visitor + previous_cancellations_group
generate_cancellation_csv(df, ['first_time_visitor', 'previous_cancellations_group'], "17_cancellation_by_first_time_previous_cancellations")

# 9. Combinacions de 3 variables
print("\nGenerant combinacions de 3 variables...")

# hotel + origin + tipo
generate_cancellation_csv(df, ['hotel', 'origin', 'tipo'], "18_cancellation_by_hotel_origin_tipo")

# hotel + country + tipo (filtrat)
generate_cancellation_csv(df_country_filtered, ['hotel', 'country', 'tipo'], "18b_cancellation_by_hotel_country_tipo")

# hotel + origin + first_time_visitor
generate_cancellation_csv(df, ['hotel', 'origin', 'first_time_visitor'], "19_cancellation_by_hotel_origin_first_time_visitor")

# hotel + origin + previous_cancellations_group
generate_cancellation_csv(df, ['hotel', 'origin', 'previous_cancellations_group'], "20_cancellation_by_hotel_origin_previous_cancellations")

# hotel + origin + country (filtrat)
generate_cancellation_csv(df_country_filtered, ['hotel', 'origin', 'country'], "21_cancellation_by_hotel_origin_country")

# hotel + tipo + previous_cancellations_group
generate_cancellation_csv(df, ['hotel', 'tipo', 'previous_cancellations_group'], "22_cancellation_by_hotel_tipo_previous_cancellations")

# origin + tipo + previous_cancellations_group
generate_cancellation_csv(df, ['origin', 'tipo', 'previous_cancellations_group'], "23_cancellation_by_origin_tipo_previous_cancellations")

# 10. Combinacions de 4 variables
print("\nGenerant combinacions de 4 variables...")

# hotel + origin + tipo + previous_cancellations_group
generate_cancellation_csv(df, ['hotel', 'origin', 'tipo', 'previous_cancellations_group'], "24_cancellation_by_hotel_origin_tipo_previous_cancellations")

# ============================================================================
# CSV ESPECIAL: TEMPORAL PER COUNTRY I ORIGIN
# ============================================================================

print("\n" + "="*70)
print("GENERANT CSV TEMPORAL (COUNTRY + ORIGIN + DATES)")
print("="*70)

# Assegurar que 'dia' és datetime
df['dia'] = pd.to_datetime(df['dia'], errors='coerce')

# Filtrar dates vàlides
df_temporal = df.dropna(subset=['dia']).copy()

# Agrupar per country, origin i dia
temporal_data = df_temporal.groupby(['country', 'origin', 'dia'], observed=True).size().reset_index(name='bookings')

# Pivotar: cada data com a columna
temporal_pivot = temporal_data.pivot_table(
    index=['country', 'origin'],
    columns='dia',
    values='bookings',
    fill_value=0
).reset_index()

# Ordenar columnes: primer country i origin, després les dates ordenades
date_cols = sorted([col for col in temporal_pivot.columns if col not in ['country', 'origin']])
temporal_pivot = temporal_pivot[['country', 'origin'] + date_cols]

# Guardar
output_path = OUTPUT_DIR / "25_temporal_bookings_by_country_origin.csv"
temporal_pivot.to_csv(output_path, index=False)
print(f"  ✓ Generat: 25_temporal_bookings_by_country_origin.csv")
print(f"    Dimensions: {temporal_pivot.shape[0]} files (combinacions country+origin)")
print(f"    Període: {date_cols[0]} a {date_cols[-1]} ({len(date_cols)} dates)")

# ============================================================================
# CSV TEMPORAL SIMPLIFICAT: Només top countries
# ============================================================================

print("\nGenerant versió simplificada amb top countries...")

# Top 10 països per nombre de reserves
top_countries = df['country'].value_counts().head(10).index.tolist()
df_top_countries = df_temporal[df_temporal['country'].isin(top_countries)].copy()

temporal_data_top = df_top_countries.groupby(['country', 'origin', 'dia'], observed=True).size().reset_index(name='bookings')

temporal_pivot_top = temporal_data_top.pivot_table(
    index=['country', 'origin'],
    columns='dia',
    values='bookings',
    fill_value=0
).reset_index()

date_cols_top = sorted([col for col in temporal_pivot_top.columns if col not in ['country', 'origin']])
temporal_pivot_top = temporal_pivot_top[['country', 'origin'] + date_cols_top]

output_path_top = OUTPUT_DIR / "26_temporal_bookings_top10_countries.csv"
temporal_pivot_top.to_csv(output_path_top, index=False)
print(f"  ✓ Generat: 26_temporal_bookings_top10_countries.csv")
print(f"    Dimensions: {temporal_pivot_top.shape[0]} files")
print(f"    Països: {', '.join(top_countries)}")

# ============================================================================
# CSV TEMPORAL PER ORIGIN (sense country)
# ============================================================================

print("\nGenerant versió per origin (sense country)...")

temporal_data_origin = df_temporal.groupby(['origin', 'dia'], observed=True).size().reset_index(name='bookings')

temporal_pivot_origin = temporal_data_origin.pivot_table(
    index=['origin'],
    columns='dia',
    values='bookings',
    fill_value=0
).reset_index()

date_cols_origin = sorted([col for col in temporal_pivot_origin.columns if col != 'origin'])
temporal_pivot_origin = temporal_pivot_origin[['origin'] + date_cols_origin]

output_path_origin = OUTPUT_DIR / "27_temporal_bookings_by_origin.csv"
temporal_pivot_origin.to_csv(output_path_origin, index=False)
print(f"  ✓ Generat: 27_temporal_bookings_by_origin.csv")
print(f"    Dimensions: {temporal_pivot_origin.shape[0]} files")

# ============================================================================
# CSV TEMPORAL PER HOTEL + ORIGIN
# ============================================================================

print("\nGenerant versió per hotel + origin...")

temporal_data_hotel_origin = df_temporal.groupby(['hotel', 'origin', 'dia'], observed=True).size().reset_index(name='bookings')

temporal_pivot_hotel_origin = temporal_data_hotel_origin.pivot_table(
    index=['hotel', 'origin'],
    columns='dia',
    values='bookings',
    fill_value=0
).reset_index()

date_cols_hotel_origin = sorted([col for col in temporal_pivot_hotel_origin.columns if col not in ['hotel', 'origin']])
temporal_pivot_hotel_origin = temporal_pivot_hotel_origin[['hotel', 'origin'] + date_cols_hotel_origin]

output_path_hotel_origin = OUTPUT_DIR / "28_temporal_bookings_by_hotel_origin.csv"
temporal_pivot_hotel_origin.to_csv(output_path_hotel_origin, index=False)
print(f"  ✓ Generat: 28_temporal_bookings_by_hotel_origin.csv")
print(f"    Dimensions: {temporal_pivot_hotel_origin.shape[0]} files")

# ============================================================================
# CSVs AMB CANCEL RATE (PERCENTATGES) PER HEATMAPS
# ============================================================================

print("\n" + "="*70)
print("GENERANT CSVs AMB CANCEL RATE (PER HEATMAPS)")
print("="*70)

def generate_cancel_rate_csv(df, groupby_cols, output_name):
    """
    Genera un CSV amb cancel rate (percentatge) per heatmaps
    """
    if not groupby_cols:
        result = df.groupby('Status', observed=True).agg({
            'is_canceled': ['count', 'sum']
        }).reset_index()
        result.columns = ['Status', 'total', 'canceled']
        result['cancel_rate'] = (result['canceled'] / result['total'] * 100).round(2)
        result = result[['Status', 'cancel_rate', 'total', 'canceled']]
    else:
        grouped = df.groupby(groupby_cols, observed=True).agg({
            'is_canceled': ['count', 'sum']
        }).reset_index()
        grouped.columns = groupby_cols + ['total', 'canceled']
        grouped['cancel_rate'] = (grouped['canceled'] / grouped['total'] * 100).round(2)
        result = grouped[groupby_cols + ['cancel_rate', 'total', 'canceled']]
    
    output_path = OUTPUT_DIR / f"{output_name}.csv"
    result.to_csv(output_path, index=False)
    print(f"  ✓ Generat: {output_name}.csv ({len(result)} files)")
    return result

# Cancel rate per hotel + origin + tipo (per heatmap principal)
generate_cancel_rate_csv(df, ['hotel', 'origin', 'tipo'], "29_cancel_rate_hotel_origin_tipo")

# Cancel rate per hotel + origin
generate_cancel_rate_csv(df, ['hotel', 'origin'], "30_cancel_rate_hotel_origin")

# Cancel rate per origin + tipo
generate_cancel_rate_csv(df, ['origin', 'tipo'], "31_cancel_rate_origin_tipo")

# Cancel rate per hotel + tipo
generate_cancel_rate_csv(df, ['hotel', 'tipo'], "32_cancel_rate_hotel_tipo")

# Cancel rate per previous_cancellations_group
generate_cancel_rate_csv(df, ['previous_cancellations_group'], "33_cancel_rate_previous_cancellations")

# Cancel rate per hotel + previous_cancellations_group
generate_cancel_rate_csv(df, ['hotel', 'previous_cancellations_group'], "34_cancel_rate_hotel_previous_cancellations")

# Cancel rate per origin + previous_cancellations_group
generate_cancel_rate_csv(df, ['origin', 'previous_cancellations_group'], "35_cancel_rate_origin_previous_cancellations")

# ============================================================================
# CSVs AMB ADR (PER BOXPLOTS/VIOLINS)
# ============================================================================

print("\n" + "="*70)
print("GENERANT CSVs AMB ADR (PER BOXPLOTS/VIOLINS)")
print("="*70)

# Assegurar que ADR és numèric
df['adr'] = pd.to_numeric(df['adr'], errors='coerce')
df_adr = df.dropna(subset=['adr']).copy()

def generate_adr_csv(df, groupby_cols, output_name):
    """
    Genera un CSV amb ADR per cada grup (per boxplots/violins)
    """
    if not groupby_cols:
        result = df[['adr']].copy()
        result['group'] = 'All'
        result = result[['group', 'adr']]
    else:
        result = df[groupby_cols + ['adr']].copy()
        # Renombrar columnes per facilitar l'ús
        result.columns = groupby_cols + ['adr']
    
    output_path = OUTPUT_DIR / f"{output_name}.csv"
    result.to_csv(output_path, index=False)
    print(f"  ✓ Generat: {output_name}.csv ({len(result)} files)")
    return result

# ADR per tipo
generate_adr_csv(df_adr, ['tipo'], "36_adr_by_tipo")

# ADR per hotel + tipo
generate_adr_csv(df_adr, ['hotel', 'tipo'], "37_adr_by_hotel_tipo")

# ADR per origin + tipo
generate_adr_csv(df_adr, ['origin', 'tipo'], "38_adr_by_origin_tipo")

# ADR per hotel + origin + tipo
generate_adr_csv(df_adr, ['hotel', 'origin', 'tipo'], "39_adr_by_hotel_origin_tipo")

# ADR per hotel
generate_adr_csv(df_adr, ['hotel'], "40_adr_by_hotel")

# ADR per origin
generate_adr_csv(df_adr, ['origin'], "41_adr_by_origin")

# ADR per hotel + origin
generate_adr_csv(df_adr, ['hotel', 'origin'], "42_adr_by_hotel_origin")

# ADR per previous_cancellations_group
generate_adr_csv(df_adr, ['previous_cancellations_group'], "43_adr_by_previous_cancellations")

# ============================================================================
# CSVs AMB ESTADÍSTIQUES RESUMIDES D'ADR
# ============================================================================

print("\nGenerant estadístiques resumides d'ADR...")

def generate_adr_summary_csv(df, groupby_cols, output_name):
    """
    Genera un CSV amb estadístiques resumides d'ADR (mitjana, mediana, etc.)
    """
    if not groupby_cols:
        summary = df['adr'].agg(['mean', 'median', 'std', 'min', 'max', 'count']).reset_index()
        summary.columns = ['statistic', 'value']
        summary['group'] = 'All'
        result = summary[['group', 'statistic', 'value']]
    else:
        summary = df.groupby(groupby_cols, observed=True)['adr'].agg([
            'mean', 'median', 'std', 'min', 'max', 'count'
        ]).reset_index()
        summary = summary.melt(
            id_vars=groupby_cols,
            value_vars=['mean', 'median', 'std', 'min', 'max', 'count'],
            var_name='statistic',
            value_name='value'
        )
        result = summary[groupby_cols + ['statistic', 'value']]
    
    output_path = OUTPUT_DIR / f"{output_name}.csv"
    result.to_csv(output_path, index=False)
    print(f"  ✓ Generat: {output_name}.csv ({len(result)} files)")
    return result

# Estadístiques ADR per tipo
generate_adr_summary_csv(df_adr, ['tipo'], "44_adr_summary_by_tipo")

# Estadístiques ADR per hotel + tipo
generate_adr_summary_csv(df_adr, ['hotel', 'tipo'], "45_adr_summary_by_hotel_tipo")

# Estadístiques ADR per origin + tipo
generate_adr_summary_csv(df_adr, ['origin', 'tipo'], "46_adr_summary_by_origin_tipo")

# ============================================================================
# CSVs SETMANALS (PER TEMPORALITAT)
# ============================================================================

print("\n" + "="*70)
print("GENERANT CSVs SETMANALS (PER TEMPORALITAT)")
print("="*70)

# Crear columna setmana
df_temporal['week'] = df_temporal['dia'].dt.to_period('W').dt.start_time

# Setmanal per origin
weekly_origin = df_temporal.groupby(['origin', 'week'], observed=True).size().reset_index(name='bookings')
weekly_origin_pivot = weekly_origin.pivot_table(
    index=['origin'],
    columns='week',
    values='bookings',
    fill_value=0
).reset_index()
week_cols_origin = sorted([col for col in weekly_origin_pivot.columns if col != 'origin'])
weekly_origin_pivot = weekly_origin_pivot[['origin'] + week_cols_origin]
output_path = OUTPUT_DIR / "47_weekly_bookings_by_origin.csv"
weekly_origin_pivot.to_csv(output_path, index=False)
print(f"  ✓ Generat: 47_weekly_bookings_by_origin.csv")

# Setmanal per hotel + origin
weekly_hotel_origin = df_temporal.groupby(['hotel', 'origin', 'week'], observed=True).size().reset_index(name='bookings')
weekly_hotel_origin_pivot = weekly_hotel_origin.pivot_table(
    index=['hotel', 'origin'],
    columns='week',
    values='bookings',
    fill_value=0
).reset_index()
week_cols_hotel_origin = sorted([col for col in weekly_hotel_origin_pivot.columns if col not in ['hotel', 'origin']])
weekly_hotel_origin_pivot = weekly_hotel_origin_pivot[['hotel', 'origin'] + week_cols_hotel_origin]
output_path = OUTPUT_DIR / "48_weekly_bookings_by_hotel_origin.csv"
weekly_hotel_origin_pivot.to_csv(output_path, index=False)
print(f"  ✓ Generat: 48_weekly_bookings_by_hotel_origin.csv")

# Setmanal per hotel
weekly_hotel = df_temporal.groupby(['hotel', 'week'], observed=True).size().reset_index(name='bookings')
weekly_hotel_pivot = weekly_hotel.pivot_table(
    index=['hotel'],
    columns='week',
    values='bookings',
    fill_value=0
).reset_index()
week_cols_hotel = sorted([col for col in weekly_hotel_pivot.columns if col != 'hotel'])
weekly_hotel_pivot = weekly_hotel_pivot[['hotel'] + week_cols_hotel]
output_path = OUTPUT_DIR / "49_weekly_bookings_by_hotel.csv"
weekly_hotel_pivot.to_csv(output_path, index=False)
print(f"  ✓ Generat: 49_weekly_bookings_by_hotel.csv")

# Setmanal amb cancel·lacions per origin
weekly_cancel_origin = df_temporal.groupby(['origin', 'week', 'Status'], observed=True).size().reset_index(name='count')
weekly_cancel_origin_pivot = weekly_cancel_origin.pivot_table(
    index=['origin', 'week'],
    columns='Status',
    values='count',
    fill_value=0
).reset_index()
weekly_cancel_origin_pivot['total'] = weekly_cancel_origin_pivot['Canceled'] + weekly_cancel_origin_pivot['Not canceled']
weekly_cancel_origin_pivot['cancel_rate'] = (weekly_cancel_origin_pivot['Canceled'] / weekly_cancel_origin_pivot['total'] * 100).round(2)
output_path = OUTPUT_DIR / "50_weekly_cancellations_by_origin.csv"
weekly_cancel_origin_pivot.to_csv(output_path, index=False)
print(f"  ✓ Generat: 50_weekly_cancellations_by_origin.csv")

# Setmanal amb cancel·lacions per hotel + origin
weekly_cancel_hotel_origin = df_temporal.groupby(['hotel', 'origin', 'week', 'Status'], observed=True).size().reset_index(name='count')
weekly_cancel_hotel_origin_pivot = weekly_cancel_hotel_origin.pivot_table(
    index=['hotel', 'origin', 'week'],
    columns='Status',
    values='count',
    fill_value=0
).reset_index()
weekly_cancel_hotel_origin_pivot['total'] = weekly_cancel_hotel_origin_pivot['Canceled'] + weekly_cancel_hotel_origin_pivot['Not canceled']
weekly_cancel_hotel_origin_pivot['cancel_rate'] = (weekly_cancel_hotel_origin_pivot['Canceled'] / weekly_cancel_hotel_origin_pivot['total'] * 100).round(2)
output_path = OUTPUT_DIR / "51_weekly_cancellations_by_hotel_origin.csv"
weekly_cancel_hotel_origin_pivot.to_csv(output_path, index=False)
print(f"  ✓ Generat: 51_weekly_cancellations_by_hotel_origin.csv")

# ============================================================================
# CSVs AMB ESTADÍSTIQUES RESUMIDES PER SEGMENTS
# ============================================================================

print("\n" + "="*70)
print("GENERANT CSVs AMB ESTADÍSTIQUES RESUMIDES PER SEGMENTS")
print("="*70)

def generate_segment_summary_csv(df, groupby_cols, output_name):
    """
    Genera un CSV amb estadístiques resumides per segment (total, cancelat, cancel rate, etc.)
    """
    summary = df.groupby(groupby_cols, observed=True).agg({
        'is_canceled': ['count', 'sum', 'mean']
    }).reset_index()
    summary.columns = groupby_cols + ['total_bookings', 'canceled_bookings', 'cancel_rate']
    summary['cancel_rate'] = (summary['cancel_rate'] * 100).round(2)
    summary['not_canceled_bookings'] = summary['total_bookings'] - summary['canceled_bookings']
    
    # Afegir estadístiques d'ADR si està disponible
    if 'adr' in df.columns:
        adr_stats = df.groupby(groupby_cols, observed=True)['adr'].agg(['mean', 'median']).reset_index()
        adr_stats.columns = groupby_cols + ['adr_mean', 'adr_median']
        summary = summary.merge(adr_stats, on=groupby_cols, how='left')
        summary['adr_mean'] = summary['adr_mean'].round(2)
        summary['adr_median'] = summary['adr_median'].round(2)
    
    output_path = OUTPUT_DIR / f"{output_name}.csv"
    summary.to_csv(output_path, index=False)
    print(f"  ✓ Generat: {output_name}.csv ({len(summary)} files)")
    return summary

# Resum per hotel + origin + tipo
generate_segment_summary_csv(df, ['hotel', 'origin', 'tipo'], "52_segment_summary_hotel_origin_tipo")

# Resum per hotel + origin
generate_segment_summary_csv(df, ['hotel', 'origin'], "53_segment_summary_hotel_origin")

# Resum per previous_cancellations_group
generate_segment_summary_csv(df, ['previous_cancellations_group'], "54_segment_summary_previous_cancellations")

# Resum per hotel + previous_cancellations_group
generate_segment_summary_csv(df, ['hotel', 'previous_cancellations_group'], "55_segment_summary_hotel_previous_cancellations")

# Resum per origin + previous_cancellations_group
generate_segment_summary_csv(df, ['origin', 'previous_cancellations_group'], "56_segment_summary_origin_previous_cancellations")

# ============================================================================
# CSV: LEAD_TIME | HOTEL | ORIGIN | % CANCEL·LACIONS
# ============================================================================

print("\n" + "="*70)
print("GENERANT CSV: LEAD_TIME | HOTEL | ORIGIN | % CANCEL·LACIONS")
print("="*70)

# Assegurar que lead_time és numèric
df['lead_time'] = pd.to_numeric(df['lead_time'], errors='coerce')
df_lead_time = df.dropna(subset=['lead_time']).copy()

# Crear variable agrupada de lead_time
def categorize_lead_time(lead_time):
    """Categoritza lead_time en grups"""
    if lead_time == 0:
        return '0'
    elif 1 <= lead_time <= 7:
        return '1-7'
    elif 8 <= lead_time <= 15:
        return '8-15'
    elif 16 <= lead_time <= 30:
        return '16-30'
    elif 31 <= lead_time <= 90:
        return '31-90'
    elif 91 <= lead_time <= 180:
        return '91-180'
    elif 181 <= lead_time <= 365:
        return '181-365'
    else:
        return '+365'

df_lead_time['lead_time_group'] = df_lead_time['lead_time'].apply(categorize_lead_time)

# Assegurar que origin existeix
if 'origin' not in df_lead_time.columns:
    df_lead_time['origin'] = df_lead_time['country'].apply(lambda x: 'Portugal' if x == 'PRT' else 'International')

# Agrupar per lead_time_group, hotel i origin
lead_time_grouped = df_lead_time.groupby(['lead_time_group', 'hotel', 'origin'], observed=True).agg({
    'is_canceled': ['count', 'sum']
}).reset_index()

lead_time_grouped.columns = ['lead_time_group', 'hotel', 'origin', 'total', 'canceled']
lead_time_grouped['cancel_rate'] = (lead_time_grouped['canceled'] / lead_time_grouped['total'] * 100).round(2)
lead_time_grouped = lead_time_grouped.drop(columns=['total', 'canceled'])

# Reordenar columnes
lead_time_result = lead_time_grouped[['lead_time_group', 'hotel', 'origin', 'cancel_rate']]

# Afegir files "Total" per origin
# Agrupar per lead_time_group i hotel (sense origin) per calcular Total
lead_time_total = df_lead_time.groupby(['lead_time_group', 'hotel'], observed=True).agg({
    'is_canceled': ['count', 'sum']
}).reset_index()

lead_time_total.columns = ['lead_time_group', 'hotel', 'total', 'canceled']
lead_time_total['cancel_rate'] = (lead_time_total['canceled'] / lead_time_total['total'] * 100).round(2)
lead_time_total = lead_time_total.drop(columns=['total', 'canceled'])

# Afegir columna origin='Total'
lead_time_total['origin'] = 'Total'

# Reordenar columnes per coincidir amb lead_time_result
lead_time_total = lead_time_total[['lead_time_group', 'hotel', 'origin', 'cancel_rate']]

# Afegir al resultat
lead_time_result = pd.concat([lead_time_result, lead_time_total], ignore_index=True)

# Ordenar per lead_time_group (ordre personalitzat) i després per origin (Total al final)
lead_time_order = ['0', '1-7', '8-15', '16-30', '31-90', '91-180', '181-365', '+365']
lead_time_result['lead_time_order'] = lead_time_result['lead_time_group'].apply(lambda x: lead_time_order.index(x) if x in lead_time_order else 999)
lead_time_result['origin_order'] = lead_time_result['origin'].apply(lambda x: 1 if x == 'Total' else 0)
lead_time_result = lead_time_result.sort_values(['lead_time_order', 'hotel', 'origin_order', 'origin'])
lead_time_result = lead_time_result.drop(columns=['lead_time_order', 'origin_order'])

# Mapping de noms de columnes
lead_time_result = lead_time_result.rename(columns={
    'lead_time_group': 'Lead Time',
    'hotel': 'Hotel',
    'origin': 'Origin',
    'cancel_rate': '% Cancellations'
})

# Guardar
output_path = OUTPUT_DIR / "57_lead_time_hotel_origin_cancel_rate.csv"
lead_time_result.to_csv(output_path, index=False)
print(f"  ✓ Generat: 57_lead_time_hotel_origin_cancel_rate.csv ({len(lead_time_result)} files)")

# ============================================================================
# CSV: HOTEL | ORIGIN | MES | % CANCEL·LACIONS
# ============================================================================

print("\n" + "="*70)
print("GENERANT CSV: HOTEL | ORIGIN | MES | % CANCEL·LACIONS")
print("="*70)

# Assegurar que 'dia' és datetime
df['dia'] = pd.to_datetime(df['dia'], errors='coerce')
df_month = df.dropna(subset=['dia']).copy()

# Extreure el mes (nom del mes en anglès per ordenar correctament)
df_month['month'] = df_month['dia'].dt.month
df_month['month_name'] = df_month['dia'].dt.strftime('%B')  # Gener, Febrer, etc.

# Assegurar que origin existeix
if 'origin' not in df_month.columns:
    df_month['origin'] = df_month['country'].apply(lambda x: 'Portugal' if x == 'PRT' else 'International')

# Agrupar per hotel, origin i mes
month_grouped = df_month.groupby(['hotel', 'origin', 'month', 'month_name'], observed=True).agg({
    'is_canceled': ['count', 'sum']
}).reset_index()

month_grouped.columns = ['hotel', 'origin', 'month', 'month_name', 'total', 'canceled']
month_grouped['cancel_rate'] = (month_grouped['canceled'] / month_grouped['total'] * 100).round(2)
month_grouped = month_grouped.drop(columns=['total', 'canceled', 'month'])

# Reordenar columnes
month_grouped_result = month_grouped[['hotel', 'origin', 'month_name', 'cancel_rate']]

# Afegir files "Total" per origin
# Agrupar per hotel i mes (sense origin) per calcular Total
month_total = df_month.groupby(['hotel', 'month', 'month_name'], observed=True).agg({
    'is_canceled': ['count', 'sum']
}).reset_index()

month_total.columns = ['hotel', 'month', 'month_name', 'total', 'canceled']
month_total['cancel_rate'] = (month_total['canceled'] / month_total['total'] * 100).round(2)
month_total = month_total.drop(columns=['total', 'canceled', 'month'])

# Afegir columna origin='Total'
month_total['origin'] = 'Total'

# Reordenar columnes per coincidir amb month_grouped_result
month_total = month_total[['hotel', 'origin', 'month_name', 'cancel_rate']]

# Afegir al resultat
month_result_long = pd.concat([month_grouped_result, month_total], ignore_index=True)

# Pivotar: Hotel com a columnes
month_result = month_result_long.pivot_table(
    index=['origin', 'month_name'],
    columns='hotel',
    values='cancel_rate',
    fill_value=0
).reset_index()

# Renombrar columnes de hotel
month_result.columns.name = None  # Eliminar nom del multi-index
month_result = month_result.rename(columns={
    'City Hotel': '% Cancellations City Hotel',
    'Resort Hotel': '% Cancellations Resort Hotel'
})

# Ordenar per mes (ordre cronològic) i després per origin (Total al final)
month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
               'July', 'August', 'September', 'October', 'November', 'December']
month_result['month_order'] = month_result['month_name'].apply(lambda x: month_order.index(x) if x in month_order else 999)
month_result['origin_order'] = month_result['origin'].apply(lambda x: 1 if x == 'Total' else 0)
month_result = month_result.sort_values(['month_order', 'origin_order', 'origin'])
month_result = month_result.drop(columns=['month_order', 'origin_order'])

# Reordenar columnes: Origin, Month, després les columnes de hotel
hotel_cols = [col for col in month_result.columns if col not in ['origin', 'month_name']]
month_result = month_result[['origin', 'month_name'] + hotel_cols]

# Mapping de noms de columnes
month_result = month_result.rename(columns={
    'origin': 'Origin',
    'month_name': 'Month'
})

# Guardar
output_path = OUTPUT_DIR / "58_hotel_origin_month_cancel_rate.csv"
month_result.to_csv(output_path, index=False)
print(f"  ✓ Generat: 58_hotel_origin_month_cancel_rate.csv ({len(month_result)} files)")

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


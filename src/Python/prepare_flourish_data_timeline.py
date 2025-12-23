#!/usr/bin/env python3
"""
Script per preparar dades temporals per Flourish.studio
Genera CSVs amb setmanes/anys com a columnes per visualitzacions de timeline
"""

import pandas as pd
import numpy as np
from pathlib import Path

# Configuració
INPUT_FILE = "../../output/hotel_booking_final.csv"
OUTPUT_DIR = Path("flourish_timeline_csv")
OUTPUT_DIR.mkdir(exist_ok=True)

print("Carregant dades...")
df = pd.read_csv(INPUT_FILE)

# Neteja bàsica: assegurar que is_canceled és numèric
df['is_canceled'] = pd.to_numeric(df['is_canceled'], errors='coerce')
df['is_canceled'] = df['is_canceled'].fillna(0).astype(int)

# Assegurar que 'dia' és datetime
df['dia'] = pd.to_datetime(df['dia'], errors='coerce')

# Filtrar dates vàlides
df = df.dropna(subset=['dia']).copy()

# Crear columna week (setmana)
df['week'] = df['dia'].dt.to_period('W').dt.start_time

# Assegurar que origin existeix
if 'origin' not in df.columns:
    df['origin'] = df['country'].apply(lambda x: 'Portugal' if x == 'PRT' else 'International')

# Top 5 països
top_countries_list = ['PRT', 'GBR', 'FRA', 'ESP', 'DEU']
df_top_countries = df[df['country'].isin(top_countries_list)].copy()

print(f"Dades carregades: {len(df)} files")
print(f"Període: {df['dia'].min()} a {df['dia'].max()}")

# ============================================================================
# FUNCIÓ PER FORMATAR COLUMNES DE SETMANA
# ============================================================================

def format_week_columns(df_result, groupby_cols):
    """
    Formata les columnes de setmana per eliminar la part de l'hora (00:00:00)
    """
    # Crear un nou diccionari de renombrat
    rename_dict = {}
    for col in df_result.columns:
        if col not in groupby_cols:
            # Si és una columna de setmana (datetime), formatar-la
            if isinstance(col, pd.Timestamp):
                rename_dict[col] = col.strftime('%Y-%m-%d')
            elif isinstance(col, str):
                # Si ja és string amb hora, eliminar-la
                if ' 00:00:00' in col:
                    rename_dict[col] = col.split(' 00:00:00')[0]
                elif '00:00:00' in col:
                    # Cas on pot estar al principi o mig
                    rename_dict[col] = col.replace(' 00:00:00', '').replace('00:00:00', '')
    
    if rename_dict:
        df_result = df_result.rename(columns=rename_dict)
    
    return df_result

def combine_groupby_columns(df_result, groupby_cols):
    """
    Combina les columnes d'agrupació en una única columna "Label" separada per guions
    """
    # Identificar columnes que no són dates (són les columnes d'agrupació o altres)
    date_cols = []
    label_cols = []
    
    for col in df_result.columns:
        # Detectar si és una columna de data (format YYYY-MM-DD)
        if isinstance(col, str) and len(col) == 10 and col[4] == '-' and col[7] == '-':
            try:
                pd.to_datetime(col)
                date_cols.append(col)
            except:
                label_cols.append(col)
        elif col in groupby_cols:
            label_cols.append(col)
        else:
            # Si no és una data coneguda, assumir que és una columna d'agrupació
            if col not in ['Label']:
                label_cols.append(col)
    
    # Si hi ha columnes d'agrupació, combinar-les en "Label"
    if label_cols:
        # Crear columna Label combinant les columnes d'agrupació
        df_result['Label'] = df_result[label_cols].astype(str).agg('-'.join, axis=1)
        
        # Reordenar: Label primer, després les dates
        df_result = df_result[['Label'] + date_cols]
    
    return df_result

# ============================================================================
# DATASET 1: Hotel | Origin | (cada setmana) - Nombre de reserves
# ============================================================================

print("\n" + "="*70)
print("GENERANT DATASET 1: Hotel | Origin | Setmanes (Nombre de reserves)")
print("="*70)

# Agrupar per week, hotel i origin
weekly_hotel_origin = df.groupby(['week', 'hotel', 'origin'], observed=True).size().reset_index(name='bookings')

# Pivotar: cada setmana com a columna
timeline_1 = weekly_hotel_origin.pivot_table(
    index=['hotel', 'origin'],
    columns='week',
    values='bookings',
    fill_value=0
).reset_index()

# Ordenar columnes: primer hotel i origin, després les setmanes ordenades
week_cols = sorted([col for col in timeline_1.columns if col not in ['hotel', 'origin']])
timeline_1 = timeline_1[['hotel', 'origin'] + week_cols]

# Formatar columnes de setmana (eliminar 00:00:00)
timeline_1 = format_week_columns(timeline_1, ['hotel', 'origin'])

# Mapping de noms de columnes
timeline_1 = timeline_1.rename(columns={'hotel': 'Hotel', 'origin': 'Origin'})

# Combinar columnes d'agrupació en "Label"
timeline_1 = combine_groupby_columns(timeline_1, ['Hotel', 'Origin'])

# Guardar
output_path = OUTPUT_DIR / "01_timeline_hotel_origin_bookings.csv"
timeline_1.to_csv(output_path, index=False)
print(f"  ✓ Generat: 01_timeline_hotel_origin_bookings.csv")
print(f"    Dimensions: {timeline_1.shape[0]} files (combinacions hotel+origin)")
print(f"    Període: {len(week_cols)} setmanes")

# ============================================================================
# DATASET 2: Hotel | Country | (cada setmana) - Nombre de reserves (Top 5)
# ============================================================================

print("\n" + "="*70)
print("GENERANT DATASET 2: Hotel | Country | Setmanes (Nombre de reserves - Top 5)")
print("="*70)

# Agrupar per week, hotel i country (només top 5)
weekly_hotel_country = df_top_countries.groupby(['week', 'hotel', 'country'], observed=True).size().reset_index(name='bookings')

# Pivotar: cada setmana com a columna
timeline_2 = weekly_hotel_country.pivot_table(
    index=['hotel', 'country'],
    columns='week',
    values='bookings',
    fill_value=0
).reset_index()

# Ordenar columnes: primer hotel i country, després les setmanes ordenades
week_cols_2 = sorted([col for col in timeline_2.columns if col not in ['hotel', 'country']])
timeline_2 = timeline_2[['hotel', 'country'] + week_cols_2]

# Formatar columnes de setmana (eliminar 00:00:00)
timeline_2 = format_week_columns(timeline_2, ['hotel', 'country'])

# Mapping de noms de columnes
timeline_2 = timeline_2.rename(columns={'hotel': 'Hotel', 'country': 'Country'})

# Combinar columnes d'agrupació en "Label"
timeline_2 = combine_groupby_columns(timeline_2, ['Hotel', 'Country'])

# Guardar
output_path = OUTPUT_DIR / "02_timeline_hotel_country_bookings.csv"
timeline_2.to_csv(output_path, index=False)
print(f"  ✓ Generat: 02_timeline_hotel_country_bookings.csv")
print(f"    Dimensions: {timeline_2.shape[0]} files (combinacions hotel+country)")
print(f"    Països: {', '.join(top_countries_list)}")

# ============================================================================
# DATASET 3: Hotel | Origin | (cada setmana) - % de cancel·lacions
# ============================================================================

print("\n" + "="*70)
print("GENERANT DATASET 3: Hotel | Origin | Setmanes (% de cancel·lacions)")
print("="*70)

# Agrupar per week, hotel, origin i is_canceled
weekly_cancel_hotel_origin = df.groupby(['week', 'hotel', 'origin', 'is_canceled'], observed=True).size().reset_index(name='count')

# Calcular cancel rate correctament
weekly_cancel_pivot = weekly_cancel_hotel_origin.pivot_table(
    index=['week', 'hotel', 'origin'],
    columns='is_canceled',
    values='count',
    fill_value=0
).reset_index()

# Calcular cancel rate
weekly_cancel_pivot['total'] = weekly_cancel_pivot[0] + weekly_cancel_pivot[1]
weekly_cancel_pivot['cancel_rate'] = (weekly_cancel_pivot[1] / weekly_cancel_pivot['total'] * 100).round(2)
weekly_cancel_pivot = weekly_cancel_pivot[['week', 'hotel', 'origin', 'cancel_rate']]

# Pivotar: cada setmana com a columna amb cancel rate
timeline_3 = weekly_cancel_pivot.pivot_table(
    index=['hotel', 'origin'],
    columns='week',
    values='cancel_rate',
    fill_value=0
).reset_index()

# Ordenar columnes: primer hotel i origin, després les setmanes ordenades
week_cols_3 = sorted([col for col in timeline_3.columns if col not in ['hotel', 'origin']])
timeline_3 = timeline_3[['hotel', 'origin'] + week_cols_3]

# Formatar columnes de setmana (eliminar 00:00:00)
timeline_3 = format_week_columns(timeline_3, ['hotel', 'origin'])

# Mapping de noms de columnes
timeline_3 = timeline_3.rename(columns={'hotel': 'Hotel', 'origin': 'Origin'})

# Combinar columnes d'agrupació en "Label"
timeline_3 = combine_groupby_columns(timeline_3, ['Hotel', 'Origin'])

# Guardar
output_path = OUTPUT_DIR / "03_timeline_hotel_origin_cancel_rate.csv"
timeline_3.to_csv(output_path, index=False)
print(f"  ✓ Generat: 03_timeline_hotel_origin_cancel_rate.csv")
print(f"    Dimensions: {timeline_3.shape[0]} files (combinacions hotel+origin)")

# ============================================================================
# DATASET 4: Hotel | Country | (cada setmana) - % de cancel·lacions (Top 5)
# ============================================================================

print("\n" + "="*70)
print("GENERANT DATASET 4: Hotel | Country | Setmanes (% de cancel·lacions - Top 5)")
print("="*70)

# Agrupar per week, hotel, country i is_canceled (només top 5)
weekly_cancel_hotel_country = df_top_countries.groupby(['week', 'hotel', 'country', 'is_canceled'], observed=True).size().reset_index(name='count')

# Calcular cancel rate
weekly_cancel_country_pivot = weekly_cancel_hotel_country.pivot_table(
    index=['week', 'hotel', 'country'],
    columns='is_canceled',
    values='count',
    fill_value=0
).reset_index()

# Calcular cancel rate
weekly_cancel_country_pivot['total'] = weekly_cancel_country_pivot[0] + weekly_cancel_country_pivot[1]
weekly_cancel_country_pivot['cancel_rate'] = (weekly_cancel_country_pivot[1] / weekly_cancel_country_pivot['total'] * 100).round(2)
weekly_cancel_country_pivot = weekly_cancel_country_pivot[['week', 'hotel', 'country', 'cancel_rate']]

# Pivotar: cada setmana com a columna amb cancel rate
timeline_4 = weekly_cancel_country_pivot.pivot_table(
    index=['hotel', 'country'],
    columns='week',
    values='cancel_rate',
    fill_value=0
).reset_index()

# Ordenar columnes: primer hotel i country, després les setmanes ordenades
week_cols_4 = sorted([col for col in timeline_4.columns if col not in ['hotel', 'country']])
timeline_4 = timeline_4[['hotel', 'country'] + week_cols_4]

# Formatar columnes de setmana (eliminar 00:00:00)
timeline_4 = format_week_columns(timeline_4, ['hotel', 'country'])

# Mapping de noms de columnes
timeline_4 = timeline_4.rename(columns={'hotel': 'Hotel', 'country': 'Country'})

# Combinar columnes d'agrupació en "Label"
timeline_4 = combine_groupby_columns(timeline_4, ['Hotel', 'Country'])

# Guardar
output_path = OUTPUT_DIR / "04_timeline_hotel_country_cancel_rate.csv"
timeline_4.to_csv(output_path, index=False)
print(f"  ✓ Generat: 04_timeline_hotel_country_cancel_rate.csv")
print(f"    Dimensions: {timeline_4.shape[0]} files (combinacions hotel+country)")

# ============================================================================
# DATASET 5: Origin | (cada setmana) - Nombre de reserves
# ============================================================================

print("\n" + "="*70)
print("GENERANT DATASET 5: Origin | Setmanes (Nombre de reserves)")
print("="*70)

# Agrupar per week i origin
weekly_origin = df.groupby(['week', 'origin'], observed=True).size().reset_index(name='bookings')

# Pivotar: cada setmana com a columna
timeline_5 = weekly_origin.pivot_table(
    index=['origin'],
    columns='week',
    values='bookings',
    fill_value=0
).reset_index()

# Ordenar columnes: primer origin, després les setmanes ordenades
week_cols_5 = sorted([col for col in timeline_5.columns if col != 'origin'])
timeline_5 = timeline_5[['origin'] + week_cols_5]

# Formatar columnes de setmana (eliminar 00:00:00)
timeline_5 = format_week_columns(timeline_5, ['origin'])

# Mapping de noms de columnes
timeline_5 = timeline_5.rename(columns={'origin': 'Origin'})

# Combinar columnes d'agrupació en "Label"
timeline_5 = combine_groupby_columns(timeline_5, ['Origin'])

# Guardar
output_path = OUTPUT_DIR / "05_timeline_origin_bookings.csv"
timeline_5.to_csv(output_path, index=False)
print(f"  ✓ Generat: 05_timeline_origin_bookings.csv")
print(f"    Dimensions: {timeline_5.shape[0]} files (combinacions origin)")

# ============================================================================
# DATASET 6: Country | (cada setmana) - Nombre de reserves (Top 5)
# ============================================================================

print("\n" + "="*70)
print("GENERANT DATASET 6: Country | Setmanes (Nombre de reserves - Top 5)")
print("="*70)

# Agrupar per week i country (només top 5)
weekly_country = df_top_countries.groupby(['week', 'country'], observed=True).size().reset_index(name='bookings')

# Pivotar: cada setmana com a columna
timeline_6 = weekly_country.pivot_table(
    index=['country'],
    columns='week',
    values='bookings',
    fill_value=0
).reset_index()

# Ordenar columnes: primer country, després les setmanes ordenades
week_cols_6 = sorted([col for col in timeline_6.columns if col != 'country'])
timeline_6 = timeline_6[['country'] + week_cols_6]

# Formatar columnes de setmana (eliminar 00:00:00)
timeline_6 = format_week_columns(timeline_6, ['country'])

# Mapping de noms de columnes
timeline_6 = timeline_6.rename(columns={'country': 'Country'})

# Combinar columnes d'agrupació en "Label"
timeline_6 = combine_groupby_columns(timeline_6, ['Country'])

# Guardar
output_path = OUTPUT_DIR / "06_timeline_country_bookings.csv"
timeline_6.to_csv(output_path, index=False)
print(f"  ✓ Generat: 06_timeline_country_bookings.csv")
print(f"    Dimensions: {timeline_6.shape[0]} files (combinacions country)")

# ============================================================================
# DATASET 7: Origin | (cada setmana) - % de cancel·lacions
# ============================================================================

print("\n" + "="*70)
print("GENERANT DATASET 7: Origin | Setmanes (% de cancel·lacions)")
print("="*70)

# Agrupar per week, origin i is_canceled
weekly_cancel_origin = df.groupby(['week', 'origin', 'is_canceled'], observed=True).size().reset_index(name='count')

# Calcular cancel rate
weekly_cancel_origin_pivot = weekly_cancel_origin.pivot_table(
    index=['week', 'origin'],
    columns='is_canceled',
    values='count',
    fill_value=0
).reset_index()

# Calcular cancel rate
weekly_cancel_origin_pivot['total'] = weekly_cancel_origin_pivot[0] + weekly_cancel_origin_pivot[1]
weekly_cancel_origin_pivot['cancel_rate'] = (weekly_cancel_origin_pivot[1] / weekly_cancel_origin_pivot['total'] * 100).round(2)
weekly_cancel_origin_pivot = weekly_cancel_origin_pivot[['week', 'origin', 'cancel_rate']]

# Pivotar: cada setmana com a columna amb cancel rate
timeline_7 = weekly_cancel_origin_pivot.pivot_table(
    index=['origin'],
    columns='week',
    values='cancel_rate',
    fill_value=0
).reset_index()

# Ordenar columnes: primer origin, després les setmanes ordenades
week_cols_7 = sorted([col for col in timeline_7.columns if col != 'origin'])
timeline_7 = timeline_7[['origin'] + week_cols_7]

# Formatar columnes de setmana (eliminar 00:00:00)
timeline_7 = format_week_columns(timeline_7, ['origin'])

# Mapping de noms de columnes
timeline_7 = timeline_7.rename(columns={'origin': 'Origin'})

# Combinar columnes d'agrupació en "Label"
timeline_7 = combine_groupby_columns(timeline_7, ['Origin'])

# Guardar
output_path = OUTPUT_DIR / "07_timeline_origin_cancel_rate.csv"
timeline_7.to_csv(output_path, index=False)
print(f"  ✓ Generat: 07_timeline_origin_cancel_rate.csv")
print(f"    Dimensions: {timeline_7.shape[0]} files (combinacions origin)")

# ============================================================================
# DATASET 8: Country | (cada setmana) - % de cancel·lacions (Top 5)
# ============================================================================

print("\n" + "="*70)
print("GENERANT DATASET 8: Country | Setmanes (% de cancel·lacions - Top 5)")
print("="*70)

# Agrupar per week, country i is_canceled (només top 5)
weekly_cancel_country_only = df_top_countries.groupby(['week', 'country', 'is_canceled'], observed=True).size().reset_index(name='count')

# Calcular cancel rate
weekly_cancel_country_only_pivot = weekly_cancel_country_only.pivot_table(
    index=['week', 'country'],
    columns='is_canceled',
    values='count',
    fill_value=0
).reset_index()

# Calcular cancel rate
weekly_cancel_country_only_pivot['total'] = weekly_cancel_country_only_pivot[0] + weekly_cancel_country_only_pivot[1]
weekly_cancel_country_only_pivot['cancel_rate'] = (weekly_cancel_country_only_pivot[1] / weekly_cancel_country_only_pivot['total'] * 100).round(2)
weekly_cancel_country_only_pivot = weekly_cancel_country_only_pivot[['week', 'country', 'cancel_rate']]

# Pivotar: cada setmana com a columna amb cancel rate
timeline_8 = weekly_cancel_country_only_pivot.pivot_table(
    index=['country'],
    columns='week',
    values='cancel_rate',
    fill_value=0
).reset_index()

# Ordenar columnes: primer country, després les setmanes ordenades
week_cols_8 = sorted([col for col in timeline_8.columns if col != 'country'])
timeline_8 = timeline_8[['country'] + week_cols_8]

# Formatar columnes de setmana (eliminar 00:00:00)
timeline_8 = format_week_columns(timeline_8, ['country'])

# Mapping de noms de columnes
timeline_8 = timeline_8.rename(columns={'country': 'Country'})

# Combinar columnes d'agrupació en "Label"
timeline_8 = combine_groupby_columns(timeline_8, ['Country'])

# Guardar
output_path = OUTPUT_DIR / "08_timeline_country_cancel_rate.csv"
timeline_8.to_csv(output_path, index=False)
print(f"  ✓ Generat: 08_timeline_country_cancel_rate.csv")
print(f"    Dimensions: {timeline_8.shape[0]} files (combinacions country)")

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


import pandas as pd
import os

agua_esgoto = 'dados/Esgoto e Água - (2020 - 2021).csv'
agua_pluv = 'dados/Pluvial - (2020 - 2021).csv'
residuos_solid = 'dados/Residuos - (2020 - 2021).csv'
dengue_202x = 'dados/Dengue/DENGUE - 2020-2021.csv'
chiku_202x = 'dados/Chiku/CHIKU - 2020-2021.csv'
populacao_total = 'dados/População - 2022.csv'

def total_casos(filepath, doenca):
    df = pd.read_csv(filepath, sep=',', encoding='utf-8', na_values='-')
    df.columns = df.columns.str.strip().str.replace(',', '', regex=False)
    colunas_estados = [
        'RO', 'AC', 'AM', 'RR', 'PA', 'AP', 'TO', 'MA', 'PI', 'CE', 'RN',
        'PB', 'PE', 'AL', 'SE', 'BA', 'MG', 'ES', 'RJ', 'SP', 'PR', 'SC',
        'RS', 'MS', 'MT', 'GO', 'DF'
    ]
    colunas_estados_presentes = [col for col in colunas_estados if col in df.columns]
    for col in colunas_estados_presentes:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['Total'] = df[colunas_estados_presentes].sum(axis=1, skipna=True)
    df['Total'] = df['Total'].fillna(0).astype(int)
    df['Tipo_Doenca'] = doenca
    df_final = df[['Codigo do municipio', 'Total', 'Ano', 'Tipo_Doenca']]
    print(f"Lido casos: {os.path.basename(filepath)}")
    return df_final

def dados_agua_esgoto(filepath):
    colunas_snis = ['Codigo do municipio', 'Municipio', 'Ano', 'UF', 'IN055', 'IN056']
    df = pd.read_csv(filepath, sep=',', encoding='utf-8', usecols=colunas_snis, decimal='.')
    df.columns = df.columns.str.strip()
    df = df.rename(columns={'IN055': 'Agua', 'IN056': 'Esgoto'})
    df['Agua'] = pd.to_numeric(df['Agua'], errors='coerce')
    df['Esgoto'] = pd.to_numeric(df['Esgoto'], errors='coerce')
    print(f"Lido SNIS AE: {os.path.basename(filepath)}")
    return df

def dados_residuos(filepath):
    colunas_residuos = ['Codigo do municipio', 'Ano', 'IN015']
    df = pd.read_csv(filepath, sep=',', encoding='utf-8', usecols=colunas_residuos, decimal=',')
    df = df.rename(columns={'IN015': 'Coleta_Lixo'})
    df['Coleta_Lixo'] = pd.to_numeric(df['Coleta_Lixo'], errors='coerce')
    print(f"Lido SNIS RSU: {os.path.basename(filepath)}")
    return df[['Codigo do municipio', 'Ano', 'Coleta_Lixo']]

def dados_pluvial(filepath):
    colunas_pluvial = ['Codigo do municipio', 'Ano', 'IN026']
    df = pd.read_csv(filepath, sep=',', encoding='utf-8', usecols=colunas_pluvial, decimal=',')
    df = df.rename(columns={'IN026': 'Drenagem'})
    df['Drenagem'] = pd.to_numeric(df['Drenagem'], errors='coerce')
    print(f"Lido SNIS AP: {os.path.basename(filepath)}")
    return df[['Codigo do municipio', 'Ano', 'Drenagem']]

def dados_populacao(filepath):
    colunas_pop = ['Municipio', 'Populacao']
    df = pd.read_csv(filepath,
                     sep=',',
                     encoding='latin1',
                     usecols=colunas_pop,
                     thousands='.',
                     skipinitialspace=True
                    )
    df.columns = df.columns.str.strip()
    if df['Populacao'].dtype == 'object':
        df['Populacao'] = df['Populacao'].astype(str).str.replace(r'\s+', '', regex=True)
        df['Populacao'] = pd.to_numeric(df['Populacao'], errors='coerce')
    df.dropna(subset=['Populacao'], inplace=True)
    df['Populacao'] = df['Populacao'].astype(int)
    df = df.drop_duplicates(subset=['Municipio'], keep='first')
    print(f"Lido População: {os.path.basename(filepath)}")
    return df[['Municipio', 'Populacao']]

total_dengue = total_casos(dengue_202x, 'Dengue')
total_chiku = total_casos(chiku_202x, 'Chiku')
agua_esgoto = dados_agua_esgoto(agua_esgoto)
residuos = dados_residuos(residuos_solid)
pluvial = dados_pluvial(agua_pluv)
populacao = dados_populacao(populacao_total)

casos_dengue = pd.pivot_table(total_dengue,
                              values='Total',
                              index=['Codigo do municipio', 'Ano'],
                              columns='Tipo_Doenca',
                              aggfunc='sum',
                              fill_value=0).reset_index()

casos_chiku = pd.pivot_table(total_chiku,
                             values='Total',
                             index=['Codigo do municipio', 'Ano'],
                             columns='Tipo_Doenca',
                             aggfunc='sum',
                             fill_value=0).reset_index()

df_final = agua_esgoto.copy()
df_final = pd.merge(df_final, residuos, on=['Codigo do municipio', 'Ano'], how='left')
df_final = pd.merge(df_final, pluvial, on=['Codigo do municipio', 'Ano'], how='left')
df_final = pd.merge(df_final, populacao, on='Municipio', how='left')
df_final = pd.merge(df_final, casos_dengue, on=['Codigo do municipio', 'Ano'], how='inner')
df_final = pd.merge(df_final, casos_chiku, on=['Codigo do municipio', 'Ano'], how='inner')

indicadores_todos = ['Agua', 'Esgoto', 'Coleta_Lixo', 'Drenagem']

for col in indicadores_todos:
    if col in df_final.columns:
        df_final[col] = df_final[col].fillna(0)
    else:
        print(f"Atenção: Coluna '{col}' não encontrada após merge. Será criada com valor 0.")
        df_final[col] = 0

df_final['Dengue'] = df_final['Dengue'].fillna(0).astype(int)
df_final['Chiku'] = df_final['Chiku'].fillna(0).astype(int)
df_final['Total'] = df_final['Dengue'] + df_final['Chiku']

original_rows_before_validation = len(df_final)

df_final.dropna(subset=['Esgoto'], inplace=True)
df_final.dropna(subset=['Populacao'], inplace=True)
df_final = df_final[df_final['Populacao'] > 0]

rows_before_indicator_filter = len(df_final)
df_final = df_final[
    (df_final['Agua'] > 0) &
    (df_final['Esgoto'] > 0) &
    (df_final['Coleta_Lixo'] > 0) &
    (df_final['Drenagem'] > 0)
].copy()
rows_after_indicator_filter = len(df_final)

df_final['Deficit_Agua'] = 100 - df_final['Agua']
df_final['Deficit_Esgoto'] = 100 - df_final['Esgoto']
df_final['Deficit_Coleta_Lixo'] = 100 - df_final['Coleta_Lixo']
df_final['Deficit_Drenagem'] = 100 - df_final['Drenagem']

df_final['Pontuacao_Deficit_Saneamento'] = (
    df_final['Deficit_Agua'] +
    df_final['Deficit_Esgoto'] +
    df_final['Deficit_Coleta_Lixo'] +
    df_final['Deficit_Drenagem']
) / 4

df_final['Pontuacao_Deficit_Saneamento'] = df_final['Pontuacao_Deficit_Saneamento'].round(2)

indicadores_arredondar = ['Agua', 'Esgoto', 'Coleta_Lixo', 'Drenagem']
for col in indicadores_arredondar:
     if col in df_final.columns:
        df_final[col] = df_final[col].round(2)

pd.options.display.float_format = '{:.2f}'.format

print("RANKING DOS MUNICÍPIOS COM PIOR SANEAMENTO BÁSICO (MÉDIA DOS 4 INDICADORES)")
print("Quanto maior a pontuação, pior o saneamento médio.")

ranking_deficit = df_final.sort_values(by=['Ano', 'Pontuacao_Deficit_Saneamento'], ascending=[False, False])

colunas_para_mostrar = ['Ano', 'Municipio', 'UF', 'Pontuacao_Deficit_Saneamento', 'Dengue', 'Chiku', 'Total']

print("\nTop 20 Piores - 2021")
df_print_2021 = ranking_deficit[ranking_deficit['Ano'] == 2021][colunas_para_mostrar].head(20)
if not df_print_2021.empty:
    print(df_print_2021.to_string(index=False))
else:
    print("Não há dados para 2021 neste ranking após a filtragem.")

print("\nTop 20 Piores - 2020")
df_print_2020 = ranking_deficit[ranking_deficit['Ano'] == 2020][colunas_para_mostrar].head(20)
if not df_print_2020.empty:
    print(df_print_2020.to_string(index=False))
else:
    print("Não há dados para 2020 neste ranking após a filtragem.")

print("\n--- Processamento Concluído ---")
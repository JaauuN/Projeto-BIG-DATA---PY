import pandas as pd

agua_esgoto = 'dados/Esgoto e Água - (2020 - 2021).csv'
agua_pluv = 'dados/Pluvial - (2020 - 2021).csv'
residuos_solid = 'dados/Residuos - (2020 - 2021).csv'
dengue_202x = 'dados/Dengue/DENGUE - 2020-2021.csv'
chiku_202x = 'dados/Chiku/CHIKU - 2020-2021.csv'


def total_casos(filepath, doenca):
    df = pd.read_csv(filepath, sep=',', encoding='utf-8', na_values='-')
    colunas_estados = ['RO', 'AC', 'AM', 'RR', 'PA', 'AP', 'TO', 'MA', 'PI', 'CE', 'RN','PB', 'PE', 'AL', 'SE', 'BA', 'MG', 'ES', 'RJ', 'SP', 'PR', 'SC','RS', 'MS', 'MT', 'GO', 'DF']
    colunas_estados_presentes = [col for col in colunas_estados if col in df.columns]
    df[colunas_estados_presentes] = df[colunas_estados_presentes].apply(pd.to_numeric, errors='coerce')
    df['Total'] = df[colunas_estados_presentes].sum(axis=1, skipna=True).fillna(0).astype(int)
    df['Tipo_Doenca'] = doenca
    return df[['Codigo do municipio', 'Total', 'Ano', 'Tipo_Doenca']]

def dados_agua_esgoto(filepath):
    colunas = ['Codigo do municipio', 'Municipio', 'Ano', 'UF', 'IN055', 'IN056']
    df = pd.read_csv(filepath, sep=',', encoding='utf-8', usecols=colunas, decimal='.')
    df = df.rename(columns={'IN055': 'Agua', 'IN056': 'Esgoto'})
    df[['Agua', 'Esgoto']] = df[['Agua', 'Esgoto']].apply(pd.to_numeric, errors='coerce')
    return df

def dados_residuos(filepath):
    df = pd.read_csv(filepath, sep=',', encoding='utf-8', usecols=['Codigo do municipio', 'Ano', 'IN015'], decimal=',')
    df = df.rename(columns={'IN015': 'Coleta_Lixo'})
    df['Coleta_Lixo'] = pd.to_numeric(df['Coleta_Lixo'], errors='coerce')
    return df

def dados_pluvial(filepath):
    df = pd.read_csv(filepath, sep=',', encoding='utf-8', usecols=['Codigo do municipio', 'Ano', 'IN026'], decimal=',')
    df = df.rename(columns={'IN026': 'Drenagem'})
    df['Drenagem'] = pd.to_numeric(df['Drenagem'], errors='coerce')
    return df

total_dengue = total_casos(dengue_202x, 'Dengue')
total_chiku = total_casos(chiku_202x, 'Chiku')
agua_esgoto = dados_agua_esgoto(agua_esgoto)
residuos = dados_residuos(residuos_solid)
pluvial = dados_pluvial(agua_pluv)

casos_dengue = total_dengue.pivot_table(values='Total', index=['Codigo do municipio', 'Ano'],columns='Tipo_Doenca', aggfunc='sum', fill_value=0).reset_index()
casos_chiku = total_chiku.pivot_table(values='Total', index=['Codigo do municipio', 'Ano'],columns='Tipo_Doenca', aggfunc='sum', fill_value=0).reset_index()

df_final = agua_esgoto.copy() 
df_final = pd.merge(df_final, residuos, on=['Codigo do municipio', 'Ano'], how='left') 
df_final = pd.merge(df_final, pluvial, on=['Codigo do municipio', 'Ano'], how='left') 
df_final = pd.merge(df_final, casos_dengue, on=['Codigo do municipio', 'Ano'], how='left') 
df_final = pd.merge(df_final, casos_chiku, on=['Codigo do municipio', 'Ano'], how='left')


for col in ['Agua', 'Esgoto', 'Coleta_Lixo', 'Drenagem']:
    df_final[col] = df_final[col].fillna(0).round(2)

df_final['Dengue'] = df_final['Dengue'].fillna(0).astype(int)
df_final['Chiku'] = df_final['Chiku'].fillna(0).astype(int)
df_final['Total'] = df_final['Dengue'] + df_final['Chiku']

df_final['Deficit_Agua'] = 100 - df_final['Agua']
df_final['Deficit_Esgoto'] = 100 - df_final['Esgoto']
df_final['Deficit_Coleta_Lixo'] = 100 - df_final['Coleta_Lixo']
df_final['Deficit_Drenagem'] = 100 - df_final['Drenagem']

df_final['Pontuacao_Deficit_Saneamento'] = (
    df_final[['Deficit_Agua', 'Deficit_Esgoto', 'Deficit_Coleta_Lixo', 'Deficit_Drenagem']].mean(axis=1)
).round(2)

colunas_para_mostrar = ['Ano', 'Municipio', 'UF', 'Pontuacao_Deficit_Saneamento', 'Dengue', 'Chiku', 'Total']

ranking_filtrado = df_final.query('Dengue > 0 and Chiku > 0 and Pontuacao_Deficit_Saneamento < 100')
ranking_deficit = ranking_filtrado.sort_values(['Ano', 'Pontuacao_Deficit_Saneamento'], ascending=[False, False])
ranking_casos = df_final.sort_values(['Ano', 'Total'], ascending=[False, False])

print("\nTop 20 Piores - 2020")
print(ranking_deficit[ranking_deficit['Ano'] == 2020][colunas_para_mostrar].head(20).to_string(index=False))

print("\nTop 20 Piores - 2021")
print(ranking_deficit[ranking_deficit['Ano'] == 2021][colunas_para_mostrar].head(20).to_string(index=False))

print("\nTop 10 Mais Casos - 2020")
print(ranking_casos[ranking_casos['Ano'] == 2020][colunas_para_mostrar].head(10).to_string(index=False))

print("\nTop 10 Mais Casos - 2021")
print(ranking_casos[ranking_casos['Ano'] == 2021][colunas_para_mostrar].head(10).to_string(index=False))
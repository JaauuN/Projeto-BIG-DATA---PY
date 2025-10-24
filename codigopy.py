import pandas as pd
import os 

# https://tratabrasil.org.br/dengue-e-saneamento-especialista-do-trata-brasil-alerta-que-a-ausencia-do-basico-favorece-a-proliferacao-do-mosquito/
#(Caminhos do projeto)
snis_202x = 'dados/Esgoto e Água - (2020 - 2021).csv'
dengue_202x = 'dados/Dengue/DENGUE - 2020-2021.csv'
chiku_202x = 'dados/Chiku/CHIKU - 2020-2021.csv'

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
    return df_final

# --- 3. FUNÇÃO PARA LER DADOS DO SNIS  ---
def dados_snis(filepath):
    colunas_snis = ['Codigo do municipio', 'Municipio', 'Ano', 'UF', 'IN055', 'IN056']

    df = pd.read_csv(filepath, sep=',', encoding='utf-8', usecols=colunas_snis, decimal='.')
    df.columns = df.columns.str.strip()
    
    df = df.rename(columns={'IN055': 'Indice_Agua', 'IN056': 'Indice_Esgoto'})
    
    df['Indice_Agua'] = df['Indice_Agua'].astype(str).str.replace(',', '.', regex=False)
    df['Indice_Esgoto'] = df['Indice_Esgoto'].astype(str).str.replace(',', '.', regex=False)
    df['Indice_Agua'] = pd.to_numeric(df['Indice_Agua'], errors='coerce')
    df['Indice_Esgoto'] = pd.to_numeric(df['Indice_Esgoto'], errors='coerce')
    
    return df

# --- 3.5. FUNÇÃO DE ANÁLISE (MODIFICADA PARA TABELA ÚNICA) ---
def analisar_top_10_casos_totais(df_completo):

    anos_para_analise = [2020, 2021]
    
    for ano in anos_para_analise:
        print(f"\n(Top 10 Casos Totais em {ano})")
        
        df_ano = df_completo[df_completo['Ano'] == ano]
        top_10_total = df_ano.sort_values(by='Total', ascending=False).head(10)
        
        colunas = ['Municipio', 'UF', 'Total', 'Dengue', 'Chiku', 'Indice_Esgoto']
        print(top_10_total[colunas].to_string(index=False))


total_dengue = total_casos(dengue_202x, 'Dengue')
total_chiku = total_casos(chiku_202x, 'Chiku')

todas_doencas = pd.concat([total_dengue, total_chiku])

snis = dados_snis(snis_202x)

# --- 5. PROCESSAMENTO E UNIÃO ---

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
    
rank_piores = pd.merge(snis, casos_dengue, on=['Codigo do municipio', 'Ano'], how='inner')
rank_piores = pd.merge(rank_piores, casos_chiku, on=['Codigo do municipio', 'Ano'], how='inner')

rank_piores['Dengue'] = rank_piores['Dengue'].fillna(0).astype(int)
rank_piores['Chiku'] = rank_piores['Chiku'].fillna(0).astype(int)
rank_piores['Total'] = rank_piores['Dengue'] + rank_piores['Chiku']

rank_piores.dropna(subset=['Indice_Esgoto'], inplace=True)

ranking_saneamento = rank_piores.sort_values(by=['Ano', 'Indice_Esgoto'], ascending=[False, True])

# --- RANKING DE SANEAMENTO (Mantido) ---
colunas_ranking = ['Ano', 'Municipio', 'UF', 'Indice_Esgoto', 'Dengue', 'Chiku', 'Total']

print("\nRanking dos 20 Piores Municípios - 2020")
ranking_2020 = ranking_saneamento[
    (ranking_saneamento['Ano'] == 2020) &
    (ranking_saneamento['Indice_Esgoto'])
].head(20)
colunas_ranking_presentes = [col for col in colunas_ranking if col in ranking_2020.columns]
print(ranking_2020[colunas_ranking_presentes].to_string(index=False))

print("\nRanking dos 20 Piores Municípios - 2021")
ranking_2021 = ranking_saneamento[
    (ranking_saneamento['Ano'] == 2021) & 
    (ranking_saneamento['Indice_Esgoto'])
].head(20)
colunas_ranking_presentes = [col for col in colunas_ranking if col in ranking_2021.columns]
print(ranking_2021[colunas_ranking_presentes].to_string(index=False))

# --- 7. CHAMADA DA NOVA FUNÇÃO (TABELA ÚNICA) ---
analisar_top_10_casos_totais(rank_piores)
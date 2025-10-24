import pandas as pd
import os 

# https://tratabrasil.org.br/dengue-e-saneamento-especialista-do-trata-brasil-alerta-que-a-ausencia-do-basico-favorece-a-proliferacao-do-mosquito/
#(Caminhos do projeto)
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
    print(f"Lido casos: {filepath.split('/')[-1]}")
    return df_final

# --- 3. FUNÇÃO PARA LER DADOS DO SNIS ---
def dados_agua_esgoto(filepath):
    colunas_snis = ['Codigo do municipio', 'Municipio', 'Ano', 'UF', 'IN055', 'IN056']

    df = pd.read_csv(filepath, sep=',', encoding='utf-8', usecols=colunas_snis, decimal='.')
    
    df.columns = df.columns.str.strip()
    df = df.rename(columns={'IN055': 'Agua', 'IN056': 'Esgoto'})

    df['Agua'] = pd.to_numeric(df['Agua'])
    df['Esgoto'] = pd.to_numeric(df['Esgoto'])
    print(f"Lido SNIS: {filepath.split('/')[-1]}")
    return df


def  dados_residuos(filepath):
    colunas_residuos = ['Codigo do municipio', 'Ano', 'IN015']
  
    df = pd.read_csv(filepath, sep=',', encoding='utf-8', usecols=colunas_residuos, decimal=',')
    
    df = df.rename(columns={'IN015': 'Coleta_Lixo'})
    df['Coleta_Lixo'] = pd.to_numeric(df['Coleta_Lixo'])
    print(f"Lido SNIS RSU: {filepath.split('/')[-1]}")
    return df[['Codigo do municipio', 'Ano', 'Coleta_Lixo']]

def dados_pluvial(filepath):
    colunas_pluvial = ['Codigo do municipio', 'Ano', 'IN026']

    df = pd.read_csv(filepath, sep=',', encoding='utf-8', usecols=colunas_pluvial , decimal=',')
    
    df = df.rename(columns={'IN026': 'Drenagem'})
    df['Drenagem'] = pd.to_numeric(df['Drenagem'])
    print(f"Lido SNIS AP: {filepath.split('/')[-1]}")
    return df[['Codigo do municipio', 'Ano', 'Drenagem']]

def dados_populacao(filepath):
    colunas_pop = ['Municipio', 'Populacao']

    df = pd.read_csv(filepath, sep=',', encoding='latin1', usecols=colunas_pop, decimal='.')
  

    df['Populacao'] = pd.to_numeric(df['Populacao'])
    # Remover duplicatas de código de município
    df = df.drop_duplicates(subset=['Municipio'], keep='first')
    print(f"Lido População: {filepath.split('/')[-1]}")
    return df

# --- 3.5. FUNÇÃO DE ANÁLISE (Mantida) ---
def analisar_top_10_casos_totais(df_completo):
    anos = [2020, 2021]
    print("\n(Top 10 Municípios por Casos Totais)")
    for ano in anos:
        print(f"\n(Ano: {ano})")
        df_ano = df_completo[df_completo['Ano'] == ano].copy() # Usar .copy() para evitar SettingWithCopyWarning
        # Calcular Incidência se População existir
        if 'Populacao' in df_ano.columns and df_ano['Populacao'].notna().any() and (df_ano['Populacao'] > 0).any():
             df_ano['Incidencia_100k'] = ((df_ano['Total'] / df_ano['Populacao']) * 100000).round(2)
             colunas = ['Municipio', 'UF', 'Total', 'Dengue', 'Chiku', 'Esgoto', 'Populacao','Incidencia_100k']
        else:
             colunas = ['Municipio', 'UF', 'Total', 'Dengue', 'Chiku', 'Esgoto']

        top_10_total = df_ano.sort_values(by='Total', ascending=False).head(10)
        print(top_10_total[colunas].to_string(index=False))


# --- 4. EXECUÇÃO PRINCIPAL ---
print("\n--- Iniciando Carga Geral de Dados ---")
total_dengue = total_casos(dengue_202x, 'Dengue')
total_chiku = total_casos(chiku_202x, 'Chiku')
agua_esgoto = dados_agua_esgoto(agua_esgoto) # Carrega AE (contém Município e UF)
residuos = dados_residuos(residuos_solid)
pluvial = dados_pluvial(agua_pluv) # Usando o nome fornecido pelo usuário
populacao = dados_populacao(populacao_total)
print("--- Carga Geral Concluída ---")


# --- 5. PROCESSAMENTO E UNIÃO ---
print("\n--- Iniciando Processamento e União ---")
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

# Merge principal - Começa com SNIS AE (que tem nomes/UFs)
rank_piores = agua_esgoto.copy()

# Adiciona Resíduos (RSU)
rank_piores = pd.merge(rank_piores, residuos, on=['Codigo do municipio', 'Ano'], how='left')
# Adiciona Pluvial (AP)
rank_piores = pd.merge(rank_piores, pluvial, on=['Codigo do municipio', 'Ano'], how='left')
# Adiciona População (Nota: População não tem Ano, o merge replica o valor para 2020 e 2021)

# Adiciona Casos de Dengue e Chiku (usando INNER para manter a lógica original)
rank_piores = pd.merge(rank_piores, casos_dengue, on=['Codigo do municipio', 'Ano'], how='inner')
rank_piores = pd.merge(rank_piores, casos_chiku, on=['Codigo do municipio', 'Ano'], how='inner')
print("--- Merges Concluídos ---")


# --- Tratamento Pós-Merge ---
indicadores_todos = ['Agua', 'Esgoto', 'Coleta_Lixo', 'Drenagem']
print(f"Tratando Ausentes (NaN -> 0) para indicadores: {indicadores_todos}")
for col in indicadores_todos:
    if col in rank_piores.columns:
        original_nan_count = rank_piores[col].isna().sum()
        rank_piores[col] = rank_piores[col].fillna(0)
        #print(f"  Coluna '{col}': {original_nan_count} NaNs preenchidos com 0.")
    else:
        print(f"  ATENÇÃO: Coluna '{col}' não encontrada após merge. Será criada com valor 0.")
        rank_piores[col] = 0

rank_piores['Dengue'] = rank_piores['Dengue'].fillna(0).astype(int)
rank_piores['Chiku'] = rank_piores['Chiku'].fillna(0).astype(int)
rank_piores['Total'] = rank_piores['Dengue'] + rank_piores['Chiku']

# Remove linhas onde o Índice de Esgoto original era NaN (mantendo lógica anterior)
# E também onde a População é NaN ou 0 (necessário para cálculo de déficit e incidência)
original_rows = len(rank_piores)
rank_piores.dropna(subset=['Esgoto'], inplace=True) # Mantido da lógica original
rank_piores.dropna(subset=['POPULACAO TOTAL'], inplace=True)
rank_piores = rank_piores[rank_piores['Populacao'] > 0]
print(f"Removidas {original_rows - len(rank_piores)} linhas por Esgoto NaN ou População inválida.")


# --- Calcular Pontuação de Déficit ---
print("Calculando Pontuação de Déficit de Saneamento...")
rank_piores['Deficit_Agua'] = 100 - rank_piores['Agua']
rank_piores['Deficit_Esgoto'] = 100 - rank_piores['Esgoto']
rank_piores['Deficit_Coleta_Lixo'] = 100 - rank_piores['Coleta_Lixo']
rank_piores['Deficit_Drenagem'] = 100 - rank_piores['Drenagem']

rank_piores['Pontuacao_Deficit_Saneamento'] = (
    rank_piores['Deficit_Agua'] +
    rank_piores['Deficit_Esgoto'] +
    rank_piores['Deficit_Coleta_Lixo'] +
    rank_piores['Deficit_Drenagem']
) / 4

# Arredondar para melhor visualização
rank_piores['Pontuacao_Deficit_Saneamento'] = rank_piores['Pontuacao_Deficit_Saneamento'].round(2)
indicadores_arredondar = ['Agua', 'Esgoto', 'Coleta_Lixo', 'Drenagem']
for col in indicadores_arredondar:
    rank_piores[col] = rank_piores[col].round(2)
pd.options.display.float_format = '{:.2f}'.format # Formatar display do pandas


# --- Ranking por Déficit de Saneamento (NOVO) ---
print("\n" + "="*60)
print("--- Ranking de Déficit Médio de Saneamento (4 Pilares) ---")
print("    Quanto maior a pontuação, pior o saneamento médio.")
print("="*60)
ranking_deficit = rank_piores.sort_values(by=['Ano', 'Pontuacao_Deficit_Saneamento'], ascending=[False, False])
colunas_deficit = ['Ano', 'Municipio', 'UF', 'Pontuacao_Deficit_Saneamento','Agua', 'Esgoto', 'Coleta_Lixo', 'Drenagem']

print("\nTop 20 Piores - 2021")
print(ranking_deficit[ranking_deficit['Ano'] == 2021][colunas_deficit].head(20).to_string(index=False))

print("\nTop 20 Piores - 2020")
print(ranking_deficit[ranking_deficit['Ano'] == 2020][colunas_deficit].head(20).to_string(index=False))


# --- Ranking por Esgoto (Mantido da lógica original) ---
# A variável 'ranking_saneamento' agora se refere à ordenação por esgoto
ranking_saneamento = rank_piores.sort_values(by=['Ano', 'Esgoto'], ascending=[False, True])
colunas_ranking = ['Ano', 'Municipio', 'UF', 'Esgoto', 'Dengue', 'Chiku', 'Total']

print("\n" + "="*60)
print("--- Ranking dos Piores Municípios por Índice de Esgoto (IN056) ---")
print("    (Considerando apenas municípios presentes nos dados de doenças)")
print("="*60)

print("\nTop 20 Piores - 2021 (Índice Esgoto > 0)") # Adicionado filtro > 0 que estava implícito antes
ranking_2021_esgoto = ranking_saneamento[
    (ranking_saneamento['Ano'] == 2021) &
    (ranking_saneamento['Esgoto'] > 0) # Tornando explícito o filtro > 0
].head(20)
if not ranking_2021_esgoto.empty:
    colunas_presentes_21 = [col for col in colunas_ranking if col in ranking_2021_esgoto.columns]
    print(ranking_2021_esgoto[colunas_presentes_21].to_string(index=False))
else:
    print("Não há dados para 2021 com Índice de Esgoto > 0.")


print("\nTop 20 Piores - 2020 (Índice Esgoto > 0)") # Adicionado filtro > 0
ranking_2020_esgoto = ranking_saneamento[
    (ranking_saneamento['Ano'] == 2020) &
    (ranking_saneamento['Esgoto'] > 0) # Tornando explícito o filtro > 0
].head(20)
if not ranking_2020_esgoto.empty:
    colunas_presentes_20 = [col for col in colunas_ranking if col in ranking_2020_esgoto.columns]
    print(ranking_2020_esgoto[colunas_presentes_20].to_string(index=False))
else:
    print("Não há dados para 2020 com Índice de Esgoto > 0.")


# --- Análise de Casos Totais (Mantida) ---
analisar_top_10_casos_totais(rank_piores) # Passa o dataframe final que agora inclui população

print("\n--- Processamento Concluído ---")
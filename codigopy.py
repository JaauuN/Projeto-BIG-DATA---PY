import pandas as pd


snis_2020 = 'dados/SNIS - 2020.csv'
snis_2021 = 'dados/SNIS - 2021.csv'
dengue_202x = 'dados/Dengue (2020 - 2021)/DENGUE - (2020-2021).csv'
chiku_202x = 'dados/Chiku (2020 - 2021)/CHIKU - (2020-2021).csv'

def total_casos(filepath, ano, doenca):
    try:
        df = pd.read_csv(filepath, sep=',', encoding='latin1', na_values='-')
        df = df[['Código do município', 'Total']]

        df['Total'] = df['Total'].astype(str).str.replace('.', '', regex=False)
        df['Total'] = pd.to_numeric(df['Total'], errors='coerce').fillna(0)
        
        df['Ano'] = ano
        df['Tipo_Doenca'] = doenca
        return df
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado: {filepath}")
        return pd.DataFrame()

def dados_snis(filepath, ano):
    try:
        colunas_snis = [
            'Código do município', 
            'Município', 
            'UF', 
            'IN055', 
            'IN056'
        ]
        df = pd.read_csv(filepath, sep=',', encoding='latin1', usecols=colunas_snis, decimal='.')
        df = df.rename(columns={
            'IN055': 'Indice_Agua',
            'IN056': 'Indice_Esgoto'
        })

        df['Indice_Agua'] = pd.to_numeric(df['Indice_Agua'], errors='coerce')
        df['Indice_Esgoto'] = pd.to_numeric(df['Indice_Esgoto'], errors='coerce')
        
        df['Ano'] = ano
        return df
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado: {filepath}")
        return pd.DataFrame()
    
print("--- Calculando Totais de Casos (SINAN) ---")
total_dengue_2020 = total_casos(dengue_2020, 2020, 'Dengue')
total_dengue_2021 = total_casos(dengue_2021, 2021, 'Dengue')
total_chiku_2020 = total_casos(chiku_2020, 2020, 'Chikungunya')
total_chiku_2021 = total_casos(chiku_2021, 2021, 'Chikungunya')
print("-" * 45)

todas_doencas = pd.concat([total_dengue_2020, total_dengue_2021, total_chiku_2020, total_chiku_2021])

print("\n--- Processando Dados de Saneamento (SNIS) ---")
snis_2020 = dados_snis(snis_2020, 2020)
snis_2021 = dados_snis(snis_2021, 2021)

snis_total = pd.concat([snis_2020, snis_2021])


if not snis_total.empty and not todas_doencas.empty:
    print("\n--- Processando e Unindo Dados ---")
    
    # 1. Agrega os casos (Soma Dengue + Chiku) por município e ano
    df_casos_agregados = todas_doencas.groupby(['Código do município', 'Ano'])['Total'].sum().reset_index()
    

    # 2. Junta (merge) os dados do SNIS com os casos agregados
    # Usa 'left' merge para manter todos os municípios do SNIS
    df_mestre = pd.merge(snis_total, df_casos_agregados, on=['Código do município', 'Ano'], how='left')
    
    # 3. Limpa o resultado
    # Converte 'NaN' (municípios sem casos) para 0
    df_mestre['Total'] = df_mestre['Total'].fillna(0).astype(int)
    # Remove municípios onde o dado de esgoto não foi informado (NaN)
    df_mestre.dropna(subset=['Indice_Esgoto'], inplace=True)
    
    # 4. Cria o Ranking (Ordena por Ano, depois por Pior Esgoto)
    df_ranking = df_mestre.sort_values(by=['Ano', 'Indice_Esgoto'], ascending=[False, True])
    
    # --- RANKING ATUALIZADO ---
    print("\n--- Ranking dos 20 Piores Municípios em Saneamento (Esgoto) - 2021 ---")
    ranking_2021 = df_ranking[df_ranking['Ano'] == 2021].head(20)
    # Adiciona a coluna 'Casos_Totais' ao print
    print(ranking_2021[['Ano', 'Município', 'UF', 'Indice_Esgoto', 'Total']].to_string(index=False))

    print("\n--- Ranking dos 20 Piores Municípios em Saneamento (Esgoto) - 2020 ---")
    ranking_2020 = df_ranking[df_ranking['Ano'] == 2020].head(20)
    # Adiciona a coluna 'Casos_Totais' ao print
    print(ranking_2020[['Ano', 'Município', 'UF', 'Indice_Esgoto', 'Total']].to_string(index=False))
else:
    print("Não foi possível gerar o ranking final pois os arquivos SNIS ou de Doenças não foram carregados.")

print("\n--- Análise Inicial Concluída ---")
import pandas as pd
import os # Importado para checagem de arquivos

# --- 1. DEFINIÇÃO DOS CAMINHOS (Exatamente como no seu script) ---
snis_202x = 'dados/SNIS - (2020 - 2021).csv'
dengue_202x = 'dados/Dengue/DENGUE - (2020-2021).csv'
chiku_202x = 'dados/Chiku/CHIKU - (2020-2021).csv'

# --- 2. FUNÇÃO PARA LER CASOS (Corrigida) ---
# O parâmetro 'ano' foi removido, pois o arquivo .csv já deve conter a coluna 'Ano'
def total_casos(filepath, doenca):     
    try:
        df = pd.read_csv(filepath, sep=',', encoding='latin1', na_values='-')
        
        colunas_casos = [
            'Codigo do municipio', 
            'Total', 
            'Ano'
        ]
        if not all(col in df.columns for col in colunas_casos):
            print(f"ERRO: O arquivo {filepath} não contém as colunas necessárias: {colunas_casos}")
            print(f"Colunas encontradas: {list(df.columns)}")
            return pd.DataFrame()

        df = df[colunas_casos]
        # Lógica de limpeza mantida do seu script
        df['Total'] = df['Total'].astype(str).str.replace('.', '', regex=False)
        df['Total'] = pd.to_numeric(df['Total'], errors='coerce').fillna(0)
        
        df['Tipo_Doenca'] = doenca
        print(f"Lido com sucesso: {filepath}")
        return df
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado: {filepath}")
        return pd.DataFrame()
    except Exception as e:
        print(f"ERRO ao ler {filepath}: {e}. Verifique o separador (sep=','),")
        return pd.DataFrame()


# --- 3. FUNÇÃO PARA LER DADOS DO SNIS (Mantida 100% como no seu script) ---
def dados_snis(filepath):
    try:
        colunas_snis = [
            'Codigo do municipio', 
            'Municipio', 
            'Ano',
            'UF', 
            'IN055', 
            'IN056',
        ]
        df = pd.read_csv(filepath, sep=',', encoding='utf-8', usecols=colunas_snis, decimal='.')
        df = df.rename(columns={
            'IN055': 'Indice_Agua',
            'IN056': 'Indice_Esgoto'
        })
        df['Indice_Agua'] = pd.to_numeric(df['Indice_Agua'], errors='coerce')
        df['Indice_Esgoto'] = pd.to_numeric(df['Indice_Esgoto'], errors='coerce')
        
        print(f"Lido com sucesso: {filepath}")
        return df
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado: {filepath}")
        return pd.DataFrame()
    except Exception as e:
        print(f"ERRO ao ler {filepath}: {e}. Verifique o separador (sep=',') e decimal ('.').")
        return pd.DataFrame()

# --- 4. EXECUÇÃO PRINCIPAL (Corrigida) ---
    
print("--- Calculando Totais de Casos (SINAN) ---")
# Bloco corrigido para chamar as variáveis corretas (dengue_202x, chiku_202x)
# e usar a função 'total_casos' corrigida (sem o parâmetro de ano)
total_dengue = total_casos(dengue_202x, 'Dengue')
total_chiku = total_casos(chiku_202x, 'Chikungunya')
print("-" * 45)

# Bloco corrigido para concatenar os 2 dataframes criados
todas_doencas = pd.concat([total_dengue, total_chiku])

print("\n--- Processando Dados de Saneamento (SNIS) ---")
# Esta parte do seu script já estava correta
snis = dados_snis(snis_202x)

# --- 5. RESTANTE DO SCRIPT (Mantido 100% como no seu script) ---
if not snis.empty and not todas_doencas.empty:
    print("\n--- Processando e Unindo Dados ---")
    
    # 1. Agrega os casos (Soma Dengue + Chiku) por município e ano
    df_casos_agregados = todas_doencas.groupby(['Codigo do municipio', 'Ano'])['Total'].sum().reset_index()
    
    # 2. Junta (merge) os dados do SNIS com os casos agregados
    df_mestre = pd.merge(snis, df_casos_agregados, on=['Codigo do municipio', 'Ano'], how='left')
    
    # 3. Limpa o resultado
    df_mestre['Total'] = df_mestre['Total'].fillna(0).astype(int)
    # Remove municípios onde o dado de esgoto não foi informado (NaN)
    df_mestre.dropna(subset=['Indice_Esgoto'], inplace=True)
    
    # 4. Cria o Ranking (Ordena por Ano, depois por Pior Esgoto)
    df_ranking = df_mestre.sort_values(by=['Ano', 'Indice_Esgoto'], ascending=[False, True])
    
    # --- RANKING ATUALIZADO ---
    print("\n--- Ranking dos 20 Piores Municípios em Saneamento (Esgoto) - 2021 ---")
    ranking_2021 = df_ranking[df_ranking['Ano'] == 2021].head(20)
    # Adiciona a coluna 'Casos_Totais' ao print
    print(ranking_2021[['Ano', 'Municipio', 'UF', 'Indice_Esgoto', 'Total']].to_string(index=False))

    print("\n--- Ranking dos 20 Piores Municípios em Saneamento (Esgoto) - 2020 ---")
    ranking_2020 = df_ranking[df_ranking['Ano'] == 2020].head(20)
    # Adiciona a coluna 'Casos_Totais' ao print
    print(ranking_2020[['Ano', 'Municipio', 'UF', 'Indice_Esgoto', 'Total']].to_string(index=False))
else:
    print("Não foi possível gerar o ranking final pois os arquivos SNIS ou de Doenças não foram carregados.")

print("\n--- Análise Inicial Concluída ---")
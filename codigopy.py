import pandas as pd
import os # Importado para checagem de arquivos

# --- 1. DEFINIÇÃO DOS CAMINHOS (Mantido como no seu script) ---
# (Nota: O script executado usou os caminhos completos do ambiente, 
# mas seu script original com caminhos relativos 'dados/...' deve funcionar
# se executado do diretório raiz do projeto)

snis_202x = 'dados/SNIS - (2020 - 2021).csv'
dengue_202x = 'dados/Dengue/DENGUE - (2020-2021).csv'
chiku_202x = 'dados/Chiku/CHIKU - (2020-2021).csv'

# --- 2. FUNÇÃO PARA LER CASOS (Exatamente como você forneceu) ---
def total_casos(filepath, doenca):     
    try:
        df = pd.read_csv(filepath, sep=',', encoding='latin1', na_values='-')
        
        # Limpar nomes das colunas
        df.columns = df.columns.str.strip().str.replace(',', '', regex=False)

        colunas_casos = [
            'Codigo do municipio', 
            'Total', 
            'Ano'
        ]
        
        if not all(col in df.columns for col in colunas_casos):
            print(f"ERRO: O arquivo {filepath} não contém as colunas necessárias.")
            print(f"Colunas esperadas: {colunas_casos}")
            print(f"Colunas encontradas: {list(df.columns)}")
            return pd.DataFrame()

        df = df[colunas_casos]
        
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


# --- 3. FUNÇÃO PARA LER DADOS DO SNIS (Exatamente como você forneceu) ---
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
        
        df.columns = df.columns.str.strip()
        
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

# --- 4. EXECUÇÃO PRINCIPAL (Como você forneceu) ---
    
print("--- Calculando Totais de Casos (SINAN) ---")
total_dengue = total_casos(dengue_202x, 'Dengue')
total_chiku = total_casos(chiku_202x, 'Chikungunya')
print("-" * 45)

todas_doencas = pd.concat([total_dengue, total_chiku])

print("\n--- Processando Dados de Saneamento (SNIS) ---")
snis = dados_snis(snis_202x)

# --- 5. PROCESSAMENTO E UNIÃO (Modificado para incluir colunas separadas) ---
if not snis.empty and not todas_doencas.empty:
    print("\n--- Processando e Unindo Dados ---")
    
    # 1. *** MODIFICAÇÃO INICIA AQUI ***
    # Pivota os dados para ter colunas separadas para 'Dengue' e 'Chikungunya'
    df_casos_pivot = pd.pivot_table(todas_doencas,
                                     values='Total',
                                     index=['Codigo do municipio', 'Ano'],
                                     columns='Tipo_Doenca',
                                     aggfunc='sum',
                                     fill_value=0).reset_index()
    
    # Renomeia colunas para o resultado final
    df_casos_pivot.rename(columns={'Dengue': 'Casos_Dengue', 'Chikungunya': 'Casos_Chikungunya'}, inplace=True)
    
    # 2. Cria a coluna Total (soma das duas doenças)
    df_casos_pivot['Total'] = df_casos_pivot['Casos_Dengue'] + df_casos_pivot['Casos_Chikungunya']
    # *** MODIFICAÇÃO TERMINA AQUI ***
    
    # 3. Junta (merge) os dados do SNIS com os casos (agora separados e com total)
    df_mestre = pd.merge(snis, df_casos_pivot, on=['Codigo do municipio', 'Ano'], how='left')
    
    # 4. Limpa o resultado (preenche NaNs para municípios do SNIS sem casos)
    # (Adicionado preenchimento para as novas colunas)
    df_mestre['Casos_Dengue'] = df_mestre['Casos_Dengue'].fillna(0).astype(int)
    df_mestre['Casos_Chikungunya'] = df_mestre['Casos_Chikungunya'].fillna(0).astype(int)
    df_mestre['Total'] = df_mestre['Total'].fillna(0).astype(int)
    
    # Remove municípios onde o dado de esgoto não foi informado (NaN)
    df_mestre.dropna(subset=['Indice_Esgoto'], inplace=True)
    
    # 5. Cria o Ranking (Ordena por Ano, depois por Pior Esgoto)
    df_ranking = df_mestre.sort_values(by=['Ano', 'Indice_Esgoto'], ascending=[False, True])
    
    # --- RANKING ATUALIZADO (Modificado para mostrar as novas colunas) ---
    print("\n--- Ranking dos 20 Piores Municípios em Saneamento (Esgoto) - 2021 ---")
    ranking_2021 = df_ranking[df_ranking['Ano'] == 2021].head(20)
    # Adiciona as colunas 'Casos_Dengue' e 'Casos_Chikungunya' ao print
    colunas_ranking = ['Ano', 'Municipio', 'UF', 'Indice_Esgoto', 'Casos_Dengue', 'Casos_Chikungunya', 'Total']
    print(ranking_2021[colunas_ranking].to_string(index=False))

    print("\n--- Ranking dos 20 Piores Municípios em Saneamento (Esgoto) - 2020 ---")
    ranking_2020 = df_ranking[df_ranking['Ano'] == 2020].head(20)
    # Adiciona as colunas 'Casos_Dengue' e 'Casos_Chikungunya' ao print
    print(ranking_2020[colunas_ranking].to_string(index=False))
else:
    print("Não foi possível gerar o ranking final pois os arquivos SNIS ou de Doenças não foram carregados.")

print("\n--- Análise (com colunas separadas) Concluída ---")
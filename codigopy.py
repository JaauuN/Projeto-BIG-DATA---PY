import pandas as pd
import os # Importado para checagem de arquivos

# --- 1. DEFINIÇÃO DOS CAMINHOS (Mantido como no seu script) ---
# (Nota: O script executado usou os caminhos completos do ambiente, 
# mas seu script original com caminhos relativos 'dados/...' deve funcionar
# se executado do diretório raiz do projeto)

snis_202x = 'dados/SNIS - (2020 - 2021).csv'
dengue_202x = 'dados/Dengue/DENGUE - (2020-2021).csv'
chiku_202x = 'dados/Chiku/CHIKU - (2020-2021).csv'

def total_casos(filepath, doenca):     
    try:
        # Tenta ler com latin1
        try:
            df = pd.read_csv(filepath, sep=',', encoding='latin1', na_values='-')
        except UnicodeDecodeError:
             # Se falhar, tenta com utf-8
            print(f"Falha ao ler {filepath} com latin1, tentando utf-8...")
            df = pd.read_csv(filepath, sep=',', encoding='utf-8', na_values='-')

        
        # Limpar nomes das colunas
        df.columns = df.columns.str.strip().str.replace(',', '', regex=False)

        colunas_casos = [
            'Codigo do municipio', 
            'Total', 
            'Ano'
        ]
        
        # Checagem de colunas
        colunas_faltando = [col for col in colunas_casos if col not in df.columns]
        if colunas_faltando:
            print(f"ERRO: O arquivo {filepath} não contém as colunas necessárias: {colunas_faltando}")
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
        print("Colunas encontradas:", list(pd.read_csv(filepath, nrows=0).columns))
        return pd.DataFrame()


# --- 3. FUNÇÃO PARA LER DADOS DO SNIS (Exatamente como você forneceu) ---
def dados_snis(filepath):
    try:
        colunas_snis = [
            'Codigo do municipio', 
            'Municipio', 
            'Ano',
            'UF', 
            'IN055', # Índice de atendimento total de água
            'IN056', # Índice de atendimento total de esgoto
        ]
        
        # Tenta ler com utf-8
        try:
            df = pd.read_csv(filepath, sep=',', encoding='utf-8', usecols=colunas_snis, decimal='.')
        except UnicodeDecodeError:
            # Se falhar, tenta com latin1
            print(f"Falha ao ler {filepath} com utf-8, tentando latin1...")
            df = pd.read_csv(filepath, sep=',', encoding='latin1', usecols=colunas_snis, decimal='.')

        
        df.columns = df.columns.str.strip()
        
        df = df.rename(columns={
            'IN055': 'Indice_Agua',
            'IN056': 'Indice_Esgoto'
        })
        
        # Assegura que os índices são numéricos, tratando vírgula como decimal
        df['Indice_Agua'] = df['Indice_Agua'].astype(str).str.replace(',', '.', regex=False)
        df['Indice_Esgoto'] = df['Indice_Esgoto'].astype(str).str.replace(',', '.', regex=False)

        df['Indice_Agua'] = pd.to_numeric(df['Indice_Agua'], errors='coerce')
        df['Indice_Esgoto'] = pd.to_numeric(df['Indice_Esgoto'], errors='coerce')
        
        print(f"Lido com sucesso: {filepath}")
        return df
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado: {filepath}")
        return pd.DataFrame()
    except Exception as e:
        print(f"ERRO ao ler {filepath}: {e}. Verifique o separador (sep=',') e decimal ('.').")
        print("Colunas encontradas:", list(pd.read_csv(filepath, nrows=0).columns))
        return pd.DataFrame()

# --- 3.5. NOVA FUNÇÃO ADICIONADA ---
def analisar_piores_municipios_sem_zero(df_completo):
    """
    Encontra o município com o menor 'Indice_Esgoto' (maior que 0) 
    para 2020 e 2021 e exibe seus casos de Dengue e Chiku.
    """
    print("\n" + "---" * 15)
    print("--- ANÁLISE ADICIONAL: Pior Município (Índice Esgoto > 0) ---")
    print("---" * 15)

    anos_para_analise = [2020, 2021]
    colunas_exibir = ['Municipio', 'UF', 'Indice_Esgoto', 'Dengue', 'Chiku', 'Total']
    
    for ano in anos_para_analise:
        print(f"\n--- Analisando Ano: {ano} ---")
        
        # 1. Filtra por ano E pela CONDIÇÃO (Índice > 0)
        df_filtrado = df_completo[
            (df_completo['Ano'] == ano) & 
            (df_completo['Indice_Esgoto'] > 0)
        ]
        
        if df_filtrado.empty:
            print(f"Não foram encontrados dados para {ano} com 'Indice_Esgoto' > 0.")
            continue
            
        # 2. Encontra o menor índice (que agora é > 0)
        min_indice_esgoto = df_filtrado['Indice_Esgoto'].min()
        
        # 3. Seleciona o município (ou municípios, em caso de empate) com esse índice
        pior_municipio_df = df_filtrado[df_filtrado['Indice_Esgoto'] == min_indice_esgoto]
        
        print(f"Município(s) com o menor 'Indice_Esgoto' (> 0) em {ano} (Índice: {min_indice_esgoto}):")
        
        # Itera caso haja empate (mais de um município com o mesmo índice mínimo)
        for _, row in pior_municipio_df.iterrows():
            print(f"  - Município: {row['Municipio']} ({row['UF']})")
            print(f"    Índice Esgoto: {row['Indice_Esgoto']}")
            print(f"    Casos Dengue:  {row['Dengue']}")
            print(f"    Casos Chiku:   {row['Chiku']}")
            print(f"    Casos Totais:  {row['Total']}")

# --- 4. EXECUÇÃO PRINCIPAL (Como você forneceu) ---
    
print("--- Calculando Totais de Casos (SINAN) ---")
total_dengue = total_casos(dengue_202x, 'Dengue')
total_chiku = total_casos(chiku_202x, 'Chiku')
print("-" * 45)

# (Este 'todas_doencas' não é usado na Seção 5 que você colou, mas mantido)
todas_doencas = pd.concat([total_dengue, total_chiku])

print("\n--- Processando Dados de Saneamento (SNIS) ---")
snis = dados_snis(snis_202x)

# --- 5. PROCESSAMENTO E UNIÃO (CORRIGIDO para funcionar conforme intenção) ---
if not snis.empty and not (total_dengue.empty or total_chiku.empty):
    print("\n--- Processando e Unindo Dados ---")
    
    # 1. Pivota os dados para ter colunas separadas para 'Dengue'
    casos_dengue = pd.pivot_table(total_dengue,
                                  values='Total',
                                  index=['Codigo do municipio', 'Ano'],
                                  columns='Tipo_Doenca', # Cria coluna 'Dengue'
                                  aggfunc='sum',
                                  fill_value=0).reset_index()
    
    # 2. *** CORREÇÃO DO BUG ***
    #    Pivota os dados para ter colunas separadas para 'Chiku'
    #    (A variável foi renomeada para 'casos_chiku' para não sobrescrever 'casos_dengue')
    casos_chiku = pd.pivot_table(total_chiku, # <--- Usando o df correto
                                 values='Total',
                                 index=['Codigo do municipio', 'Ano'],
                                 columns='Tipo_Doenca', # Cria coluna 'Chiku'
                                 aggfunc='sum',
                                 fill_value=0).reset_index()
    

    
    # 3. *** CORREÇÃO DO MERGE ***
    #    Junta (merge) os dados do SNIS com os casos de Dengue
    df_mestre = pd.merge(snis, casos_dengue, on=['Codigo do municipio', 'Ano'], how='left')
    #    Junta (merge) o resultado com os casos de Chiku
    df_mestre = pd.merge(df_mestre, casos_chiku, on=['Codigo do municipio', 'Ano'], how='left')
    
    # 4. Limpa o resultado (preenche NaNs para municípios do SNIS sem casos)
    df_mestre['Dengue'] = df_mestre['Dengue'].fillna(0).astype(int)
    df_mestre['Chiku'] = df_mestre['Chiku'].fillna(0).astype(int)
    
    # 5. *** CORREÇÃO DA COLUNA TOTAL ***
    #    Soma as colunas 'Dengue' e 'Chiku' para criar o 'Total'
    df_mestre['Total'] = df_mestre['Dengue'] + df_mestre['Chiku']
    
    # Remove municípios onde o dado de esgoto não foi informado (NaN)
    df_mestre.dropna(subset=['Indice_Esgoto'], inplace=True)
    
    # 6. Cria o Ranking (Ordena por Ano, depois por Pior Esgoto)
    df_ranking = df_mestre.sort_values(by=['Ano', 'Indice_Esgoto'], ascending=[False, True])
    
    # --- RANKING ATUALIZADO (Como no seu script) ---
    print("\n--- Ranking dos 20 Piores Municípios em Saneamento (Esgoto) - 2021 ---")
    ranking_2021 = df_ranking[df_ranking['Ano'] == 2021].head(20)
    colunas_ranking = ['Ano', 'Municipio', 'UF', 'Indice_Esgoto', 'Dengue', 'Chiku', 'Total']
    
    # Checagem final de colunas antes de imprimir
    colunas_ranking_presentes = [col for col in colunas_ranking if col in ranking_2021.columns]
    print(ranking_2021[colunas_ranking_presentes].to_string(index=False))

    print("\n--- Ranking dos 20 Piores Municípios em Saneamento (Esgoto) - 2020 ---")
    ranking_2020 = df_ranking[df_ranking['Ano'] == 2020].head(20)
    
    colunas_ranking_presentes = [col for col in colunas_ranking if col in ranking_2020.columns]
    print(ranking_2020[colunas_ranking_presentes].to_string(index=False))
    
    # --- 6. CHAMADA DA NOVA FUNÇÃO ADICIONADA ---
    #    Esta função executa a análise específica que você pediu
    analisar_piores_municipios_sem_zero(df_mestre)

else:
    print("\nNão foi possível gerar o ranking final pois os arquivos SNIS ou de Doenças não foram carregados.")

print("\n--- Análise (com colunas separadas e análise adicional) Concluída ---")
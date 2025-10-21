import pandas as pd
from IPython.display import display
import matplotlib.pyplot as plt
import seaborn as sns
import warnings

# Ignora avisos de performance do pandas, que são comuns com 'melt'
warnings.simplefilter(action='ignore', category=FutureWarning)

def carregar_dados_sinan(caminho_arquivo, nome_coluna_casos):
    """
    Lê um arquivo do SINAN (Zika, Dengue, ou Chiku), limpa e transforma
    de formato 'wide' (largo) para 'long' (longo).
    """
    print(f"Processando arquivo: {caminho_arquivo}")
    
    # 1. Leitura: Pula as primeiras 3 linhas do cabeçalho e usa ';' como separador
    df = pd.read_csv(caminho_arquivo, sep=';', encoding='latin1', skiprows=3)
    
    # 2. Limpeza: Remove linhas vazias e a linha 'Total'
    df = df.dropna(how='all')
    df = df[df['Ano notificação'].str.contains('Total') == False]
    
    # 3. Transformação (Melt): Gira a tabela
    df_melted = df.melt(id_vars=['Ano notificação'], 
                        var_name='UF', 
                        value_name=nome_coluna_casos)
    
    # 4. Limpeza final
    # Converte Ano para número
    df_melted['Ano'] = pd.to_numeric(df_melted['Ano notificação'], errors='coerce')
    
    # Remove valores não numéricos (como '-') dos casos
    df_melted[nome_coluna_casos] = df_melted[nome_coluna_casos].str.strip()
    df_melted = df_melted[df_melted[nome_coluna_casos] != '-']
    df_melted[nome_coluna_casos] = pd.to_numeric(df_melted[nome_coluna_casos], errors='coerce')
    
    # Remove linhas que falharam na conversão e seleciona colunas
    df_melted = df_melted.dropna(subset=['Ano', nome_coluna_casos])
    df_melted['Ano'] = df_melted['Ano'].astype(int)
    
    return df_melted[['Ano', 'UF', nome_coluna_casos]]

def carregar_dados_snis(caminho_arquivo):
    """
    Lê o arquivo do SNIS, agrega por Estado e Ano,
    e calcula as taxas de cobertura.
    """
    print(f"Processando arquivo: {caminho_arquivo}")
    
    # 1. Leitura: Usa os parâmetros do seu código e 'on_bad_lines' para pular
    #    as linhas corrompidas (com quebra de linha) no seu CSV
    try:
        df = pd.read_csv(caminho_arquivo, 
                         encoding='utf-16', 
                         sep=',', 
                         on_bad_lines='warn') # 'warn' avisa sobre linhas puladas
    except Exception as e:
        print(f"Erro ao ler SNIS.csv: {e}")
        print("Verifique se o encoding 'utf-16' está correto. Tentando 'latin1'...")
        df = pd.read_csv(caminho_arquivo, 
                         encoding='latin1', 
                         sep=',', 
                         on_bad_lines='warn')

    
    # 2. Seleção de Colunas: Pega só o que precisamos
    colunas_populacao = [
        "G12A - População total residente do(s) município(s) com abastecimento de água, segundo o IBGE",
        "G12B - População total residente do(s) município(s) com esgotamento sanitário, segundo o IBGE",
        "AG001 - População total atendida com abastecimento de água",
        "ES001 - População total atendida com esgotamento sanitário"
    ]
    colunas_chave = ['Estado', 'Ano de Referência']
    
    # Renomear colunas para facilitar
    df = df.rename(columns={
        "G12A - População total residente do(s) município(s) com abastecimento de água, segundo o IBGE": "Pop_Total_Agua",
        "G12B - População total residente do(s) município(s) com esgotamento sanitário, segundo o IBGE": "Pop_Total_Esgoto",
        "AG001 - População total atendida com abastecimento de água": "Pop_Atendida_Agua",
        "ES001 - População total atendida com esgotamento sanitário": "Pop_Atendida_Esgoto"
    })
    
    colunas_renomeadas = ['Pop_Total_Agua', 'Pop_Total_Esgoto', 'Pop_Atendida_Agua', 'Pop_Atendida_Esgoto']

    # 3. Limpeza: Converte colunas de população para número
    for col in colunas_renomeadas:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df = df.dropna(subset=colunas_renomeadas + colunas_chave) # Remove linhas sem dados
    
    # 4. Transformação (Agregação): Agrupa por Estado e Ano e soma tudo
    print("Agregando dados do SNIS por Estado e Ano...")
    df_agg = df.groupby(colunas_chave)[colunas_renomeadas].sum().reset_index()

    # 5. Criação de Indicadores: Calcula as taxas percentuais
    #    (Evita divisão por zero se a população for 0)
    df_agg['Taxa_Cobertura_Agua'] = df_agg.apply(
        lambda row: (row['Pop_Atendida_Agua'] / row['Pop_Total_Agua']) * 100 if row['Pop_Total_Agua'] > 0 else 0,
        axis=1
    )
    df_agg['Taxa_Cobertura_Esgoto'] = df_agg.apply(
        lambda row: (row['Pop_Atendida_Esgoto'] / row['Pop_Total_Esgoto']) * 100 if row['Pop_Total_Esgoto'] > 0 else 0,
        axis=1
    )
    
    # 6. Preparação para Unir: Renomeia colunas para bater com os dados do SINAN
    df_final_snis = df_agg.rename(columns={
        'Estado': 'UF',
        'Ano de Referência': 'Ano'
    })
    
    return df_final_snis[['Ano', 'UF', 'Taxa_Cobertura_Agua', 'Taxa_Cobertura_Esgoto']]

# --- PASSO 1: EXTRAIR E TRANSFORMAR ---

# Carrega e transforma os 3 arquivos de doenças
df_zika = carregar_dados_sinan('dados/ZIKA - SINAN.csv', 'Casos_Zika')
df_dengue = carregar_dados_sinan('dados/DENGUE - SINAN.csv', 'Casos_Dengue')
df_chiku = carregar_dados_sinan('dados/CHIKU - SINAN.csv', 'Casos_Chiku')

# Carrega e transforma o arquivo de saneamento
df_snis = carregar_dados_snis('dados/SNIS V2.csv')

print("\n--- Visualização Pré-Merge ---")
print("Dados de Doenças (Zika):")
display(df_zika.head())
print("\nDados de Saneamento (Agregados por Estado):")
display(df_snis.head())

# --- PASSO 2: UNIR OS DADOS (LOAD) ---

print("\n--- Unindo tabelas ---")

# Une os 3 dataframes de doenças
df_doencas = pd.merge(df_zika, df_dengue, on=['Ano', 'UF'], how='outer')
df_doencas = pd.merge(df_doencas, df_chiku, on=['Ano', 'UF'], how='outer')

# Une os dados de doenças com os de saneamento
# 'inner' garante que só teremos linhas onde há dados de AMBOS os lados
df_final = pd.merge(df_doencas, df_snis, on=['Ano', 'UF'], how='inner')

# Preenche com 0 casos onde a união 'outer' pode ter criado NaNs
df_final = df_final.fillna(0)

print("\n--- TABELA FINAL UNIFICADA ---")
display(df_final)

# --- PASSO 3: ANÁLISE DE CORRELAÇÃO ---

print("\n--- Matriz de Correlação ---")
# Calcula a correlação entre todas as variáveis numéricas
# .corr() só funciona com números, por isso filtramos
colunas_numericas = ['Casos_Zika', 'Casos_Dengue', 'Casos_Chiku', 'Taxa_Cobertura_Agua', 'Taxa_Cobertura_Esgoto']
matriz_correlacao = df_final[colunas_numericas].corr()

display(matriz_correlacao)

print("\n--- Visualização da Correlação ---")

# Plota um "mapa de calor" (heatmap) da correlação
plt.figure(figsize=(10, 7))
sns.heatmap(matriz_correlacao, annot=True, cmap='coolwarm', fmt='.2f')
plt.title('Matriz de Correlação: Saneamento vs. Incidência de Doenças')
plt.show()

# Plota a correlação específica que você quer ver
# (usando regplot para ver a linha de tendência)
print("\n--- Gráfico: Correlação Esgoto vs. Zika ---")
plt.figure(figsize=(10, 6))
sns.regplot(data=df_final, x='Taxa_Cobertura_Esgoto', y='Casos_Zika')
plt.title('Correlação entre Cobertura de Esgoto e Casos de Zika (2020-2022)')
plt.xlabel('Taxa de Cobertura de Esgoto (%) por Estado')
plt.ylabel('Nº de Casos de Zika')
plt.grid(True)
plt.show()
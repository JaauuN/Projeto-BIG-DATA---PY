import pandas as pd
from IPython.display import display
import matplotlib.pyplot as plt
import seaborn as sns
import warnings

# Ignora avisos de performance do pandas
warnings.simplefilter(action='ignore', category=FutureWarning)

def carregar_dados_sinan(caminho_arquivo, nome_coluna_casos):
    """
    (Esta função está correta)
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
    df_melted['Ano'] = pd.to_numeric(df_melted['Ano notificação'], errors='coerce')
    df_melted[nome_coluna_casos] = df_melted[nome_coluna_casos].str.strip()
    df_melted = df_melted[df_melted[nome_coluna_casos] != '-']
    df_melted[nome_coluna_casos] = pd.to_numeric(df_melted[nome_coluna_casos], errors='coerce')
    df_melted = df_melted.dropna(subset=['Ano', nome_coluna_casos])
    df_melted['Ano'] = df_melted['Ano'].astype(int)
    
    return df_melted[['Ano', 'UF', nome_coluna_casos]]

def carregar_e_juntar_snis_csv(caminho_2020, caminho_2021):
    """
    Lê os dois arquivos CSV (2020 e 2021), adiciona o ano,
    renomeia colunas e junta os dois.
    """
    print(f"Processando arquivo SNIS 2020: {caminho_2020}")
    # 1. Leitura do arquivo 2020
    df_2020 = pd.read_csv(caminho_2020, sep=',', encoding='latin1')
    df_2020['Ano'] = 2020 # Adiciona a coluna do ano

    print(f"Processando arquivo SNIS 2021: {caminho_2021}")
    # 2. Leitura do arquivo 2021
    df_2021 = pd.read_csv(caminho_2021, sep=',', encoding='latin1')
    df_2021['Ano'] = 2021 # Adiciona a coluna do ano

    # 3. Junção (Concatenação)
    df_total_snis = pd.concat([df_2020, df_2021], ignore_index=True)

    print("Colunas encontradas nos arquivos SNIS:", df_total_snis.columns.tolist())

    # 4. Renomear Colunas:
    # <-- CORREÇÃO: Adicionamos 'Região': 'UF' que estava faltando
    df_total_snis = df_total_snis.rename(columns={
        'Região': 'UF', # <--- ESTA LINHA CORRIGE O BUG
        'IN055': 'Taxa_Cobertura_Agua',
        'IN056': 'Taxa_Cobertura_Esgoto'
    })

    # 5. Limpeza (Seu código estava correto aqui)
    for col in ['Taxa_Cobertura_Agua', 'Taxa_Cobertura_Esgoto']:
        if df_total_snis[col].dtype == 'object':
            print(f"Limpando coluna {col} (removendo '.' e trocando ',' por '.')")
            df_total_snis[col] = (
                df_total_snis[col]
                .fillna('') 
                .astype(str) 
                .str.replace('.', '', regex=False) 
                .str.replace(',', '.', regex=False) 
            )
            df_total_snis[col] = pd.to_numeric(df_total_snis[col], errors='coerce')

    # 6. Seleção
    try:
        # Agora o 'UF' existe e esta linha vai funcionar
        df_final_snis = df_total_snis[['Ano', 'UF', 'Taxa_Cobertura_Agua', 'Taxa_Cobertura_Esgoto']]
        return df_final_snis
    except KeyError as e:
        print(f"ERRO: A coluna {e} não foi encontrada após renomear.")
        print("Verifique os nomes das colunas nos arquivos CSV.")
        return pd.DataFrame()

# --- PASSO 1: EXTRAIR E TRANSFORMAR ---

# Carrega e transforma os 3 arquivos de doenças
df_zika = carregar_dados_sinan('dados/ZIKA - SINAN.csv', 'Casos_Zika')
df_dengue = carregar_dados_sinan('dados/DENGUE - SINAN.csv', 'Casos_Dengue')
df_chiku = carregar_dados_sinan('dados/CHIKU - SINAN.csv', 'Casos_Chiku')

# Seu código está correto aqui (nomes dos arquivos 2020 e 2021)
caminho_snis_2020 = 'dados/SNIS - 2020.csv'
caminho_snis_2021 = 'dados/SNIS - 2021.csv'
df_snis = carregar_e_juntar_snis_csv(caminho_snis_2020, caminho_snis_2021)

# --- PASSO 2: FILTRAR POR ANO ---
anos_desejados = [2020, 2021]
print(f"\nFiltrando dados das doenças para manter apenas os anos: {anos_desejados}\n")

df_zika = df_zika[df_zika['Ano'].isin(anos_desejados)]
df_dengue = df_dengue[df_dengue['Ano'].isin(anos_desejados)]
df_chiku = df_chiku[df_chiku['Ano'].isin(anos_desejados)]

# --- PASSO 3: UNIR OS DADOS (LOAD) ---

print("--- Unindo tabelas ---")

# Une os 3 dataframes de doenças
df_doencas = pd.merge(df_zika, df_dengue, on=['Ano', 'UF'], how='outer')
df_doencas = pd.merge(df_doencas, df_chiku, on=['Ano', 'UF'], how='outer')

# Une os dados de doenças com os de saneamento
# Esta união ('merge') agora vai funcionar!
df_final = pd.merge(df_doencas, df_snis, on=['Ano', 'UF'], how='inner')
df_final = df_final.fillna(0)

print("\n--- TABELA FINAL UNIFICADA (2020-2021) ---")
display(df_final)

# --- PASSO 4: GRÁFICO DE PERCENTUAL DE CASOS POR ESTADO ---
if not df_final.empty:
    
    print("\n--- Gerando Gráfico: Percentual de Casos Totais por Estado (2020-2021) ---")
    
    # 1. Soma todas as doenças para ter um "índice de doenças"
    df_final['Casos_Totais'] = df_final['Casos_Zika'] + df_final['Casos_Dengue'] + df_final['Casos_Chiku']

    # 2. Agrega os dados por UF (soma os casos de 2020 e 2021)
    df_agregado = df_final.groupby('UF')['Casos_Totais'].sum().reset_index()
    
    # 3. Calcula o total de casos no Brasil (para o percentual)
    total_casos_brasil = df_agregado['Casos_Totais'].sum()
    
    # 4. Cria a coluna de Percentual
    df_agregado['Porcentagem'] = (df_agregado['Casos_Totais'] / total_casos_brasil) * 100
    
    # 5. Ordena o dataframe para o gráfico ficar do "maior para o menor"
    df_agregado = df_agregado.sort_values(by='Porcentagem', ascending=False)
    
    # 6. Cria o gráfico
    plt.figure(figsize=(12, 10)) # Um gráfico grande para caber todos os estados
    sns.barplot(
        data=df_agregado,
        x='Porcentagem',
        y='UF'
    )
    plt.title('Percentual do Total de Casos de Doenças (Zika, Dengue, Chiku) por Estado (2020-2021)')
    plt.xlabel('Percentual do Total de Casos no Brasil (%)')
    plt.ylabel('Estado (UF)')
    plt.tight_layout() # Ajusta para não cortar os nomes
    plt.savefig('casos_por_estado_percentual.png')
    plt.show()

else:
    print("\nA união dos dados falhou. A tabela final está vazia.")
    print("Verifique se a coluna 'UF' (ex: 'SP', 'RJ') está idêntica nos arquivos SINAN e SNIS.")
import pandas as pd
from IPython.display import display
import matplotlib.pyplot as plt
import seaborn as sns
import warnings

# Ignora avisos de performance do pandas
warnings.simplefilter(action='ignore', category=FutureWarning)

def carregar_dados_sinan(caminho_arquivo, nome_coluna_casos):
    """
    (Esta função continua a mesma)
    Lê um arquivo do SINAN (Zika, Dengue, ou Chiku), limpa e transforma
    de formato 'wide' (largo) para 'long' (longo).
    """
    print(f"Processando arquivo: {caminho_arquivo}")
    df = pd.read_csv(caminho_arquivo, sep=';', encoding='latin1', skiprows=3)
    df = df.dropna(how='all')
    df = df[df['Ano notificação'].str.contains('Total') == False]
    df_melted = df.melt(id_vars=['Ano notificação'], 
                        var_name='UF', 
                        value_name=nome_coluna_casos)
    df_melted['Ano'] = pd.to_numeric(df_melted['Ano notificação'], errors='coerce')
    df_melted[nome_coluna_casos] = df_melted[nome_coluna_casos].str.strip()
    df_melted = df_melted[df_melted[nome_coluna_casos] != '-']
    df_melted[nome_coluna_casos] = pd.to_numeric(df_melted[nome_coluna_casos], errors='coerce')
    df_melted = df_melted.dropna(subset=['Ano', nome_coluna_casos])
    df_melted['Ano'] = df_melted['Ano'].astype(int)
    return df_melted[['Ano', 'UF', nome_coluna_casos]]

# --- ESTA É A NOVA FUNÇÃO ---
def carregar_e_juntar_snis_csv(caminho_2020, caminho_2021):
    """
    Lê os dois arquivos CSV (2020 e 2021), pulando os cabeçalhos,
    adiciona o ano, renomeia colunas e junta os dois.
    """
    print(f"Processando arquivo SNIS 2020: {caminho_2020}")
    # 1. Leitura do arquivo 2020
    # Usamos skiprows=6 para pular as 6 linhas de título
    # Usamos sep=',' baseado no cabeçalho do arquivo
    df_2020 = pd.read_csv(caminho_2020, sep=',', encoding='latin1', skiprows=6)
    df_2020['Ano'] = 2020 # Adiciona a coluna do ano

    print(f"Processando arquivo SNIS 2021: {caminho_2021}")
    # 2. Leitura do arquivo 2021 (assumindo que tem a mesma estrutura)
    df_2021 = pd.read_csv(caminho_2021, sep=',', encoding='latin1', skiprows=6)
    df_2021['Ano'] = 2021 # Adiciona a coluna do ano

    # 3. Junção (Concatenação)
    df_total_snis = pd.concat([df_2020, df_2021], ignore_index=True)

    print("Colunas encontradas nos arquivos SNIS:", df_total_snis.columns.tolist())

    # 4. Renomear Colunas: Ajuste os nomes aqui para bater com seu arquivo
    # <-- MUDE AQUI
    # O seu arquivo mostra o nome da coluna de região, mas não os indicadores.
    # Você precisa olhar no seu CSV e ver o nome EXATO das colunas.
    df_total_snis = df_total_snis.rename(columns={
        'Região': 'UF',  # <-- CONFIRME SE A COLUNA DE ESTADO se chama 'Região' ou 'Estado' ou 'UF'
        'IN055': 'Taxa_Cobertura_Agua', # Coluna do indicador IN055
        'IN056': 'Taxa_Cobertura_Esgoto' # Coluna do indicador IN056
    })

    # 5. Limpeza (MUITO IMPORTANTE)
    # Os números no seu arquivo estão como texto (ex: "89,951.16")
    # Este código limpa isso:
    for col in ['Taxa_Cobertura_Agua', 'Taxa_Cobertura_Esgoto']:
        if df_total_snis[col].dtype == 'object':
            print(f"Limpando coluna {col} (removendo '.' e trocando ',' por '.')")
            df_total_snis[col] = (
                df_total_snis[col]
                .astype(str)
                .str.replace('.', '', regex=False) # Remove o separador de milhar
                .str.replace(',', '.', regex=False) # Troca a vírgula decimal por ponto
            )
            df_total_snis[col] = pd.to_numeric(df_total_snis[col], errors='coerce')

    # 6. Seleção: Pega apenas as colunas que vamos usar
    try:
        df_final_snis = df_total_snis[['Ano', 'UF', 'Taxa_Cobertura_Agua', 'Taxa_Cobertura_Esgoto']]
        return df_final_snis
    except KeyError as e:
        print(f"ERRO: A coluna {e} não foi encontrada após renomear.")
        print("Verifique os nomes das colunas no passo '# <-- MUDE AQUI'.")
        return pd.DataFrame()

# --- PASSO 1: EXTRAIR E TRANSFORMAR ---

# Carrega e transforma os 3 arquivos de doenças
df_zika = carregar_dados_sinan('dados/ZIKA - SINAN.csv', 'Casos_Zika')
df_dengue = carregar_dados_sinan('dados/DENGUE - SINAN.csv', 'Casos_Dengue')
df_chiku = carregar_dados_sinan('dados/CHIKU - SINAN.csv', 'Casos_Chiku')

# <-- MUDE AQUI: Coloque os nomes dos seus DOIS arquivos CSV do SNIS
caminho_snis_2020 = 'dados/SNIS - 2020.csv' # Você já enviou este
caminho_snis_2021 = 'dados/SNIS - 2021.csv' # <--- Coloque o nome do seu arquivo de 2021 aqui
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
df_final = pd.merge(df_doencas, df_snis, on=['Ano', 'UF'], how='inner')
df_final = df_final.fillna(0)

print("\n--- TABELA FINAL UNIFICADA (2020-2021) ---")
display(df_final)

# --- PASSO 4: ANÁLISE DE CORRELAÇÃO ---
# (O restante do código continua o mesmo)
if not df_final.empty:
    print("\n--- Matriz de Correlação (2020-2021) ---")
    colunas_numericas = ['Casos_Zika', 'Casos_Dengue', 'Casos_Chiku', 'Taxa_Cobertura_Agua', 'Taxa_Cobertura_Esgoto']
    matriz_correlacao = df_final[colunas_numericas].corr()
    display(matriz_correlacao)

    print("\n--- Visualização da Correlação ---")
    plt.figure(figsize=(10, 7))
    sns.heatmap(matriz_correlacao, annot=True, cmap='coolwarm', fmt='.2f')
    plt.title('Matriz de Correlação: Saneamento vs. Doenças (Dados de 2020-2021)')
    plt.savefig('matriz_correlacao.png')
    plt.show()

    print("\n--- Gráfico: Correlação Esgoto vs. Zika (2020-2021) ---")
    plt.figure(figsize=(10, 6))
    sns.regplot(data=df_final, x='Taxa_Cobertura_Esgoto', y='Casos_Zika')
    plt.title('Correlação entre Cobertura de Esgoto e Casos de Zika (2020-2021)')
    plt.xlabel('Taxa de Cobertura de Esgoto (%) por Estado')
    plt.ylabel('Nº de Casos de Zika')
    plt.grid(True)
    plt.savefig('correlacao_esgoto_zika.png')
    plt.show()
else:
    print("\nA união dos dados falhou. Verifique os nomes das colunas e os dados nos arquivos de entrada.")
    print("Certifique-se que a coluna 'UF' (ex: 'SP', 'RJ') está idêntica nos arquivos SINAN e SNIS.")
import pandas as pd

def carregar_dados_sinan(caminho_arquivo, nome_coluna_casos):
    """
    Lê um arquivo do SINAN (Zika, Dengue, ou Chiku), limpa e transforma
    de formato 'wide' (largo) para 'long' (longo).
    """
    print(f"Processando arquivo: {caminho_arquivo}")
    try:
        df = pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8', skiprows=3)
    except UnicodeDecodeError:
        df = pd.read_csv(caminho_arquivo, sep=';', encoding='latin1', skiprows=3)
    
    df = df.dropna(how='all')
    if df.empty or df.columns[0] is None:
        print(f"Arquivo {caminho_arquivo} parece estar vazio ou formatado incorretamente.")
        return pd.DataFrame(columns=['Ano', 'UF', nome_coluna_casos])
        
    df = df.dropna(subset=[df.columns[0]])
    df = df[~df[df.columns[0]].astype(str).str.contains('Total', case=False, na=False)]
    
    df = df.rename(columns={df.columns[0]: 'Ano notificação'})
    
    df_melted = df.melt(id_vars=['Ano notificação'], 
                        var_name='UF', 
                        value_name=nome_coluna_casos)
    
    df_melted['Ano'] = df_melted['Ano notificação'].astype(str).str.extract(r'(\d{4})').astype(float)
    
    if df_melted[nome_coluna_casos].dtype == 'object':
        df_melted[nome_coluna_casos] = df_melted[nome_coluna_casos].astype(str).str.strip()
        df_melted[nome_coluna_casos] = df_melted[nome_coluna_casos].replace('-', pd.NA)
        # Remove '.' usado como separador de milhar
        df_melted[nome_coluna_casos] = df_melted[nome_coluna_casos].astype(str).str.replace(r'\.', '', regex=True)
        df_melted[nome_coluna_casos] = pd.to_numeric(df_melted[nome_coluna_casos], errors='coerce')
    
    df_melted = df_melted.dropna(subset=['Ano', nome_coluna_casos])
    df_melted['Ano'] = df_melted['Ano'].astype(int)
    
    df_melted['UF'] = df_melted['UF'].str.replace(r'\s*\(.*\)', '', regex=True).str.strip()
    
    return df_melted[['Ano', 'UF', nome_coluna_casos]]

def carregar_e_juntar_snis_csv(caminho_2020, caminho_2021):
    """
    Lê os dois arquivos CSV (2020 e 2021), adiciona o ano,
    renomeia colunas e junta os dois.
    """
    print(f"Processando arquivo SNIS 2020: {caminho_2020}")
    try:
        df_2020 = pd.read_csv(caminho_2020, sep=',', encoding='utf-8')
    except UnicodeDecodeError:
        df_2020 = pd.read_csv(caminho_2020, sep=',', encoding='latin1')
    df_2020['Ano'] = 2020

    print(f"Processando arquivo SNIS 2021: {caminho_2021}")
    try:
        df_2021 = pd.read_csv(caminho_2021, sep=',', encoding='utf-8')
    except UnicodeDecodeError:
        df_2021 = pd.read_csv(caminho_2021, sep=',', encoding='latin1')
    df_2021['Ano'] = 2021

    df_total_snis = pd.concat([df_2020, df_2021], ignore_index=True)

    rename_dict = {
        'IN055': 'Taxa_Cobertura_Agua',
        'IN056': 'Taxa_Cobertura_Esgoto'
    }
    if 'Região' in df_total_snis.columns:
        rename_dict['Região'] = 'UF'
    elif 'UF' in df_total_snis.columns:
         rename_dict['UF'] = 'UF'
    else:
        print("ERRO: Nenhuma coluna 'UF' ou 'Região' encontrada nos arquivos SNIS.")
        return pd.DataFrame()
        
    df_total_snis = df_total_snis.rename(columns=rename_dict)

    for col in ['Taxa_Cobertura_Agua', 'Taxa_Cobertura_Esgoto']:
        if col in df_total_snis.columns:
            if df_total_snis[col].dtype == 'object':
                print(f"Limpando coluna {col} (removendo '.' e trocando ',' por '.')")
                df_total_snis[col] = (
                    df_total_snis[col]
                    .fillna('') 
                    .astype(str) 
                    .str.replace(r'[^\d,-]', '', regex=True)
                    .str.replace('.', '', regex=False) 
                    .str.replace(',', '.', regex=False) 
                )
                df_total_snis[col] = pd.to_numeric(df_total_snis[col], errors='coerce')
        else:
            print(f"Aviso: Coluna {col} não encontrada no SNIS.")

    colunas_necessarias = ['Ano', 'UF']
    if 'Taxa_Cobertura_Agua' in df_total_snis.columns:
        colunas_necessarias.append('Taxa_Cobertura_Agua')
    if 'Taxa_Cobertura_Esgoto' in df_total_snis.columns:
        colunas_necessarias.append('Taxa_Cobertura_Esgoto')

    try:
        df_final_snis = df_total_snis[colunas_necessarias]
        # Filtra apenas UFs válidas, removendo agregados como "Brasil" ou "Nordeste"
        ufs_validas = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
        df_final_snis = df_final_snis[df_final_snis['UF'].isin(ufs_validas)]
        return df_final_snis
    except KeyError as e:
        print(f"ERRO: A coluna {e} não foi encontrada após renomear.")
        print("Colunas disponíveis:", df_total_snis.columns.tolist())
        return pd.DataFrame()

# --- Bloco principal para rodar o script ---
if __name__ == "__main__":
    # --- PASSO 1: EXTRAIR E TRANSFORMAR ---
    # ATENÇÃO: Substitua os caminhos abaixo pelos caminhos corretos em seu computador
    path_zika = 'dados/ZIKA - SINAN.csv'
    path_dengue = 'dados/DENGUE - SINAN.csv'
    path_chiku = 'dados/CHIKU - SINAN.csv'
    path_snis_2020 = 'dados/SNIS - 2020.csv'
    path_snis_2021 = 'dados/SNIS - 2021.csv'
    
    # (O script executado no servidor usa os caminhos completos dos arquivos carregados)
    # Exemplo:
    # path_zika = 'jaauun/projeto-big-data---py/Projeto-BIG-DATA---PY-234189b7ce7090152fa0506e20937fb1c23d715d/dados/ZIKA - SINAN.csv'
    # ... e assim por diante para os outros arquivos

    df_zika = carregar_dados_sinan(path_zika, 'Casos_Zika')
    df_dengue = carregar_dados_sinan(path_dengue, 'Casos_Dengue')
    df_chiku = carregar_dados_sinan(path_chiku, 'Casos_Chiku')
    df_snis = carregar_e_juntar_snis_csv(path_snis_2020, path_snis_2021)

    # --- PASSO 2: FILTRAR POR ANO ---
    anos_desejados = [2020, 2021]
    print(f"\nFiltrando dados das doenças para manter apenas os anos: {anos_desejados}\n")

    df_zika = df_zika[df_zika['Ano'].isin(anos_desejados)]
    df_dengue = df_dengue[df_dengue['Ano'].isin(anos_desejados)]
    df_chiku = df_chiku[df_chiku['Ano'].isin(anos_desejados)]

    # --- PASSO 3: UNIR OS DADOS (LOAD) ---
    print("--- Unindo tabelas ---")

    df_doencas = pd.merge(df_zika, df_dengue, on=['Ano', 'UF'], how='outer')
    df_doencas = pd.merge(df_doencas, df_chiku, on=['Ano', 'UF'], how='outer')
    df_doencas = df_doencas.fillna(0) # Preenche com 0 casos onde não houveram notificações

    # Une os dados de doenças com os de saneamento (apenas UFs presentes em ambos)
    df_final = pd.merge(df_doencas, df_snis, on=['Ano', 'UF'], how='inner')

    print("\n--- TABELA FINAL UNIFICADA (2020-2021) ---")
    print(df_final)

    # --- PASSO 4: GRÁFICO DE RANKING DE CASOS TOTAIS POR ESTADO (NOVO GRÁFICO) ---
    if not df_final.empty:
        print("\n--- Gerando Gráfico: Ranking de Casos Totais por Estado (2020-2021) ---")
        
        # 1. Soma todas as doenças para ter os 'Casos_Totais'
        df_final['Casos_Totais'] = df_final['Casos_Zika'] + df_final['Casos_Dengue'] + df_final['Casos_Chiku']

        # 2. Agrega os dados por UF (soma os casos de 2020 e 2021)
        df_agregado = df_final.groupby('UF')['Casos_Totais'].sum().reset_index()
        
        # 3. Ordena o dataframe pelo total de casos (maior para o menor)
        df_agregado = df_agregado.sort_values(by='Casos_Totais', ascending=False)
        
        print("\n--- Dados Agregados para o Gráfico ---")
        print(df_agregado)
        

    else:
        print("\nA união dos dados falhou. A tabela final está vazia.")
        print("Verifique se a coluna 'UF' (ex: 'SP', 'RJ') está idêntica nos arquivos SINAN e SNIS.")
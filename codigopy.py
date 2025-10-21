import pandas as pd
from IPython.display import display

tabela = pd.read_csv("dados\SNIS V2.csv", encoding='utf-16', sep=',')
display(tabela)
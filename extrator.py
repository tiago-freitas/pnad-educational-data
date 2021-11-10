from glob import glob
import sqlite3

import pandas as pd

PATH = '/media/tiago/HDD - Tiago/pnad'

for ano in range(2012, 2016):
        print(ano)
        with open(f'{PATH}/{ano}/Dicionários e input/input PES{ano}.txt',
                   encoding='windows-1252') as myfile:
                input_PNADC_trimestre2 = [line.replace('INPUT', '').strip().split(maxsplit=3)
                                          for line in myfile 
                                          if line.strip() and
                                          ((line.strip()[0] == '@') 
                                          or (line.strip()[:5] == 'INPUT'))]
        
        desde, names, sizes, desc = zip(*input_PNADC_trimestre2)
        desde = [int(n.strip('@')) - 1 for n in desde]
        widths = [int(w.strip('$.')) for w in sizes]
        colspecs = [(start, start + width) for start, width in zip(desde, widths)]
        # dtypes = {nome:'category' for nome, tipo in zip(names, sizes) if tipo[0] == '$'}


        chunks = pd.read_fwf(glob(f'{PATH}/{ano}/Dados/PES{ano}*')[0],
             colspecs=colspecs,
             names=names,
             na_values='.',
             chunksize=50_000)

        with sqlite3.connect(f'pnad{ano}.sqlite') as conn:   
                for df in chunks:
                        df.to_sql('tabela', conn, if_exists='append', index=False)


#!/usr/bin/env python
import pandas as pd
import numpy as np
xl = pd.ExcelFile('./NVT_Metadatentabelle_MASTER.xlsm')
xl.sheet_names  # see all sheet names
## events = pd.read_excel('./NVT_Metadatentabelle_MASTER.xlsm', sheet_name='Ereignisse')
productions = pd.read_excel('./NVT_Metadatentabelle_MASTER.xlsm', sheet_name='Produktionen', header=1)
prclean = (productions[productions['Identifier / geeinigter Name'].str.contains('PR', na=False)] ## drops all rows that don't start with 'PR' or are NaN
           .drop(['Musik (für GEMA)'], axis='columns') ## drops unmodeled column
           .drop([2]) ## drops example row
           .reset_index(drop=True) ## resets index to be continouus
           .rename(columns={'Identifier / geeinigter Name':'ID', ## renames columns for future reference
                                          'Produktionsname / Titel':'PR_Titel',
                                          'Quelle (Beschreibung)':'Q_Beschreibung',
                                          'Verwendete Texte':'Verwendete_Texte',
                                          'Autor(en) der Texte':'Autoren_Texte',
                                          'Beteiligte Gruppen / Compagnies':'Beteiligte_Gruppen',
                                          'Sprecher*in':'SprecherIn',
                                          'Darsteller allgem.':'Darsteller',
                                          'Weitere Mitwirkende':'Mitwirkende',
                                          'Spielzeit / Laufzeit Start':'Spielzeit_Start',
                                          'Spielzeit / Laufzeit Ende':'Spielzeit_Ende'
}))
prclean.head()

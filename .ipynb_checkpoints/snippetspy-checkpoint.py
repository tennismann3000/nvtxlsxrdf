jupyter nbconvert --to script rearrangement.ipynb

dict(zip(list(pr_clean.columns.values), list(range(0, len(pr_clean.columns.values))))) ## liste von spalten + index

productions = productions.rename(columns={'Identifier / geeinigter Name':'ID',
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
})

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

pr_clean["Spielzeit"] = ("season_" + pr_clean["Spielzeit_Start"].map(str) + pr_clean["Spielzeit_Ende"].map(str))

pr_clean["Spielzeit_Start"][49]
type(pr_clean["Spielzeit_Start"][49])
datetime_object = datetime.strptime(pr_clean["Spielzeit_Start"][49], '%Y-%m-%d:%H:%M')
datetime_object
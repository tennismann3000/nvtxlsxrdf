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

 + "_" + pr_clean["Spielzeit_Ende"][:4] "season_" + pr_clean["ID"] + "_" +

pr_clean["Spielzeit_Start"] = pr_clean["Spielzeit_Start"].apply(str)
type(pr_clean["Spielzeit_Start"][8])

pr_clean["Spielzeit_Start"] = pr_clean["Spielzeit_Start"].apply(str)
pr_clean["Spielzeit_Ende"] = pr_clean["Spielzeit_Ende"].apply(str)

## nicht gut:
for j, i in pr_clean.iterrows():
    print(i["Spielzeit"])
    if pd.notna(i["Spielzeit_Start"]) and pd.notna(i["Spielzeit_Ende"]):
        i["Spielzeit"] = "season_" + str(i["ID"]) + "_" + str(i["Spielzeit_Start"])[:4] + "_" + str(i["Spielzeit_Ende"])[:4]
        print(j, i["Spielzeit"])
    elif pd.notna(i["Spielzeit_Start"]) and pd.isna(i["Spielzeit_Ende"]):
        i["Spielzeit"] = "season_" + str(i["ID"]) + "_" + str(i["Spielzeit_Start"])[:4]
        print(j, i["Spielzeit"])
    elif pd.isna(i["Spielzeit_Start"]) and pd.notna(i["Spielzeit_Ende"]):
        i["Spielzeit"] = "season_" + str(i["ID"]) + "_" + str(i["Spielzeit_Ende"])[:4]
        print(i["Spielzeit"])
    else:
        pass
## besser:
pr_clean.loc[pr_clean["Spielzeit_Start"].notna() & pr_clean["Spielzeit_Ende"].notna(), "Spielzeit"] = "season_" + pr_clean["ID"] + "_" + \
pr_clean["Spielzeit_Start"].apply(str).str[:4] + "_" + pr_clean["Spielzeit_Ende"].apply(str).str[:4]

pr_clean.loc[pr_clean["Spielzeit_Start"].notna() & pr_clean["Spielzeit_Ende"].isna(), "Spielzeit"] = "season_" + pr_clean["ID"] + "_" + \
pr_clean["Spielzeit_Start"].apply(str).str[:4]

pr_clean.loc[pr_clean["Spielzeit_Start"].isna() & pr_clean["Spielzeit_Ende"].notna(), "Spielzeit"] = "season_" + pr_clean["ID"] + "_" + \
pr_clean["Spielzeit_Ende"].apply(str).str[:4]

## noch besser:
def merge_seasons(df, col1, col2, col3):
    df.loc[df[col1].notna() & df[col2].notna(), col3] = "season_" + df["ID"] + "_" + df[col1].apply(str).str[:4] + "_" + pr_clean[col2].apply(str).str[:4]
    df.loc[df[col1].notna() & df[col2].isna(), col3] = "season_" + df["ID"] + "_" + df[col1].apply(str).str[:4]
    df.loc[df[col1].isna() & df[col2].notna(), col3] = "season_" + df["ID"] + "_" + df[col2].apply(str).str[:4]
    return df

def merge_description(df, col1, col2, col3):
    df.loc[df[col1].notna() & df[col2].notna(), col3] = "Beschreibung: " + pr_clean[col1] + " Quelle: " + pr_clean[col2]
    df.loc[df[col1].notna() & df[col2].isna(), col3] = "Beschreibung: " + pr_clean[col1]
    df.loc[df[col1].isna() & df[col2].notna(), col3] = " Quelle: " + pr_clean[col2]
    return df

def merge_columns(df, col1, col2, result=None):
    if result == "season":
        col3 = "Spielzeit"
        df.loc[df[col1].notna() & df[col2].notna(), col3] = "season_" + df["ID"] + "_" + df[col1].apply(str).str[:4] + "_" + pr_clean[col2].apply(str).str[:4]
        df.loc[df[col1].notna() & df[col2].isna(), col3] = "season_" + df["ID"] + "_" + df[col1].apply(str).str[:4]
        df.loc[df[col1].isna() & df[col2].notna(), col3] = "season_" + df["ID"] + "_" + df[col2].apply(str).str[:4]
        return df
    elif result == "description":
        col3 = "BeschreibungQuelle"
        df.loc[df[col1].notna() & df[col2].notna(), col3] = "Beschreibung: " + pr_clean[col1] + " Quelle: " + pr_clean[col2]
        df.loc[df[col1].notna() & df[col2].isna(), col3] = "Beschreibung: " + pr_clean[col1]
        df.loc[df[col1].isna() & df[col2].notna(), col3] = " Quelle: " + pr_clean[col2]
        return df

pr_clean.loc[pr_clean["Beschreibung"].notna() & pr_clean["Q_Beschreibung"].notna(), "Beschreibung_Quelle"] = "Beschreibung: " + \
pr_clean["Beschreibung"] + " Quelle: " + pr_clean["Q_Beschreibung"]
# first class try

class MergeColumns:

    def __init__(self, df, col1, col2, col3):

        self.df = df
        self.col1 = col1
        self.col2 = col2
        self.col3 = col3

        notnot = df.loc[df[col1].notna() & df[col2].notna(), col3]
        notis = df.loc[df[col1].notna() & df[col2].isna(), col3]
        isnot = df.loc[df[col1].isna() & df[col2].notna(), col3]

class MergeSeason(MergeColumns):

    def __init__(self, df, col1, col2, col3):

        super().__init__(df, col1, col2, col3)
        print(col1, df[col2][8], col3)
        notnot = "season_" + df["ID"] + "_" + df[col1].apply(str).str[:4] + "_" + pr_clean[col2].apply(str).str[:4]

pr_clean["Spielzeit"] = np.nan
pr_test = MergeSeason(pr_clean, "Spielzeit_Start", "Spielzeit_Ende", "Spielzeit")

## um sich spalten mit Informationen anzuschauen:

for idx, i in enumerate(pr_clean.columns):
    print([idx, i])

for idx, i in enumerate(pr_clean.columns):
    if idx == 0:
        print("Referenzliste zum splitten und replacen; I=ID R=reference L=literal N=None\n\nrefreplace für I und R, split für R, erst split dann refreplace\n")
        print(["I", idx, i])
    elif idx in range(6, 27) or idx in range(31, 35):
        print(["R", idx, i])
    elif idx in range(2, 6):
        print(["N", idx])
    else:
        print(["L", idx, i])

        pr_clean.iloc[:, 6:27] = pr_clean.iloc[:, 6:27].apply(refreplace, axis=1).apply(supersplit, axis=1)
pr_clean.iloc[:, 31:35] = pr_clean.iloc[:, 31:35].apply(refreplace, axis=1).apply(supersplit, axis=1)

owl = Namespace("http://www.w3.org/2002/07/owl#")
rdf = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
rdfs = Namespace("http://www.w3.org/2000/01/rdf-schema#")
wgs84_pos = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
edm = Namespace("http://www.europeana.eu/schemas/edm/")
dc = Namespace("http://purl.org/dc/elements/1.1/")
dct = Namespace("http://purl.org/dc/terms/")
nvt = Namespace("http://lod.iti-germany.de/resource/")
nvto = Namespace("http://lod.iti-germany.de/schema/nvto/")

pr_graph = Graph(identifier="http://lod.iti-germany.de/contexts/productions")
ev_graph = Graph(identifier="http://lod.iti-germany.de/contexts/events")
vid_graph = Graph(identifier="http://lod.iti-germany.de/contexts/videos")
text_graph = Graph(identifier="http://lod.iti-germany.de/contexts/texts")
img_graph = Graph(identifier="http://lod.iti-germany.de/contexts/images")
aud_graph = Graph(identifier="http://lod.iti-germany.de/contexts/audio")
person_graph = Graph(identifier="http://lod.iti-germany.de/contexts/persons")
group_graph = Graph(identifier="http://lod.iti-germany.de/contexts/groups")
loc_graph = Graph(identifier="http://lod.iti-germany.de/contexts/locations")
city_graph = Graph(identifier="http://lod.iti-germany.de/contexts/cities")
country_graph = Graph(identifier="http://lod.iti-germany.de/contexts/countries")
col_graph = Graph(identifier="http://lod.iti-germany.de/contexts/collections")
series_graph = Graph(identifier="http://lod.iti-germany.de/contexts/series")
concept_graph = Graph(identifier="http://lod.iti-germany.de/contexts/concepts")
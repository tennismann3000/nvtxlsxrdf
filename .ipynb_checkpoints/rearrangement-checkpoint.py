import pandas as pd
import rdflib
from rdflib import Graph, Namespace, URIRef, BNode, Literal, RDF
from rdflib.namespace import NamespaceManager
import datetime
pd.options.display.max_colwidth = 144

nvt_master = pd.read_excel('./NVT_Metadatentabelle_MASTER_0207.xlsm', sheet_name=None)

context = Namespace("http://lod.iti-germany.de/contexts/")
dc = Namespace("http://purl.org/dc/elements/1.1/")
dcterms = Namespace("http://purl.org/dc/terms/")
edm = Namespace("http://www.europeana.eu/schemas/edm/")
foaf = Namespace("http://xmlns.com/foaf/0.1/")
nvt = Namespace("http://lod.iti-germany.de/resource/")
nvto = Namespace("http://lod.iti-germany.de/schema/nvto/")
owl = Namespace("http://www.w3.org/2002/07/owl#")
rdf = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
rdfs = Namespace("http://www.w3.org/2000/01/rdf-schema#")
skos = Namespace("http://www.w3.org/2004/02/skos/core#")
wgs84_pos = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")

def bindbind(graph):
    graph.bind("context", "http://lod.iti-germany.de/contexts/")
    graph.bind("dc", "http://purl.org/dc/elements/1.1/")
    graph.bind("dcterms", "http://purl.org/dc/terms/")
    graph.bind("edm", "http://www.europeana.eu/schemas/edm/")
    graph.bind("foaf", "http://xmlns.com/foaf/0.1/")
    graph.bind("nvt", "http://lod.iti-germany.de/resource/")
    graph.bind("nvto", "http://lod.iti-germany.de/schema/nvto/")
    graph.bind("owl", "http://www.w3.org/2002/07/owl#")
    graph.bind("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    graph.bind("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
    graph.bind("skos", "http://www.w3.org/2004/02/skos/core#")
    graph.bind("wgs84_pos", "http://www.w3.org/2003/01/geo/wgs84_pos#")
    return graph

nvt_ds = rdflib.Dataset()
nvt_ds = bindbind(nvt_ds)
pr_graph = nvt_ds.graph(identifier="http://lod.iti-germany.de/contexts/productions")
ev_graph = nvt_ds.graph(identifier="http://lod.iti-germany.de/contexts/events")
vid_graph = nvt_ds.graph(identifier="http://lod.iti-germany.de/contexts/videos")
text_graph = nvt_ds.graph(identifier="http://lod.iti-germany.de/contexts/texts")
img_graph = nvt_ds.graph(identifier="http://lod.iti-germany.de/contexts/images")
aud_graph = nvt_ds.graph(identifier="http://lod.iti-germany.de/contexts/audio")
person_graph = nvt_ds.graph(identifier="http://lod.iti-germany.de/contexts/persons")
group_graph = nvt_ds.graph(identifier="http://lod.iti-germany.de/contexts/groups")
loc_graph = nvt_ds.graph(identifier="http://lod.iti-germany.de/contexts/locations")
city_graph = nvt_ds.graph(identifier="http://lod.iti-germany.de/contexts/cities")
country_graph = nvt_ds.graph(identifier="http://lod.iti-germany.de/contexts/countries")
col_graph = nvt_ds.graph(identifier="http://lod.iti-germany.de/contexts/collections")
series_graph = nvt_ds.graph(identifier="http://lod.iti-germany.de/contexts/series")
concept_graph = nvt_ds.graph(identifier="http://lod.iti-germany.de/contexts/concepts")
graph_list = [pr_graph, ev_graph, vid_graph, text_graph, img_graph, aud_graph, person_graph, group_graph, loc_graph, city_graph, country_graph, col_graph, series_graph, concept_graph]

class Sheet():
## name: Name des Reiters
## head: Indikator, wo im df Tabellenüberschriften zu finden sind (0 = Inhalte Zeile 1, 1 = Inhalte Zeile 2, x = Index unverändert)
## df: DataFrame pro Sheet
## tdf: Kopie von df, mit über head definierter Indexanpassung
## tdf_empty_transposed: DataFrame, der nur aus Überschriften (ohne Spaltenachsennamen) besteht, zur Konvertierung nach Excel, inklusive Transponierung
    def __init__(self, name, head, df, graph):
        self.name = name
        self.head = head
        self.df = df
        self.graph = graph
        self.tdf = df.copy()
        self.column_names = None
        self.tdf_empty_transposed = None
        if self.head != "x":
            self.tdf.columns = self.tdf.iloc[self.head]
            self.tdf.columns.name = None
        else:
            pass
        if isinstance(self.tdf, pd.DataFrame):
            self.column_names = self.tdf.columns.tolist()
            self.tdf_empty_transposed = self.tdf[0:0].transpose(copy=True)

## Zur Erzeungung von Sheet Instanzen mit Name und Spaltenkopfreferenz als Liste und mit dict Referenz
## column_headers: Liste von 'echten' Spaltenüberschriften, 0, 1, für erste, zweite Zeile, x=Standard
## col_ref dict um nicht mit Zahlen, sondern Namen zu arbeiten
sheet_list = []
col_ref = {}
column_headers = [0, 0, 0, 0, 0, 0, 1, "x", "x", "x", "x", "x", "x"]
for idx, i in enumerate([sheet for sheet in nvt_master.keys()][2:15]):
    sheet_list.append(Sheet(name = i, head = column_headers[idx], df = nvt_master[i], graph = graph_list[idx]))
    col_ref[i] = sheet_list[idx]

## writer = pd.ExcelWriter('Columns_for_Mapping.xlsx', engine='xlsxwriter')
## for i in sheet_list:
##     i.tdf_empty_transposed.to_excel(writer, header=False, sheet_name=i.name)
## writer.save()

def merge_columns(df, col1, col2, result=None):
    ## noch ein bisschen sauberer wäre gut, zeilenumbruch nach beschreibung vllt und personennamen in richtiger schreibweise
    ## bei result = texts müssten Autoren noch mit den Labels der Personen, auf die verwiesen wird, versehen werden

    if result == "zeitraum":
        col3 = "Zeitraum"
        df.loc[df[col1].notna() & df[col2].notna(), col3] = "timespan_" + df["ID"] + "_" + df[col1].apply(str).str[:4] + "_" + df[col2].apply(str).str[:4]
        df.loc[df[col1].notna() & df[col2].isna(), col3] = "timespan_" + df["ID"] + "_" + df[col1].apply(str).str[:4]
        df.loc[df[col1].isna() & df[col2].notna(), col3] = "timespan_" + df["ID"] + "_" + df[col2].apply(str).str[:4]
        return df

    elif result == "season":
        col3 = "Spielzeit"
        df.loc[df[col1].notna() & df[col2].notna(), col3] = "season_" + df["ID"] + "_" + df[col1].apply(str).str[:4] + "_" + df[col2].apply(str).str[:4]
        df.loc[df[col1].notna() & df[col2].isna(), col3] = "season_" + df["ID"] + "_" + df[col1].apply(str).str[:4]
        df.loc[df[col1].isna() & df[col2].notna(), col3] = "season_" + df["ID"] + "_" + df[col2].apply(str).str[:4]
        return df

    elif result == "description":
        col3 = "Beschreibung_Quelle"
        df.loc[df[col1].notna() & df[col2].notna(), col3] = df[col1] + " Quelle: " + df[col2]
        df.loc[df[col1].notna() & df[col2].isna(), col3] = df[col1]
        df.loc[df[col1].isna() & df[col2].notna(), col3] = " Quelle: " + df[col2]
        return df

    elif result == "texts":
        col3 = "Texte_Autoren"
        df[col2] = peoplereplace(df[col2])
        df.loc[df[col1].notna() & df[col2].notna(), col3] = "Text(e): " + df[col1] + " Autor(en): " + df[col2]
        df.loc[df[col1].notna() & df[col2].isna(), col3] = "Text(e): " + df[col1]
        df.loc[df[col1].isna() & df[col2].notna(), col3] = "Autor(en): " + df[col2]
        return df

    elif result == "condition":
        col3 = "Zustand_Datum"
        df.loc[df[col1].notna() & df[col2].notna(), col3] = "Zustand: " + df[col1] + " Datum Zustandsaufnahme: " + df[col2].apply(str)
        df.loc[df[col1].notna() & df[col2].isna(), col3] = "Zustand: " + df[col1]
        df.loc[df[col1].isna() & df[col2].notna(), col3] = " Datum Zustandsaufnahme: " + df[col2].apply(str)
        return df

    elif result == "institution":
        col3 = "LOC_Institution"
        df.loc[df[col2].notna(), col3] = df[col1].str.replace("LOC_", "G_")
        return df
    else:
        print("Wrong Keyword, nothing happened")
        return df

def physicaldigital(df, col):
    df.loc[df[col].notna(), "Phys_ID"] = "phys_" + df[col]
    df.loc[df[col].notna(), "Digi_ID"] = "digi_" + df[col]
    return df

def peoplereplace(df):
    ## temporary string beautifier for persons as literal values
    df = df.str.replace("_"," ")
    df = df.str.replace(",", " ")
    df = df.str.replace(";", "; ")
    return df

def refreplace(df):
    ## string converter for URIs
    df = df.str.lower()
    df = df.str.replace("ä", "ae")
    df = df.str.replace("ö","oe")
    df = df.str.replace("ü","ue")
    df = df.str.replace("ß","ss")
    df = df.str.replace("!?,\.", "")
    df = df.str.replace("[^a-zA-Z0-9;]", "_")
    df = df.str.replace("-", "_")
    df = df.str.replace("__", "_")
    df = df.str.strip("_")
    df = df.str.strip()
    return df

def supersplit(df):
    ## for more convenient str splitting within the columns (expand=false), mainly for splitting all URIs
    if df.any():
        df = df.str.split(";", expand = False)
    return df

def splitreplace(df):
    ## combines refreplace and supersplit in correct order for they are both always used for any columns containing URIs
    df = df.apply(refreplace, axis=1)
    df = df.apply(supersplit, axis=1)
    return df

def sort_reindex(df, index=None):
    df = (df.sort_values(df.columns[index])
          .reset_index(drop=True) ## resets index to be continouus
         )
    return df

def uri_list(column2, predicate, column="ID"):
    if isinstance(row[column], list):
        idid = row[column][0] ## weil id immer einzigartig ist
        if isinstance(row[column2], list):
            for i in row[column2]:
                graph.add((nvt[idid], predicate, nvt[str(i)]))

def url_list(column2, predicate, column="ID"):
    if isinstance(row[column], list):
        idid = row[column][0] ## weil id immer einzigartig ist
        if isinstance(row[column2], list):
            for i in row[column2]:
                uri = URIRef(i.strip()) ## strip an dieser Stelle weil ichs nich in supersplit hinbekommen habe
                graph.add((nvt[idid], predicate, uri))

def lit_list(column2, predicate, column="ID", language=None):
    lang = None
    if isinstance(row[column], list):
        idid = row[column][0]
        if isinstance(row[column2], str) or isinstance(row[column2], datetime.datetime):
            graph.add((nvt[idid], predicate, Literal(row[column2], lang=language)))

def uri_type(column, uri_type):
    if isinstance(row[column], list):
        for i in row[column]:
            graph.add((nvt[i], rdf.type, uri_type))

pr_clean = nvt_master["Produktionen"]
pr_clean.columns = pr_clean.iloc[0]

pr_clean = (pr_clean[pr_clean['Identifier / geeinigter Name'].str.contains('PR', na=False)] ## drops all rows that don't start with 'PR' or are NaN
            .drop(pr_clean[pr_clean['Identifier / geeinigter Name'].str.contains('PR_Internationaler_Workshop_zur_Biomechanik_GITIS Moskau_Januar_1993', na=False)].index)
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
                           }
                   )
           )
pr_clean = merge_columns(pr_clean, "Spielzeit_Start", "Spielzeit_Ende", result = "season") ## in Zukunft vllt nur Keyword, wenn Tabellentitel angepasst sind
pr_clean = merge_columns(pr_clean, "Verwendete_Texte", "Autoren_Texte", result = "texts") ## in Zukunft müssten die Autoren mit der Personentabelle abgeglichen werden, am besten trotzdem noch als literal
pr_clean = merge_columns(pr_clean, "Beschreibung", "Q_Beschreibung", result = "description")

pr_clean.iloc[:, [0]] = splitreplace(pr_clean.iloc[:, [0]])
pr_clean.iloc[:, 6:28] = splitreplace(pr_clean.iloc[:, 6:28])
pr_clean.iloc[:, 32:36] = splitreplace(pr_clean.iloc[:, 32:36])

pr_clean = sort_reindex(pr_clean, index=0)

ev_clean = nvt_master["Ereignisse"]
ev_clean.columns = ev_clean.iloc[0]

ev_clean = (ev_clean[ev_clean['Identifier / geeinigter Name'].str.contains('EV', na=False)]
            .drop(ev_clean[ev_clean['Identifier / geeinigter Name'].str.contains('EV_Internationaler_Workshop_zur_Biomechanik_GITIS Moskau_Januar_1993_001', na=False)].index) ## drops example line
            .rename(columns={'Identifier / geeinigter Name':'ID', ## renames columns for future reference
                             'Ereignisname / Ereignistitel':'EV_Titel',
                             'Quelle (Beschreibung)':'Q_Beschreibung',
                             'Gehört zu Produktion':'Gehört_PR',
                             'Erwähnung von':'Erwähnung_von',
                             'Bezug auf / Über / zentraler Gegenstand (Subject)':'Subject',
                             'Teilereignis von':'Teilereignis_von',
                             'Beteiligte Gruppen / Compagnies':'Beteiligte_Gruppen',
                             'Sprecher*in':'SprecherIn',
                             'Darsteller allgem.':'Darsteller',
                             'Lehrer  / Workshopleiter':'Lehrer',
                             'Weitere Mitwirkende':'Mitwirkende',
                             'Zeitpunkt (Datum)':'Zeitpunkt',
                             'Zeitraum Start':'Zeitraum_Start',
                             'Zeitraum Ende':'Zeitraum_Ende'
                           }
                   )
           )
ev_clean = merge_columns(ev_clean, "Beschreibung", "Q_Beschreibung", result = "description")
ev_clean = merge_columns(ev_clean, "Zeitraum_Start", "Zeitraum_Ende", result = "zeitraum")

ev_clean.iloc[:, 5:31] = splitreplace(ev_clean.iloc[:, 5:31])
ev_clean.iloc[:, [0, 2, 35, 36, 37, 39]] = splitreplace(ev_clean.iloc[:, [0, 2, 35, 36, 37, 39]])

ev_clean = sort_reindex(ev_clean, index = 0)

vid_clean = nvt_master["Objekte VIDEOS"]
vid_clean.columns = vid_clean.iloc[0]
vid_clean = (vid_clean[vid_clean['Projekt ID'].str.contains('vid', na=False)]
             .rename(columns={'Projekt ID':'ID',
                              'andere IDs':'andere_ID',
                              'Unterobjekt von':'Unterobjekt_von',
                              'Serie / Abfolge':'Serie',
                              'Sammlung (Projekt / DB)':'Sammlung',
                              'Gleicher Inhalt':'Gleicher_Inhalt',
                              'Abgebildete Produktionen':'Abgebildete_Produktionen',
                              'Erwähnte Produktionen':'Erwähnte_Produktionen',
                              'Annotation/Beschreibung':'Beschreibung',
                              'Quelle (Annotation)':'Q_Beschreibung',
                              'Abgebildete Ereignisse':'Abgebildete_Ereignisse',
                              'Erwähnte Ereignisse':'Erwähnte_Ereignisse',
                              'Kamera / Aufzeichner':'Kamera_Aufzeichner',
                              'Sichtbare Entitäten':'Sichtbare_Entitäten',
                              'Hörbare Entitäten':'Hörbare_Entitäten',
                              '(Irgendwie) Erwähnte Entitäten':'Erwähnte_Entitäten',
                              'Erwähnte Gruppen / Compagnies':'Erwähnte_Gruppen',
                              'Beitragsregie / Fernsehregie':'Beitragsregie_Fernsehregie',
                              'Sprecher*in':'SprecherIn',
                              'Darsteller allgem.':'Darsteller',
                              'Weitere Mitwirkende':'Mitwirkende',
                              'Sprache des Objekts':'Sprache_Objekt',
                              'Länge gesamtes Band':'Länge_Band',
                              'Zustand Objekt':'Zustand_Phys',
                              'Objekt identisch mit':'Objekt_identisch_mit',
                              'Zustand Digitalisat':'Zustand_Digi'
                             }
                    )
            )


vid_clean = merge_columns(vid_clean, "Beschreibung", "Q_Beschreibung", result = "description")
vid_clean = merge_columns(vid_clean, "Zustand_Phys", "Datum Zustandsaufnahme", result = "condition")
vid_clean = physicaldigital(vid_clean, "ID")

vid_clean.iloc[:, [2, 68, 69]] = splitreplace(vid_clean.iloc[:, [2, 68, 69]])
vid_clean.iloc[:, 7:16] = splitreplace(vid_clean.iloc[:, 7:16])
vid_clean.iloc[:, 20:44] = splitreplace(vid_clean.iloc[:, 20:44])
vid_clean.iloc[:, 47:50] = splitreplace(vid_clean.iloc[:, 47:50])

vid_clean = sort_reindex(vid_clean, index=2)

text_clean = nvt_master["Objekte TEXT"]
text_clean.columns = text_clean.iloc[0]

text_clean = (text_clean[text_clean['Projekt ID'].str.contains('txt', na=False)]
              .rename(columns={'Projekt ID':'ID',
                               'andere IDs':'andere_ID',
                               'Unterobjekt von':'Unterobjekt_von',
                               'Bildserie':'Serie',
                               'Sammlung (Projekt / DB)':'Sammlung',
                               'Gleiche Inhalte':'Gleicher_Inhalt',
                               'Abgebildete Produktionen':'Abgebildete_Produktionen',
                               'Erwähnte Produktionen':'Erwähnte_Produktionen',
                               'Abgebildete Ereignisse':'Abgebildete_Ereignisse',
                               'Erwähnte Ereignisse':'Erwähnte_Ereignisse',
                               'Annotation/Beschreibung':'Beschreibung',
                               'Quelle (Annotation)':'Q_Beschreibung',
                               'Herausgeberschaft (Institution / Verlag)':'Herausgeberschaft',
                               'Herausgeber*in (Person)':'HerausgeberIn',
                               'Autor*in':'AutorIn',
                               'Übersetzer*in':'ÜbersetzerIn',
                               'Layout/Satz':'Layout',
                               'Grafik/künstl. Gestaltung':'Grafik_Gestaltung',
                               'Weitere Mitwirkende':'Mitwirkende',
                               'Erwähnung Personen':'Erwähnung_Personen',
                               'Erwähnte Gruppen':'Erwähnung_Gruppen',
                               'Sichtbare Personen':'Sichtbare_Personen',
                               'Sichtbare Gruppen':'Sichtbare_Gruppen',
                               'Erstausgabe (der vorliegenden Sprache)':'Erstausgabe_Sprache',
                               'Erscheinungsstadt ':'Erscheinungsstadt',
                               'Anzahl Exemplare':'Anzahl_Exemplare',
                               'Format (Dimensionen)':'Phys_Format',
                               'Zustand Objekt':'Zustand_Phys',
                               'Datum Zustandsaufnahme':'Datum_Zustandsaufnahme',
                               'Objekt identisch mit':'Objekt_identisch_mit',
                               'Zustand Digitalisat':'Zustand_Digi'
                              }
                     )
             )

text_clean = merge_columns(text_clean, "Beschreibung", "Q_Beschreibung", result = "description")
text_clean = merge_columns(text_clean, "Zustand_Phys", "Datum_Zustandsaufnahme", result = "condition")
text_clean = physicaldigital(text_clean, "ID")

text_clean.iloc[:, [2, 38, 40, 52, 59, 60]] = splitreplace(text_clean.iloc[:, [2, 38, 40, 52, 59, 60]])
text_clean.iloc[:, 7:16] = splitreplace(text_clean.iloc[:, 7:16])
text_clean.iloc[:, 22:34] = splitreplace(text_clean.iloc[:, 22:34])

text_clean = sort_reindex(text_clean, index=2)

img_clean = nvt_master["Objekte BILD"]
img_clean.columns = img_clean.iloc[0]

img_clean = (img_clean[img_clean['Projekt ID'].str.contains('img', na=False)]
             .sort_values(img_clean.columns[2])
             .rename(columns={'Projekt ID':'ID',
                              'andere IDs':'andere_ID',
                              'Unterobjekt von':'Unterobjekt_von',
                              'Bildserie':'Serie',
                              'Sammlung (Projekt / DB)':'Sammlung',
                              'Gleiches Motive':'Gleicher_Inhalt',
                              'Abgebildete Produktionen':'Abgebildete_Produktionen',
                              'Erwähnte Produktionen':'Erwähnte_Produktionen',
                              'Abgebildete Ereignisse':'Abgebildete_Ereignisse',
                              'Erwähnte Ereignisse':'Erwähnte_Ereignisse',
                              'Motivbeschreibung':'Beschreibung',
                              'Quelle (Beschreibung)':'Q_Beschreibung',
                              'abgebildete Entitäten':'abgebildete_Entitäten',
                              'Fotograf*in':'FotografIn',
                              'Aufnahmedatum/Entstehungsdatum':'Aufnahmedatum',
                              'Beschriftung + Markierungen (vorn)':'Beschriftung_Vorn',
                              'Beschriftung + Markierungen (hinten)':'Beschriftung_Hinten',
                              'Bildtyp/ Träger':'Träger',
                              'Farbe (nach AAT)':'Farbe',
                              'Zustand Objekt':'Zustand_Phys',
                              'Datum Zustandsaufnahme':'Datum_Zustandsaufnahme',
                              'Objekt identisch mit':'Objekt_identisch_mit',
                              'Zustand Digitalisat':'Zustand_Digi'
                             }
                    )
            )

img_clean = merge_columns(img_clean, "Beschreibung", "Q_Beschreibung", result = "description")
img_clean = merge_columns(img_clean, "Zustand_Phys", "Datum_Zustandsaufnahme", result = "condition")
img_clean = physicaldigital(img_clean, "ID")

img_clean.iloc[:, [2, 20, 21, 24, 25, 26, 38, 45, 46]] = splitreplace(img_clean.iloc[:, [2, 20, 21, 24, 25, 26, 38, 45, 46]])
img_clean.iloc[:, 8:17] = splitreplace(img_clean.iloc[:, 8:17])

img_clean = sort_reindex(img_clean, index=2)

aud_clean = nvt_master["Objekte AUDIO"]
aud_clean.columns = aud_clean.iloc[0]

aud_clean = (aud_clean[aud_clean['Projekt ID'].str.contains('aud', na=False)]
             .rename(columns={'Projekt ID':'ID',
                              'andere IDs':'andere_ID',
                              'Unterobjekt von':'Unterobjekt_von',
                              'Sammlung (Projekt / DB)':'Sammlung',
                              'Gleiche Aufnahme':'Gleicher_Inhalt',
                              'Abgebildete Produktionen':'Abgebildete_Produktionen',
                              'Erwähnte Produktionen':'Erwähnte_Produktionen',
                              'Abgebildete Ereignisse':'Abgebildete_Ereignisse',
                              'Erwähnte Ereignisse':'Erwähnte_Ereignisse',
                              'Annotation/Beschreibung':'Beschreibung',
                              'Quelle (Annotation)':'Q_Beschreibung',
                              'Hörbare Entitäten':'Hörbare_Entitäten',
                              '(Irgendwie) Erwähnte Entitäten':'Erwähnte_Entitäten',
                              'Aufzeichner*in':'AufzeichnerIn',
                              'Beitragsregie / Radioregie':'Beitragsregie',
                              'Erwähnte Gruppen / Compagnies':'Erwähnte_Gruppen',
                              'Sprache des Objekts':'Sprache',
                              'Audioträger / Typ':'Träger',
                              'Länge gesamtes Band':'Länge_Band',
                              'Zustand Objekt':'Zustand_Phys',
                              'Datum Zustandsaufnahme':'Datum_Zustandsaufnahme',
                              'Objekt identisch mit':'Objekt_identisch_mit',
                              'Zustand Digitalisat':'Zustand_Digi'
                             }
                    )
            )

aud_clean = merge_columns(aud_clean, "Beschreibung", "Q_Beschreibung", result = "description")
aud_clean = merge_columns(aud_clean, "Zustand_Phys", "Datum_Zustandsaufnahme", result = "condition")
aud_clean = physicaldigital(aud_clean, "ID")

aud_clean.iloc[:, [2, 28, 29, 30, 40, 48, 49]] = splitreplace(aud_clean.iloc[:, [2, 28, 29, 30, 40, 48, 49]])
aud_clean.iloc[:, 7:16] = splitreplace(aud_clean.iloc[:, 7:16])
aud_clean.iloc[:, 20:25] = splitreplace(aud_clean.iloc[:, 20:25])

aud_clean = sort_reindex(aud_clean, index=2)

person_clean = nvt_master["||_Personen"]
person_clean.columns = person_clean.iloc[1]

person_clean = (person_clean[person_clean['Identifier / geeinigte Schreibweise'].str.contains(',', na=False)]
                .drop(person_clean[person_clean['Identifier / geeinigte Schreibweise'].str.contains('Vorname Vatersname,Nachname', na=False)].index)
                .rename(columns={"Identifier / geeinigte Schreibweise":"ID",
                                "Quelle (Beschreibung)":"Q_Beschreibung"}
                       )
               )

person_clean = merge_columns(person_clean, "Beschreibung", "Q_Beschreibung", result = "description")

person_clean.iloc[:, [0]] = splitreplace(person_clean.iloc[:, [0]])
person_clean.iloc[:, 9:15] = person_clean.iloc[:, 9:15].apply(supersplit, axis=1) ## weil die inhalte weblinks sind kein str.replace

person_clean = sort_reindex(person_clean, index=0)

group_clean = nvt_master["||_Gruppen_Ensembles"]

group_clean = (group_clean[group_clean['Gruppe Identifier / geeinigte Schreibweise'].str.contains('G_', na=False)]
                .rename(columns={"Gruppe Identifier / geeinigte Schreibweise":"ID",
                                "präferierter Name":"Name",
                                "weitere Namen":"Namen",
                                "Quelle Beschreibung":"Q_Beschreibung",
                                "ist Vorgänger von":"Vorgänger_von",
                                "ist Nachfolger von":"Nachfolger_von",
                                "ist ansässig Stadt":"ansässig_Stadt",
                                "ist ansässig Land":"ansässig_Land",
                                 "ist ansässig Haus":"ansässig_Haus",
                                 "WIKIDATA URI":"Wikidata"
                                }
                       )
               )

group_clean = merge_columns(group_clean, "Beschreibung", "Q_Beschreibung", result = "description")
group_clean.iloc[:, [0, 5, 6, 7, 8, 9]] = splitreplace(group_clean.iloc[:, [0, 5, 6, 7, 8, 9]])
group_clean.iloc[:, [10, 11, 12]] = group_clean.iloc[:, [10, 11, 12]].apply(supersplit, axis=1) ## weil die inhalte weblinks sind kein str.replace

group_clean = sort_reindex(group_clean, index=0)

loc_clean = nvt_master["||_Veranstaltungsort"]

loc_clean = (loc_clean[loc_clean['PROJEKT'].str.contains('LOC_', na=False)]
             .rename(columns={"PROJEKT":"ID",
                              "ist Institution":"ist_Institution",
                              "präferierter Ortsname":"Ortsname",
                              "weitere Ortsnamen":"weitere_Ortsnamen",
                              "Quelle Beschreibung":"Q_Beschreibung",
                              "Gehört Zu":"Gehört_Zu",
                              "geo:LAT":"LAT",
                              "geo:LONG":"LONG",
                              "Wikipedia URI":"Wikipedia",
                              "GND URI":"GND",
                              "WIKIDATA URI":"WIKIDATA",
                              "geonames URI":"geonames"
                             }
                    )
            )
loc_clean = merge_columns(loc_clean, "Beschreibung", "Q_Beschreibung", result = "description")
loc_clean = merge_columns(loc_clean, "ID", "ist_Institution", result="institution")
loc_clean.iloc[:, [0, 7, 8, 9, 17]] = splitreplace(loc_clean.iloc[:, [0, 7, 8, 9, 17]])
loc_clean.iloc[:, 12:16] = loc_clean.iloc[:, 12:16].apply(supersplit, axis=1)

loc_clean = sort_reindex(loc_clean, index=0)

city_clean = nvt_master["||_Städte"]


city_clean = (city_clean.rename(columns={"Stadt Identifier / geeinigte Schreibweise":"ID",
                                         "präferierter Stadtname":"präf_Stadtname",
                                         "Stadtname DE":"Stadtname_DE",
                                         "Stadtname EN":"Stadtname_EN",
                                         "LAND (Ref)":"Land",
                                         "geo:LAT":"LAT",
                                         "geo:LONG":"LONG",
                                         "geonames URI":"geonames"
                                        }
                               )
             )
city_clean.iloc[:, [0, 4]] = splitreplace(city_clean.iloc[:, [0, 4]])
city_clean.iloc[:, [7]] = city_clean.iloc[:, [7]].apply(supersplit, axis=1)

city_clean = sort_reindex(city_clean, index=0)

country_clean = nvt_master["||_Länder"]

country_clean = (country_clean.rename(columns={"Länder Identifier / geeinigte Schreibweise":"ID",
                                               "präferierter Ländername":"präf_Landname",
                                               "Ländername DE":"Landname_DE",
                                               "Ländername EN":"Landname_EN",
                                               "geo:LAT":"LAT",
                                               "geo:LONG":"LONG",
                                               "geonames URI":"geonames"
                                              }
                                     )
                )
country_clean.iloc[:, [0]] = splitreplace(country_clean.iloc[:, [0]])
country_clean.iloc[:, [6]] = country_clean.iloc[:, [6]].apply(supersplit, axis=1)

country_clean = sort_reindex(country_clean, index=0)

col_clean = nvt_master["Sammlungen"]

col_clean = (col_clean[col_clean['PROJEKT'].str.contains('COL_', na=False)]
             .drop(col_clean[col_clean['PROJEKT'].str.contains('COL_Identifier', na=False)].index)
             .rename(columns={"PROJEKT":"ID",
                              "Sammlungsbeschreibung":"Beschreibung",
                              "Quelle der Beschreibung":"Q_Beschreibung"
                             }
                    )
            )
col_clean.iloc[:, [0]] = splitreplace(col_clean.iloc[:, [0]])
col_clean = merge_columns(col_clean, "Beschreibung", "Q_Beschreibung", result = "description")

col_clean = sort_reindex(col_clean, index=0)

series_clean = nvt_master["Serie"]

series_clean = (series_clean[series_clean['PROJEKT'].str.contains('SRS_', na=False)]
                .drop(series_clean[series_clean['PROJEKT'].str.contains('SRS_Identifier', na=False)].index)
                .rename(columns={"PROJEKT":"ID",
                                 "Serienbeschreibung":"Beschreibung",
                                 "Quelle der Beschreibung":"Q_Beschreibung"
                                }
                       )
               )

series_clean.iloc[:, [0]] = splitreplace(series_clean.iloc[:, [0]])
series_clean = merge_columns(series_clean, "Beschreibung", "Q_Beschreibung", result = "description")

series_clean = sort_reindex(series_clean, index=0)

for index, row in pr_clean.iterrows():

    graph = pr_graph

    uri_type("ID", nvto.PerformingArtsProduction)

    lit_list("PR_Titel", nvto.hasTitle)
    lit_list("PR_Titel", skos.prefLabel)
    lit_list("PR_Titel", rdfs.label)
    lit_list("Premierendatum", nvto.hasFirstPerformanceDate)
    lit_list("Dernierendatum", nvto.hasLastPerformanceDate)

    uri_list("Produktionstyp", nvto.hasType)
    uri_list("Beteiligte_Gruppen", nvto.hasContributor)
    uri_list("Konzept", nvto.hasConceptOriginator)
    uri_list("Textbearbeitung", nvto.hasAdaptor)
    uri_list("Übersetzung", nvto.hasTranslator)
    uri_list("Regie", nvto.hasDirector)
    uri_list("SprecherIn", nvto.hasSpeaker)
    uri_list("Choreographie", nvto.hasChoreographer)
    uri_list("Dramaturgie", nvto.hasDramaturge)
    uri_list("Bühnenbild", nvto.hasSetDesigner)
    uri_list("Kostümdesgin", nvto.hasCostumedesigner)
    uri_list("Figurenbau", nvto.hasPuppetDesigner)
    uri_list("Maskenbau", nvto.hasMaskDesigner)
    uri_list("Lichtdesign", nvto.hasLightDesigner)
    uri_list("Videodesign", nvto.hasVideoDesigner)
    uri_list("Schauspieler", nvto.hasActor)
    uri_list("Tänzer", nvto.hasDancer)
    uri_list("Darsteller", nvto.hasPerformer)
    uri_list("Musiker", nvto.hasMusician)
    uri_list("Komposition", nvto.hasComposer)
    uri_list("Mitwirkende", nvto.hasContributor)
    uri_list("Veranstaltungsort", nvto.hasRelatedPlace)
    uri_list("Stadt", nvto.hasRelatedPlace)
    uri_list("Land", nvto.hasRelatedPlace)
    uri_list("Spielzeit", nvto.hasSeason)
    uri_type("Spielzeit", nvto.TimeSpan)
    uri_type("Produktionstyp", skos.Concept)

    lit_list("Spielzeit_Start", nvto.beginsAtTime, "Spielzeit")
    lit_list("Spielzeit_Ende", nvto.endsAtTime, "Spielzeit")

    lit_list("Texte_Autoren", nvto.hasSource)
    lit_list("Beschreibung_Quelle", nvto.hasDescription)

for index, row in ev_clean.iterrows():

    graph = ev_graph

    uri_type("ID", nvto.Event)

    lit_list("EV_Titel", skos.prefLabel)
    lit_list("EV_Titel", rdfs.label)

    uri_list("Ereignisart", nvto.hasType)
    uri_list("Gehört_PR", nvto.isPartOf)
    uri_list("Erwähnung_von", nvto.containsReferenceTo)
    uri_list("Subject", nvto.containsReferenceTo)
    uri_list("Teilereignis_von", nvto.isPartOf)
    uri_list("Beteiligte_Gruppen", nvto.hasContributor)
    uri_list("Konzept", nvto.hasConceptOriginator)
    uri_list("Textbearbeitung", nvto.hasAdaptor)
    uri_list("Übersetzung", nvto.hasTranslator)
    uri_list("Regie", nvto.hasDirector)
    uri_list("SprecherIn", nvto.hasSpeaker)
    uri_list("Choreographie", nvto.hasChoreographer)
    uri_list("Dramaturgie", nvto.hasDramaturge)
    uri_list("Bühnenbild", nvto.hasSetDesigner)
    uri_list("Kostümdesgin", nvto.hasCostumedesigner)
    uri_list("Figurenbau", nvto.hasPuppetDesigner)
    uri_list("Maskenbau", nvto.hasMaskDesigner)
    uri_list("Lichtdesign", nvto.hasLightDesigner)
    uri_list("Videodesign", nvto.hasVideoDesigner)
    uri_list("Schauspieler", nvto.hasActor)
    uri_list("Tänzer", nvto.hasDancer)
    uri_list("Darsteller", nvto.hasPerformer)
    uri_list("Musiker", nvto.hasMusician)
    uri_list("Komposition", nvto.hasComposer)
    uri_list("Lehrer", nvto.hasMediator)
    uri_list("Teilnehmer", nvto.hasParticipant)
    uri_list("Mitwirkende", nvto.hasContributor)

    lit_list("Zeitpunkt", nvto.hasDate)
    lit_list("Zeitraum_Start", nvto.beginsAtTime, "Zeitraum")
    lit_list("Zeitraum_Ende", nvto.endsAtTime, "Zeitraum")

    uri_list("Veranstaltungsort", nvto.happenedAtPlace)
    uri_list("Stadt", nvto.happenedAtPlace)
    uri_list("Land", nvto.happenedAtPlace)

    lit_list("Beschreibung_Quelle", nvto.hasDescription)

    uri_list("Zeitraum", nvto.hasSeason)

    uri_type("Zeitraum", nvto.TimeSpan)
    uri_type("Ereignisart", skos.Concept)

for index, row in vid_clean.iterrows():

    graph = vid_graph

    uri_type("ID", nvto.InformationObject)

    uri_list("Phys_ID", nvto.hasInformationCarrier)
    uri_list("Digi_ID", nvto.hasInformationCarrier)
    uri_list("Digi_ID", nvto.hasDigitalVersion, "Phys_ID")

    uri_type("Phys_ID", nvto.PhysicalObject)
    uri_type("Digi_ID", nvto.DigitalObject)

    lit_list("andere_ID", nvto.hasIdentifier)

    uri_list("Archivalientyp", nvto.hasType)
    uri_type("Archivalientyp", skos.Concept)
    uri_list("Unterobjekt_von", nvto.isPartOf)
    uri_list("Serie", nvto.isPartOf)
    uri_list("Sammlung", nvto.isPartOf)
    uri_list("Gleicher_Inhalt", nvto.hasContentMatch)
    uri_list("Abgebildete_Produktionen", nvto.containsAudioVisualReferenceTo)
    uri_list("Erwähnte_Produktionen", nvto.containsReferenceTo)
    uri_list("Abgebildete_Ereignisse", nvto.containsAudioVisualReferenceTo)
    uri_list("Erwähnte_Ereignisse", nvto.containsReferenceTo)

    lit_list("Titel", skos.prefLabel)
    lit_list("Titel", rdfs.label)
    lit_list("Titel", nvto.hasTitle)
    lit_list("Untertitel", nvto.hasSubtitle)

    uri_list("Kamera_Aufzeichner", nvto.hasCinematographer)
    uri_list("Sichtbare_Entitäten", nvto.containsVisualReferenceTo)
    uri_list("Hörbare_Entitäten", nvto.containsAudibleReferenceTo)
    uri_list("Erwähnte_Entitäten", nvto.containsReferenceTo)
    uri_list("Erwähnte_Gruppen", nvto.containsReferenceTo)
    uri_list("Autorenschaft", nvto.hasAuthor)
    uri_list("Beitragsregie_Fernsehregie", nvto.hasDirector)
    uri_list("SprecherIn", nvto.hasSpeaker)
    uri_list("Choreographie", nvto.hasChoreographer)
    uri_list("Dramaturgie", nvto.hasDramaturge)
    uri_list("Bühnenbild", nvto.hasSetDesigner)
    uri_list("Maske", nvto.hasMaskDesigner)
    uri_list("Lichtdesign", nvto.hasLightDesigner)
    uri_list("Videodesign", nvto.hasVideoDesigner)
    uri_list("Schauspieler", nvto.hasActor)
    uri_list("Tänzer", nvto.hasDancer)
    uri_list("Darsteller", nvto.hasPerformer)
    uri_list("Musiker", nvto.hasMusician)
    uri_list("Komposition", nvto.hasComposer)
    uri_list("Mitwirkende", nvto.hasContributor)
    uri_list("Editor", nvto.hasVideoEditor)
    uri_list("Ton", nvto.hasRecordist)

    lit_list("Sprache_Objekt", nvto.language)
    lit_list("Aufnahmedatum", nvto.wasCreatedAtTime)

    uri_list("Entstehungsort", nvto.originatedAtPlace)
    uri_list("Stadt", nvto.originatedAtPlace)
    uri_list("Land", nvto.originatedAtPlace)

    lit_list("Rechteinhaber", nvto.hasRightsHolder)

    lit_list("Träger", nvto.hasMedium, "Phys_ID")
    lit_list("Länge_Band", nvto.hasExtent, "Phys_ID")
    lit_list("Bandbeschriftungen", nvto.hasLabeling, "Phys_ID")
    lit_list("Herkunft", nvto.hasProvenance, "Phys_ID")
    lit_list("Zustand_Datum", nvto.hasCondition, "Phys_ID")
    lit_list("Copyright", nvto.hasRights, "Phys_ID")

    uri_list("Objekt_identisch_mit", nvto.hasContentMatch)

    lit_list("Zustand_Digi", nvto.hasCondition, "Digi_ID")
    lit_list("Länge", nvto.hasExtent, "Digi_ID")
    lit_list("Dateiformat", nvto.hasFormat, "Digi_ID")

    lit_list("Beschreibung_Quelle", nvto.hasDescription)

for index, row in text_clean.iterrows():

    graph = text_graph

    uri_type("ID", nvto.InformationObject)

    uri_list("Phys_ID", nvto.hasInformationCarrier)
    uri_list("Digi_ID", nvto.hasInformationCarrier)

    uri_type("Phys_ID", nvto.PhysicalObject)
    uri_type("Digi_ID", nvto.DigitalObject)

    uri_list("Digi_ID", nvto.hasDigitalVersion, "Phys_ID")


    lit_list("andere_ID", nvto.hasIdentifier)

    uri_list("Archivalientyp", nvto.hasType)
    uri_type("Archivalientyp", skos.Concept)

    uri_list("Unterobjekt_von", nvto.isPartOf)
    uri_list("Serie", nvto.isPartOf)
    uri_list("Sammlung", nvto.isPartOf)
    uri_list("Gleicher_Inhalt", nvto.hasContentMatch)
    uri_list("Abgebildete_Produktionen", nvto.containsVisualReferenceTo)
    uri_list("Erwähnte_Produktionen", nvto.containsReferenceTo)
    uri_list("Abgebildete_Ereignisse", nvto.containsVisualReferenceTo)
    uri_list("Erwähnte_Ereignisse", nvto.containsReferenceTo)

    lit_list("Titel", skos.prefLabel)
    lit_list("Titel", rdfs.label)
    lit_list("Titel", nvto.hasTitle)
    lit_list("Untertitel", nvto.hasSubtitle)
    lit_list("Inhaltsverzeichnis", nvto.hasTableOfContents)
    lit_list("Herausgeberschaft", nvto.hasPublisher)

    uri_list("HerausgeberIn", nvto.hasPublisher)
    uri_list("AutorIn", nvto.hasAuthor)
    uri_list("ÜbersetzerIn", nvto.hasTranslator)
    uri_list("Layout", nvto.hasLayouter)
    uri_list("Grafik_Gestaltung", nvto.hasIllustrator)
    uri_list("Redaktion", nvto.hasEditor)
    uri_list("Fotografie", nvto.hasPhotographer)
    uri_list("Mitwirkende", nvto.hasContributor)
    uri_list("Erwähnung_Personen", nvto.containsTextualReferenceTo)
    uri_list("Erwähnung_Gruppen", nvto.containsTextualReferenceTo)
    uri_list("Sichtbare_Personen", nvto.containsVisualReferenceTo)
    uri_list("Sichtbare_Gruppen", nvto.containsVisualReferenceTo)

    lit_list("Verlag", nvto.hasPublisher)
    lit_list("Originalausgabe", nvto.hasFirstEdition)
    lit_list("Erstausgabe_Sprache", nvto.hasFirstLocalizedEdition)
    lit_list("Erscheinungsdatum", nvto.wasIssuedAtTime)

    uri_list("Erscheinungsstadt", nvto.hasPublishingPlace)

    lit_list("Sprache", nvto.language)

    uri_list("Land", nvto.hasPublishingPlace)

    lit_list("Klassifikation", nvto.hasType)
    lit_list("Textquelle", nvto.hasSource)
    lit_list("ISBN", nvto.hasIsbn)

    lit_list("Träger", nvto.hasMedium, "Phys_ID")
    lit_list("Herkunft", nvto.hasProvenance, "Phys_ID")
    lit_list("Umfang", nvto.hasExtent, "Phys_ID")
    lit_list("Phys_Format", nvto["format"], "Phys_ID")
    lit_list("Zustand_Datum", nvto.hasCondition, "Phys_ID")
    lit_list("Copyright", nvto.hasRights, "Phys_ID")

    uri_list("Objekt_identisch_mit", nvto.hasContentMatch)

    lit_list("Zustand_Digi", nvto.hasCondition, "Digi_ID")
    lit_list("Dateiformat", nvto.hasFormat, "Digi_ID")

    lit_list("Beschreibung_Quelle", nvto.hasDescription)

for index, row in img_clean.iterrows():

    graph = img_graph

    uri_type("ID", nvto.InformationObject)

    uri_list("Phys_ID", nvto.hasInformationCarrier)
    uri_list("Digi_ID", nvto.hasInformationCarrier)

    uri_type("Phys_ID", nvto.PhysicalObject)
    uri_type("Digi_ID", nvto.DigitalObject)

    uri_list("Digi_ID", nvto.hasDigitalVersion, "Phys_ID")


    lit_list("andere_ID", nvto.hasIdentifier)

    uri_list("Archivalientyp", nvto.hasType)
    uri_type("Archivalientyp", skos.Concept)

    uri_list("Unterobjekt_von", nvto.isPartOf)
    uri_list("Serie", nvto.isPartOf)
    uri_list("Sammlung", nvto.isPartOf)
    uri_list("Gleicher_Inhalt", nvto.hasContentMatch)

    uri_list("Abgebildete_Produktionen", nvto.containsVisualReferenceTo)
    uri_list("Erwähnte_Produktionen", nvto.containsTextualReferenceTo)
    uri_list("Abgebildete_Ereignisse", nvto.containsVisualReferenceTo)
    uri_list("Erwähnte_Ereignisse", nvto.containsTextualReferenceTo)

    lit_list("Bezeichner", skos.prefLabel)
    lit_list("Bezeichner", nvto.hasTitle)

    uri_list("abgebildete_Entitäten", nvto.containsVisualReferenceTo)
    uri_list("FotografIn", nvto.hasPhotographer)
    uri_list("FotografIn", nvto.hasCreator)
    uri_list("Fotostudio", nvto.hasPhotoStudio)

    lit_list("Aufnahmedatum", nvto.wasCreatedAtTime)

    uri_list("Aufnahmeort", nvto.originatedAtPlace)
    uri_list("Aufnahmestadt", nvto.originatedAtPlace)
    uri_list("Aufnahmeland", nvto.originatedAtPlace)

    lit_list("Beschriftung_Vorn", nvto.hasLabelingFront, "Phys_ID")
    lit_list("Beschriftung_Hinten", nvto.hasLabelingBack, "Phys_ID")
    lit_list("Objektbeschreibung", nvto.hasDescription, "Phys_ID")
    lit_list("Herkunft", nvto.hasProvenance, "Phys_ID")

    lit_list("Rechteinhaber", nvto.hasRightsHolder)

    lit_list("Träger", nvto.hasMedium, "Phys_ID")

    lit_list("Farbe", nvto["format"])

    lit_list("Dimensionen", nvto.hasDimensions, "Phys_ID")

    uri_list("Objekt_identisch_mit", nvto.hasContentMatch)

    lit_list("Zustand_Digi", nvto.hasCondition, "Digi_ID")
    lit_list("Dateiformat", nvto.hasFormat, "Digi_ID")

    lit_list("Copyright", nvto.hasRights, "Phys_ID")

    lit_list("Beschreibung_Quelle", nvto.hasDescription)

    lit_list("Zustand_Datum", nvto.hasCondition, "Phys_ID")

for index, row in aud_clean.iterrows():

    graph = aud_graph

    uri_type("ID", nvto.InformationObject)

    uri_list("Phys_ID", nvto.hasInformationCarrier)
    uri_list("Digi_ID", nvto.hasInformationCarrier)

    uri_type("Phys_ID", nvto.PhysicalObject)
    uri_type("Digi_ID", nvto.DigitalObject)

    uri_list("Digi_ID", nvto.hasDigitalVersion, "Phys_ID")


    lit_list("andere_ID", nvto.hasIdentifier)

    uri_list("Archivalientyp", nvto.hasType)
    uri_type("Archivalientyp", skos.Concept)

    uri_list("Unterobjekt_von", nvto.isPartOf)
    uri_list("Serie", nvto.isPartOf)
    uri_list("Sammlung", nvto.isPartOf)
    uri_list("Gleicher_Inhalt", nvto.hasContentMatch)
    uri_list("Abgebildete_Produktionen", nvto.containsAudibleReferenceTo)
    uri_list("Erwähnte_Produktionen", nvto.containsReferenceTo)
    uri_list("Abgebildete_Ereignisse", nvto.containsAudibleReferenceTo)
    uri_list("Erwähnte_Ereignisse", nvto.containsReferenceTo)

    lit_list("Titel", skos.prefLabel)
    lit_list("Titel", nvto.hasTitle)
    lit_list("Untertitel", nvto.hasSubtitle)

    uri_list("Hörbare_Entitäten", nvto.containsAudibleReferenceTo)
    uri_list("Erwähnte_Entitäten", nvto.containsReferenceTo)
    uri_list("AufzeichnerIn", nvto.hasRecordist)
    uri_list("Beitragsregie", nvto.hasDirector)
    uri_list("Erwähnte_Gruppen", nvto.containsReferenceTo)

    lit_list("Aufnahmedatum", nvto.wasCreatedAtTime)
    lit_list("Sprache", nvto.language)

    uri_list("Entstehungsort", nvto.originatedAtPlace)
    uri_list("Stadt", nvto.originatedAtPlace)
    uri_list("Land", nvto.originatedAtPlace)

    lit_list("Träger", nvto.hasMedium, "Phys_ID")
    lit_list("Länge_Band", nvto.hasExtent, "Phys_ID")
    lit_list("Herkunft", nvto.hasProvenance, "Phys_ID")

    lit_list("Rechteinhaber", nvto.hasRightsHolder)


    uri_list("Objekt_identisch_mit", nvto.hasContentMatch)

    lit_list("Zustand_Digi", nvto.hasCondition, "Digi_ID")
    lit_list("Dateiformat", nvto.hasFormat, "Digi_ID")

    lit_list("Copyright", nvto.hasRights, "Phys_ID")

    lit_list("Beschreibung_Quelle", nvto.hasDescription)

    lit_list("Zustand_Datum", nvto.hasCondition, "Phys_ID")

for index, row in person_clean.iterrows():

    graph = person_graph

    uri_type("ID", nvto.Person)

    lit_list("Vollname", skos.prefLabel)
    lit_list("Vollname", nvto.hasName)
    lit_list("Vorname", nvto.hasFirstName)
    lit_list("Nachname", nvto.hasFamilyName)
    lit_list("Geboren", nvto.hasDateOfBirth)
    lit_list("Gestorben", nvto.hasDateOfDeath)

    url_list("VIAF", nvto.hasIdentifier)
    url_list("GND", nvto.hasIdentifier)
    url_list("Wikidata", nvto.hasIdentifier)
    url_list("Website", nvto.hasHomepage)
    url_list("Websites", nvto.hasHomepage)
    url_list("dbpedia", nvto.hasIdentifier)

    lit_list("Beschreibung_Quelle", nvto.hasDescription)

for index, row in group_clean.iterrows():

    graph = group_graph

    uri_type("ID", nvto.PerformingArtsGroup)

    lit_list("Name", skos.prefLabel)
    lit_list("Namen", skos.altLabel)

    uri_list("Vorgänger_von", nvto.hasPredecessor)
    uri_list("Nachfolger_von", nvto.hasSuccessor)

    uri_list("ansässig_Stadt", nvto.hasRelatedPlace)
    uri_list("ansässig_Land", nvto.hasRelatedPlace)
    uri_list("ansässig_Haus", nvto.hasResidence)

    url_list("Website", nvto.hasHomepage)
    url_list("Wikidata", nvto.hasIdentifier)
    url_list("GND URI", nvto.hasIdentifier)

    lit_list("Beschreibung_Quelle", nvto.hasDescription)

for index, row in loc_clean.iterrows():

    graph = loc_graph

    uri_type("ID", nvto.PerformingArtsLocation)

    lit_list("Ortsname", skos.prefLabel)
    lit_list("weitere_Ortsnamen", skos.altLabel)
    lit_list("Adresse", nvto.hasAddress)

    uri_list("Gehört zu", nvto.isPartOf)
    uri_list("STADT", nvto.isPartOf)
    uri_list("LAND", nvto.isPartOf)

    lit_list("LAT", wgs84_pos.lat)
    lit_list("LONG", wgs84_pos.long)

    url_list("Wikipedia", nvto.hasIdentifier)
    url_list("GND", nvto.hasIdentifier)
    url_list("WIKIDATA", nvto.hasIdentifier)
    url_list("geonames", nvto.hasIdentifier)

    lit_list("Beschreibung_Quelle", nvto.hasDescription)

    lit_list("LOC_Institution", nvto.isResidenceOf)
    uri_type("LOC_Institution", nvto.PerformingArtsGroup)

for index, row in city_clean.iterrows():

    graph = city_graph

    uri_type("ID", nvto.City)

    lit_list("präf_Stadtname", skos.prefLabel)
    lit_list("Stadtname_DE", skos.altLabel, language="de")
    lit_list("Stadtname_EN", skos.altLabel, language="en")

    uri_list("Land", nvto.isPartOf)

    lit_list("LAT", wgs84_pos.lat)
    lit_list("LONG", wgs84_pos.long)

    url_list("geonames", nvto.hasIdentifier)

for index, row in country_clean.iterrows():

    graph = country_graph

    uri_type("ID", nvto.Country)

    lit_list("präf_Landname", skos.prefLabel)
    lit_list("Landname_DE", skos.altLabel, language="de")
    lit_list("Landname_EN", skos.altLabel, language="en")

    lit_list("LAT", wgs84_pos.lat)
    lit_list("LONG", wgs84_pos.long)

    url_list("geonames", nvto.hasIdentifier)

for index, row in col_clean.iterrows():

    graph = col_graph

    uri_type("ID", nvto.Collection)

    lit_list("Sammlungstitel", skos.prefLabel)
    lit_list("Sammlungstitel", nvto.hasTitle)

    lit_list("Beschreibung_Quelle", nvto.hasDescription)

for index, row in series_clean.iterrows():

    graph = series_graph

    uri_type("ID", nvto.Series)

    lit_list("Serientitel", skos.prefLabel)
    lit_list("Serientitel", nvto.hasTitle)

    lit_list("Beschreibung_Quelle", nvto.hasDescription)

with open('nvt_ds.trig', 'wb') as f_trig:
    f_trig.write(nvt_ds.serialize(format="trig"))

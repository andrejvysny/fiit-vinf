# 1. Odovzdanie – Report k 31. 10. 2025

> _Táto sekcia musí obsahovať stručný report pokrývajúci každý bod zadania._

## 1.1 Cieľ projektu
- **Problémová doména**: _stručný opis_ 
- **Výsledok**: _čo systém umožní používateľovi_

## 1.2 Zdrojové stránky a extrahované dáta
| URL / doména | Popis obsahu | Extrahované atribúty (min. 5 ukážok) | Poznámky |
| --- | --- | --- | --- |
| https://github.com/... | _napr. repozitár_ | - Názov projektu\n- Hviezdičky\n- … | _licencia_

_(Priložte ukážky – text, JSON, TSV – vytvorené v Task 3.)_

## 1.3 Q&A scenáre
- **Scenár 1**: _otázka → odpoveď_
- … (minimálne 5 scenárov s mapovaním na dáta)

## 1.4 Použité technológie
| Vrstva | Nástroj / knižnica | Odôvodnenie |
| --- | --- | --- |
| Crawler | `httpx`, `asyncio` | _HTTP/2, rate limiting_
| Extraktor | `BeautifulSoup`, regexy | _HTML parsing_
| Indexer | `indexer` modul | _vlastné riešenie_

## 1.5 Architektúra
- **Diagram**: _vložiť existujúci diagram (odkaz / obrázok)_
- **Popis pipeline**: _bullet list od crawl až po vyhľadávanie_

## 1.6 Metadáta pri crawlovaní
| Dokument | URL | HTTP stav | Page type | Veľkosť | Ďalšie meta |
| --- | --- | --- | --- | --- | --- |
| _príklad_ | | | | | |

## 1.7 Hlavičky, timeouty, sleep
- **Zhrnutie**: _User-Agent, Accept-*, timeouty, spánkové intervaly_
- **Odôvodnenie**: _prečo sú hodnoty etické a spoľahlivé_

## 1.8 Ukážka kódu – extrakcia URL z HTML
```python
# vložte priamo reálnu funkciu z projektu (napr. crawler/extractor.py::LinkExtractor.extract)
```
(_Nezabudnite priložiť aj link na zdroják v Gite/wiki._)

## 1.9 Ukážka kódu – extrakcia entít
```python
# napr. RegexEntityExtractor.extract – priamo z projektu
```

## 1.10 Indexer a index
- **Popis TF výpočtu**: _ako sa ukladajú term-frequency údaje_
- **Interval rebuildov**: _ako často prepočítavate index_
- **Štruktúra uloženia**: _súbory / databáza_

## 1.11 Metódy IDF
| Metóda | Vzorec | Implementačné poznámky |
| --- | --- | --- |
| IDF Log | `log((N + 1)/(df + 1)) + 1` | _odkaz na kód_
| IDF RSJ | `log((N - df + 0.5)/(df + 0.5))` | _odkaz_

## 1.12 Porovnanie IDF metód (min. 3 dopyty)
| Query | Top výsledky (Log) | Top výsledky (RSJ) | Pozorovania |
| --- | --- | --- | --- |
| `query1` | _doc + score_ | _doc + score_ | _rozdiely_

_(Generuje Task 6.)_

## 1.13 Štatistika dokumentov
| Metrika | Hodnota | Zdroj |
| --- | --- | --- |
| Počet HTML | _n_ | `tools/crawl_stats.py`
| Veľkosť na disku | _MB/GB_ | |
| Relevantných dokumentov | _%_ | _kritérium_

## 1.14 Prílohy
- **ZIP s kódom**: _link / cesta_
- **Unit testy REGEX** (bonus): _odkaz na zoznam/zip_
- **Token counts (tiktoken)** (bonus): _tabuľka alebo link_

---
_Späť: [2. Konzultácia](./konzultacia-2) · Ďalej: [3. Konzultácia](./konzultacia-3)_

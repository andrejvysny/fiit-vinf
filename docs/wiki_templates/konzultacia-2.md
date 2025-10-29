# 2. Konzultácia – Návrh a Implementácia Crawlera

> _Termín 2a: 17. 10. 2025 · Termín 2b: 31. 10. 2025._

## 2a – Návrh riešenia
### 2a.1 Použité frameworky a knižnice
| Vrstva | Knižnica / Framework | Dôvod použitia | Stav |
| --- | --- | --- | --- |
| Crawling | `httpx`, `asyncio` | _napr. HTTP/2 client_ | _hotovo / v procese_
| Parsovanie | `BeautifulSoup`, regexy | _prečo_ | |
| Ukladanie | _napr. JSONL / SQLite_ | | |

### 2a.2 Architektúra
- **Diagram**: _vložiť prelinkovanie na obrázok/diagram (napr. `![[media:diagram.png]]`)_
- **Popis komponentov**:
  1. _Frontier management_ – …
  2. _Policy enforcement_ – …
  3. _Fetcher_ – …

### 2a.3 Komunikačné hlavičky a limity
| Parameter | Hodnota | Odôvodnenie |
| --- | --- | --- |
| `User-Agent` | `ResearchCrawlerBotVINF/2.0 (+mailto:...)` | _kontaktné info, etiketa_ |
| `Accept-Language` | `en` | |
| Timeout (connect/read) | _ms_ | _bezpečnostné rezervy_ |
| Sleep per request | 3–5 s | _mimic human behaviour_ |
| Batch pause | 10–20 s / 50 req | |

### 2a.4 Implementačný stav
- **Repozitár / branch**: _odkaz_
- **Ukončené úlohy**: _bullet list_
- **Riziká**: _čo treba konzultovať_

## 2b – Dokončený crawler a extraktor
### 2b.1 Stiahnuté dáta
| Metrika | Hodnota | Poznámka |
| --- | --- | --- |
| Počet HTML súborov | _#_ | _generovať cez Task 2_
| Veľkosť na disku | _MB/GB_
| Relatívne relevantné dokumenty | _% + definícia relevancie_

### 2b.2 Extraktory (REGEX)
- **Ukážky**: _vložiť aspoň 3 konkrétne regex prípady + vysvetlenie_
- **Unit testy**: _počty testovaných strán, výstup z behu testov_
- **Príklad TSV riadku**:
  ```
  doc_id	LICENSE	MIT License	...
  ```

### 2b.3 Indexer a IDF metódy
- **Opis indexera**: _architektúra, dátové štruktúry_
- **Metóda IDF #1 (napr. log N/df)**: vzorec, implementačné poznámky
- **Metóda IDF #2 (napr. RSJ)**: vzorec, implementačné poznámky
- **Porovnanie na dopytoch**: odkaz na tabuľku (Task 6).

### 2b.4 Štatistiky a bonusy
- **Štatistika tokenov (`tiktoken`)**: _ak implementované_
- **Ďalšie bonusy**: _napr. pipeline vizualizácia_

### 2b.5 Otvorené body
- _Zoznam otázok na konzultáciu_
- _Plánované zlepšenia pred odovzdaním_

---
_Späť: [1. Konzultácia](./konzultacia-1) · Ďalej: [1. Odovzdanie](./odovzdanie-1)_

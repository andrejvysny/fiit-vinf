# VINF - Konzultácia 4

**Autor**: Andrej Vyšný
**GitHub**: https://github.com/andrejvysny/fiit-vinf
**Termín**: 28. 11. 2025

---

## 1. Spark kód

### 1.1 HTML Extractor (`spark/jobs/html_extractor.py`)

Extrakcia textu a entít z HTML súborov pomocou PySpark DataFrame API.

**Extrahované entity**:
- `TOPICS` - témy repozitára
- `LANG_STATS` - programovacie jazyky
- `LICENSE` - licencia (MIT, Apache-2.0, ...)
- `STAR_COUNT`, `FORK_COUNT` - počet hviezd a forkov

**Výstupy**:
- `workspace/store/text/*.txt` - čistý text
- `workspace/store/entities/entities.tsv` - entity (doc_id, type, value, offsets)

### 1.2 Wikipedia Extractor (`spark/jobs/wiki_extractor.py`)

Spracovanie Wikipedia XML dumpov (100GB+) pomocou streaming architektúry.

**Výstupy** (7 TSV súborov):
| Súbor | Obsah |
|-------|-------|
| `pages.tsv` | metadáta stránok (page_id, title, redirect) |
| `categories.tsv` | kategórie stránok |
| `aliases.tsv` | presmerovania (aliasy) |
| `abstract.tsv` | úvodné odseky článkov |
| `links.tsv` | interné odkazy |
| `infobox.tsv` | štruktúrované dáta z infoboxov |

### 1.3 Join (`spark/jobs/join_html_wiki.py`)

Prepojenie entít z HTML s Wikipedia stránkami.

**Postup**:
1. Normalizácia názvov (lowercase, ASCII, odstránenie interpunkcie)
2. Vytvorenie mapovania: priame stránky + aliasy (redirecty)
3. Left join entít s Wikipedia
4. Výpočet confidence (exact+cat, alias+cat, exact+abs, alias+abs)

**Výstupy**:
- `html_wiki.tsv` - prepojenia s wiki_title, wiki_abstract, wiki_categories
- `html_wiki_agg.tsv` - agregácie per dokument
- `join_stats.json` - štatistiky (unique_wiki_pages_joined)

---

## 2. PyLucene Index

### 2.1 Schéma indexu (`lucene_indexer/schema.py`)

| Pole | Typ | Odôvodnenie |
|------|-----|-------------|
| `doc_id` | StringField | Identifikátor dokumentu pre exact lookup |
| `title` | TextField | Full-text vyhľadávanie v titulkoch |
| `content` | TextField | Hlavný obsah (neukladá sa, len indexuje) |
| `topics` | TextField (multi) | GitHub témy, tokenizované pre čiastočné zhody |
| `languages` | TextField (multi) | Programovacie jazyky |
| `license` | StringField | Presná zhoda licencie |
| `star_count` | IntPoint | Range queries (napr. >1000 hviezd) |
| `fork_count` | IntPoint | Range queries |
| `wiki_title` | TextField (multi) | Názvy Wikipedia článkov |
| `wiki_categories` | TextField (multi) | Wikipedia kategórie |
| `wiki_abstract` | TextField | Abstrakt z Wikipédie pre phrase queries |
| `join_confidence` | StringField | Kvalita prepojenia s wiki |

### 2.2 Typy polí

- **TextField** - tokenizované, full-text vyhľadávanie
- **StringField** - netokenizované, presná zhoda
- **IntPoint/LongPoint** - numerické range queries

---

## 3. Typy dopytov (`lucene_indexer/search.py`)

| Typ | Popis | Príklad |
|-----|-------|---------|
| **Simple** | Full-text cez viacero polí | `python web framework` |
| **Boolean** | AND/OR/NOT operátory | `python AND docker` |
| **Range** | Numerický rozsah | `star_count:[1000 TO *]` |
| **Phrase** | Presná fráza | `"machine learning"` |
| **Fuzzy** | Tolerancia preklepov (Levenshtein) | `pyhton` → `python` |

**Príklady použitia**:
```bash
# Simple query
python -m lucene_indexer.search --query "python web" --type simple

# Boolean query
python -m lucene_indexer.search --query "python AND docker" --type boolean

# Range query (populárne repozitáre)
python -m lucene_indexer.search --type range --field star_count --min-value 1000

# Phrase query
python -m lucene_indexer.search --query "neural network" --type phrase

# Fuzzy query (preklep)
python -m lucene_indexer.search --query "pyhton" --type fuzzy
```

---

## 4. Porovnanie: Starý vs Nový index

| Aspekt | TF-IDF (starý) | PyLucene (nový) |
|--------|----------------|-----------------|
| Scoring | Vlastný TF-IDF | BM25 |
| Query typy | Len simple | Boolean, Range, Phrase, Fuzzy |
| Entity polia | Žiadne | topics, languages, stars, license |
| Wikipedia dáta | Žiadne | wiki_title, wiki_categories, wiki_abstract |
| Range queries | Nepodporované | IntPoint queries |
| Fuzzy matching | Nepodporované | Levenshtein distance |

**Nástroj na porovnanie** (`lucene_indexer/compare.py`):
```bash
python -m lucene_indexer.compare --queries "python,docker,web" --output report.md
```

Metriky: Overlap@K, latencia, počet výsledkov.

---

## 5. GitHub

**Repository**: https://github.com/andrejvysny/fiit-vinf

**Hlavné súbory**:
```
spark/jobs/
├── html_extractor.py    # HTML → text + entity
├── wiki_extractor.py    # Wikipedia dump extraction
└── join_html_wiki.py    # Entity-Wiki join

lucene_indexer/
├── schema.py            # Schéma indexu + odôvodnenia
├── build.py             # Budovanie indexu
├── search.py            # Vyhľadávanie (5 typov dopytov)
└── compare.py           # Porovnanie TF-IDF vs Lucene
```

# 1. Odovzdanie – Report k 31. 10. 2025

> _Táto sekcia musí obsahovať stručný report pokrývajúci každý bod zadania._

## 1.1 Cieľ projektu
- **Problémová doména**: Automatizovaný zber GitHub repozitárov, tém a issues/pull requests s dôrazom na etické crawlovanie a následnú textovú analýzu.
- **Výsledok**: Pipeline umožňuje spustiť asynchrónny crawler, persistovať HTML + metadáta, extrahovať text i entity do TSV a budovať vyhľadávací index pre ad hoc dopyty nad získaným korpusom.

## 1.2 Zdrojové stránky a extrahované dáta
| URL / doména | Popis obsahu | Extrahované atribúty (min. 5 ukážok) | Poznámky |
| --- | --- | --- | --- |
| https://github.com/python/cpython | Oficiálny repozitár CPythonu (release informácie, dokumentácia, prispievateľské odkazy) | - `STAR_COUNT: 69,369`<br>- `FORK_COUNT: 33,101`<br>- `LICENSE: /python/cpython/blob/main/LICENSE`<br>- `README_SECTION: "This is Python version 3.15.0..."`<br>- `VERSION: 3.15.0` | `page_type=repo_root`, doc_id `d44f9b6…` (`workspace/metadata/crawl_metadata.jsonl`, `workspace/store/entities/entities.tsv`) |
| https://github.com/torvalds/linux | Repozitár Linux jadra (návody na build, governance, podpora) | - `STAR_COUNT: 205,018`<br>- `FORK_COUNT: 57,875`<br>- `LICENSE: /torvalds/linux/tree/master/LICENSES`<br>- `README_SECTION: "Linux kernel There are several guides..."`<br>- `URL: https://www.kernel.org/doc/html/latest/` | `page_type=repo_root`, doc_id `df769559…` (`workspace/metadata/crawl_metadata.jsonl`, `workspace/store/entities/entities.tsv`) |
| https://github.com/topics/sqlmodel | GitHub Topics landing page pre SQLModel projekty | - `STAR_COUNT: 38,350`<br>- `TOPIC: react`<br>- `TOPIC: python`<br>- `TOPIC: docker`<br>- `TOPIC: sqlmodel` | `page_type=topic`, doc_id `0002297e…` (`workspace/metadata/crawl_metadata.jsonl`, `workspace/store/entities/entities.tsv`) |

_(Priložte ukážky – text, JSON, TSV – vytvorené v Task 3.)_

## 1.3 Q&A scenáre
- **Scenár 1**: „Koľko hviezdičiek má python/cpython?“ → „69,369 podľa `STAR_COUNT` v doc `d44f9b6…` (workspace/store/entities/entities.tsv).“
- **Scenár 2**: „Aké hlavné témy pokrýva topic/sqlmodel?“ → „Najčastejšie `react`, `python`, `docker`, `sqlmodel`, `typescript` (entity `TOPIC` v doc `0002297e…`).“
- **Scenár 3**: „Koľko forkov má torvalds/linux?“ → „57,875 (`FORK_COUNT` v doc `df769559…`).“
- **Scenár 4**: „Ktorý user agent použil crawler pri topic/sqlmodel?“ → „`Mozilla/5.0 … Firefox/47.0` z metadát záznamu `0002297e…` (`workspace/metadata/crawl_metadata.jsonl`).“
- **Scenár 5**: „Kde nájdem README text pre Kubernetes?“ → „`README_SECTION` extrahovaný v doc `8b452ba3…`, uložený v `workspace/store/entities/entities.tsv` a v súbore `workspace/store/readme/.../8b452ba3….txt`.“

## 1.4 Použité technológie
| Vrstva | Nástroj / knižnica | Odôvodnenie |
| --- | --- | --- |
| Crawler | `asyncio`, `httpx`, `re`, `json` | Asynchrónny HTTP/2 fetcher s rate limitingom, regex link extraction a JSONL perzistencia (pozri `crawler/unified_fetcher.py`, `crawler/extractor.py`). |
| Extraktor | `re`, `html`, vlastné moduly (`extractor/html_clean.py`, `extractor/entity_extractors.py`) | Čisto regexový stripping GitHub UI, tvorba TSV cez `TSVWriter`, deduplikácia entít. |
| Indexer | Balík `indexer` (`ingest.py`, `build.py`, `search.py`) | Tokenizácia (regex `[A-Za-z0-9]+`), TF výpočty a viacnásobné IDF tabuľky, súbory `docs.jsonl`/`postings.jsonl`. |
| CLI nástroje | `argparse`, `pathlib`, `logging` | Jednotné spúšťanie (`main.py`, `tools/crawl_stats.py`, `python -m extractor`). |

## 1.5 Architektúra
- **Diagram**: _Textová referencia v `docs/ARCHITECTURE.md`; vizuálny diagram bude doplnený v ďalšom odovzdaní._
- **Popis pipeline**: 
  - Seed URL → `CrawlerScraperService` (`crawler/service.py:42`).
  - Fetch + ukladanie HTML (`crawler/unified_fetcher.py:39`) → metadata (`crawler/metadata_writer.py`).
  - Link extraction a frontier (`crawler/extractor.py`, `crawler/crawl_frontier.py`).
  - Regex extrakcia textov a entít (`extractor/entity_main.py` → TSV/README/Text).
  - Index build & vyhľadávanie (`python -m indexer.build`, `python -m indexer.query`).

## 1.6 Metadáta pri crawlovaní
| Dokument | URL | HTTP stav | Page type | Veľkosť | Ďalšie meta |
| --- | --- | --- | --- | --- | --- |
| `d44f9b6b7bf81bd8cd7b8b7fb0fb9a06316fd11ddb0d1e2fc9cd0967e0a4ada5` | https://github.com/python/cpython | 200 | repo_root | 381.76 KB | Latencia 863.3 ms, UA `ResearchCrawlerBotVINF/2.0 (+mailto:xvysnya@stuba.sk)` (`workspace/metadata/crawl_metadata.jsonl`). |
| `df76955945858d1597325eef7ee46d4f02ff2c11bc260806ccfec4c9aa79272d` | https://github.com/torvalds/linux | 200 | repo_root | 305.07 KB | Latencia 646.7 ms, UA `ResearchCrawlerBotVINF/2.0 (+mailto:xvysnya@stuba.sk)` (`workspace/metadata/crawl_metadata.jsonl`). |
| `0002297ed0450b8dbb47cfd695075755b36ce04247e6e47aa6482dbb5018790b` | https://github.com/topics/sqlmodel | 200 | topic | 491.03 KB | Latencia 1120.3 ms, UA `Mozilla/5.0 … Firefox/47.0` (`workspace/metadata/crawl_metadata.jsonl`). |

## 1.7 Hlavičky, timeouty, sleep
- **Zhrnutie**: `User-Agent` rotácia (3 hodnoty, `config.yaml:8`), `Accept-Language: en`, `Accept-Encoding: br, gzip`, `Connect timeout: 4000 ms`, `Read timeout: 15000 ms`, `req_per_sec: 1.0`, per-request spánok 3–5 s, batch spánok 10–20 s po 50 požiadavkách (`config.yaml`, `crawler/unified_fetcher.py:53`, `crawler/service.py:152`).
- **Odôvodnenie**: Kombinácia vlastného UA s kontaktom + bežných UA znižuje blokovanie, nízky request rate a dlhšie spánky chránia GitHub infraštruktúru, timeouty bránia visiacim spojeniam – politeness podľa GitHub guidelines.

## 1.8 Ukážka kódu – extrakcia URL z HTML
```python
# crawler/extractor.py:10
class LinkExtractor:
    
    def extract(self, html_content: str, base_url: str) -> List[str]:
        if not html_content or not base_url:
            return []
        
        try:
            # Regex pattern to match href attributes in <a> tags
            # Handles both quoted (single/double) and unquoted href values
            pattern = r"""<a\s+[^>]*?href\s*=\s*(?:["']([^"']+)["']|([^\s>]+))"""
            
            seen = set()
            results = []
            
            for match in re.finditer(pattern, html_content, re.IGNORECASE | re.DOTALL):
                # Extract href value from either capture group (quoted or unquoted)
                href = match.group(1) or match.group(2)
                
                # Unescape HTML entities (e.g., &amp; -> &)
                href = html.unescape(href).strip()
                
                if not href or href.startswith("#") or href.startswith("javascript:"):
                    continue
                
                if href.startswith("mailto:") or href.startswith("tel:"):
                    continue
                absolute_url = urljoin(base_url, href)
                
                if not self._is_supported_scheme(absolute_url):
                    continue

                url_without_fragment = self._remove_fragment(absolute_url)

                # Track unique URLs while preserving appearance order
                if url_without_fragment not in seen:
                    seen.add(url_without_fragment)
                    results.append(url_without_fragment)
            
            logger.debug(f"Extracted {len(results)} links from {base_url}")
            return results
            
        except Exception as e:
            logger.error(f"Error extracting links from {base_url}: {e}")
            return []
```

## 1.9 Ukážka kódu – extrakcia entít
```python
# extractor/entity_extractors.py:432
def extract_urls(doc_id: str, html_content: str, text_content: str) -> List[EntityRow]:
    """Extract URLs from HTML and text.

    Args:
        doc_id: Document identifier
        html_content: Raw HTML string
        text_content: Preprocessed text string

    Returns:
        List of entity rows
    """
    results = []
    seen = set()
    url_patterns = regexes.get_url_regexes()

    # HTTP URLs from HTML
    for match in url_patterns['http'].finditer(html_content):
        url = match.group(0).strip()
        if url and url not in seen:
            # Store raw URL
            offsets = [{
                'start': match.start(),
                'end': match.end(),
                'source': 'html'
            }]

            results.append((
                doc_id,
                'URL',
                url,
                json.dumps(offsets, separators=(',', ':'))
            ))
            seen.add(url)

    # Markdown links from text
    for match in url_patterns['markdown'].finditer(text_content):
        url = match.group(2).strip()
        if url and url not in seen:
            # Store raw URL
            offsets = [{
                'start': match.start(),
                'end': match.end(),
                'source': 'text'
            }]

            results.append((
                doc_id,
                'URL',
                url,
                json.dumps(offsets, separators=(',', ':'))
            ))
            seen.add(url)

    return results
```
(_Nezabudnite priložiť aj link na zdroják v Gite/wiki._)

## 1.10 Indexer a index
- **Popis TF výpočtu**: `indexer/ingest.py:63` buduje `term_freq` ako raw počty tokenov na dokument; skórovanie používa `1 + log(tf)` (`indexer/search.py:107`).
- **Interval rebuildov**: Po každom väčšom behu extraktora (`python -m extractor`) manuálne spúšťame `python -m indexer.build --input workspace/store/text --output workspace/store/index/dev` (cca raz za deň testovania).
- **Štruktúra uloženia**: Index v `workspace/store/index/default` (`docs.jsonl`, `postings.jsonl`, `manifest.json`) – zapisované atomicky cez `indexer/store.py`.

## 1.11 Metódy IDF
| Metóda | Vzorec | Implementačné poznámky |
| --- | --- | --- |
| IDF Log | `log((N + 1)/(df + 1)) + 1` | Implementované v `indexer/build.py:34` (funkcia `compute_idf`), kladné váhy aj pre df=N. |
| IDF RSJ | `log((N - df + 0.5)/(df + 0.5))` | `indexer/build.py:41` s clampovaním čitateľa/menovateľa na ≥0.5, voliteľné cez `--idf-method rsj`. |

## 1.12 Porovnanie IDF metód (min. 3 dopyty)
| Query | Top výsledky (Log) | Top výsledky (RSJ) | Pozorovania |
| --- | --- | --- | --- |
| `github crawler` | 1. `14558` (27.8478, ":root {")<br>2. `6827` (26.8188, ":root {")<br>3. `650` (26.6225, ":root {")<br>4. `18808` (25.0612, ":root {")<br>5. `7529` (24.5381, ":root {") | 1. `14558` (-11.3665, ":root {")<br>2. `6827` (-12.1840, ":root {")<br>3. `650` (-12.9229, ":root {")<br>4. `25240` (-13.9471, "Explore")<br>5. `5984` (-14.0955, ":root {") | RSJ presunul dokument `25240` do top-5, inak zhodný top-3 (docs/generated/index_comparison.md). |
| `async http client` | 1. `1146` (46.5926, ":root {")<br>2. `7875` (44.5243, ":root {")<br>3. `24045` (43.4540, ":root {")<br>4. `13708` (40.4020, ":root {")<br>5. `9958` (38.6749, ":root {") | Rovnaká päťka so skóre `28.8809`, `28.3499`, `27.8492`, `25.8950`, `24.3844` | Obidve metódy dávajú identický ranking – dopyt obsahuje menej frekventované termy. |
| `repository metadata` | 1. `27565` (24.0270, ":root {")<br>2. `20164` (19.7539, ":root {")<br>3. `11332` (19.6636, ":root {")<br>4. `14108` (19.6636, ":root {")<br>5. `12824` (19.3697, ":root {") | 1. `11332` (11.8861, ":root {")<br>2. `14108` (11.8861, ":root {")<br>3. `20164` (10.4110, ":root {")<br>4. `27565` (10.1356, ":root {")<br>5. `26051` (9.9717, ":root {") | RSJ vymenil poradie celej top-5 a pridal `26051`; naznačuje, že log-IDF zvýhodňuje frekventované termy. |

_(Generuje Task 6.)_

## 1.13 Štatistika dokumentov
| Metrika | Hodnota | Zdroj |
| --- | --- | --- |
| Počet HTML | 28,353 | `workspace/state/service_stats.json` (`html_files_count`) |
| Veľkosť na disku | 10.02 GB | `workspace/state/service_stats.json` (`html_storage_formatted`) |
| Relevantných dokumentov | N/A – relevantné page typy neboli definované | `docs/generated/crawl_stats.md` (stĺpec Relevant prázdny) |

## 1.14 Prílohy
- **ZIP s kódom**: Repozitár dostupný priamo v Gite (`git clone` namiesto ZIPu).
- **Unit testy REGEX** (bonus): `tests/test_link_extractor.py`, `tests/test_html_text_extractor.py`, `tests/test_crawler_service.py`.
- **Token counts (tiktoken)** (bonus): Nezapnuté – voliteľné cez `python -m indexer.build --use-tokens` (vyžaduje `tiktoken`).

---
_Späť: [2. Konzultácia](./konzultacia-2) · Ďalej: [3. Konzultácia](./konzultacia-3)_

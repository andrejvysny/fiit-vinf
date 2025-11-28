Stručné zhrnutie fungovania režimu Simple (PyLucene):

- Používa `StandardAnalyzer` a `MultiFieldQueryParser` nad poliami `title`, `content`, `topics`, `languages`, `wiki_title`, `wiki_abstract`.
- Predvolený operátor je OR, takže stačí zhoda na ľubovoľnom z polí; prázdny alebo whitespace dopyt sa okamžite vracia ako prázdny výsledok.
- Parsovanie dopytu sa pokúsi o priamu interpretáciu, pri chybe sa použije escapovaný text, aby sa dopyt napriek chybe spustil.
- Výsledky sa získajú cez `IndexSearcher.search(query, top_k)` a mapujú sa do `SearchResult` s metadátami (napr. `doc_id`, `title`, `path`, `topics`, `languages`, `star_count`, wiki údaje).
- Definované boosty polí sú zatiaľ v Simple režime nepoužité; hľadanie je čisté multi-field fulltext bez ďalších filtrov.

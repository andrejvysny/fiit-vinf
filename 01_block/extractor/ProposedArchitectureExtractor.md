# Extractor Architecture Specification

## 1. Scope and Objectives
- Refresh the `extractor/` package into a modular, testable pipeline able to ingest HTML documents, normalize content, run multiple extraction modules, and persist artifacts (text, entities, READMEs, etc.).
- Provide a developer-facing specification with clear component boundaries, extensibility rules, and configuration story that supports rapid addition of new extraction capabilities.
- Maintain streaming-friendly processing over large corpora while improving observability, error isolation, and reuse across downstream services (indexer, analytics).

## 2. Summary of Current State
- **Entry Point**: `extractor/__main__.py` wires CLI args to `ExtractorPipeline`, but pipeline is effectively a thin loop calling `HtmlFileProcessor`.
- **Processing Model**: `HtmlFileProcessor.process` performs every operation imperatively (read file → produce text → extract entities → write outputs) with inline control flow. There is no notion of pipeline stages or shared abstractions.
- **Extraction Logic**: `entity_extractors.py` houses unrelated regex-based functions returning raw tuples; additions require editing a monolithic `extract_all_entities` function.
- **Outputs**: A single `outputs.py` module mixes TSV writing utilities with text/readme/preprocessed writers, yet other modules still try to import `output_entities`, `output_text`, etc., leading to tight coupling and naming drift.
- **Data Model**: Entities and stats are plain tuples/structs without richer metadata, making downstream validation, deduplication, or provenance tracking difficult.
- **Extensibility Pain Points**:
  - Adding a new extractor requires modifications in multiple places and changing shared code paths.
  - No hook for optional transforms (e.g., additional normalization, language-specific cleanup).
  - Output handling and entity generation are entwined; piping results to alternate sinks is non-trivial.

## 3. Design Principles
- **Stage-Oriented Pipeline**: Break processing into well-defined stages with explicit contracts so each concern is independently testable and replaceable.
- **Composable Extractors**: Introduce a plugin architecture where extractors register themselves and declare their inputs/outputs.
- **Strong Domain Model**: Use dataclasses for documents, artifacts, and entities to capture metadata, offsets, confidence scores, and provenance.
- **Separation of Concerns**: Decouple IO, transforms, extraction logic, and persistence. Each layer should depend on interfaces rather than concrete implementations.
- **Config-Driven**: All stage wiring, enabled extractors, output sinks, and sampling rules should be configurable via `config.yml`, with sane defaults.
- **Streaming & Resilience**: Process documents lazily, isolate per-stage exceptions, and capture detailed stats to aid monitoring.
- **Testability**: Favor dependency injection and pure functions where possible, provide fixtures/mocks for IO, and support stage-level unit tests plus pipeline integration tests.

## 4. High-Level Architecture
```
CLI → Config Loader → PipelineBuilder
        │
        ▼
  Pipeline (stage executor)
        │
        ├─ DiscoveryStage (HTML sources)
        ├─ LoadStage (read HTML)
        ├─ TransformStages (HTML → normalized artifacts)
        ├─ ExtractionStage (run registered extractors)
        └─ OutputStages (persist artifacts/entities, emit metrics)
```
- **PipelineBuilder** composes the stage sequence based on configuration and available plugins.
- **PipelineContext** holds shared services (config, IO adapters, stats recorder, logger, clock).
- Each stage conforms to a shared interface and may register artifacts/entities on the document or update the context.

## 5. Proposed Module Layout
```
extractor/
├── cli/
│   └── main.py                  # CLI entry point (replaces __main__)
├── config/
│   ├── model.py                 # Dataclasses for extractor config
│   └── loader.py                # Validation, schema defaults
├── core/
│   ├── context.py               # PipelineContext, dependency container
│   ├── document.py              # Document, Artifact, Entity models
│   ├── pipeline.py              # Pipeline + PipelineBuilder
│   ├── stats.py                 # File stats + aggregate summary
│   └── errors.py                # Custom exception hierarchy
├── io/
│   ├── discovery.py             # HtmlRepository / discovery strategies
│   ├── readers.py               # HtmlReader (file, archive, etc.)
│   ├── writers.py               # Artifact writers, TSV writer
│   └── filesystem.py            # Shared file helpers (replaces io_utils)
├── transforms/
│   ├── base.py                  # Transform interface
│   ├── html_clean.py            # Existing logic refactored into classes
│   └── builtin/...
├── extraction/
│   ├── base.py                  # EntityExtractor interface + result objects
│   ├── registry.py              # Plugin registration & discovery
│   ├── entities.py              # Entity dataclasses, enums
│   └── builtin/
│       ├── repository_meta.py   # stars/forks/lang stats
│       ├── readme.py
│       ├── licenses.py
│       ├── topics.py
│       ├── imports.py
│       ├── urls.py
│       ├── issues.py
│       ├── versions.py
│       ├── emails.py
│       └── code_languages.py
├── outputs/
│   ├── base.py                  # ArtifactWriter interface
│   ├── entities.py              # Entities TSV writer, streaming flush
│   ├── text.py                  # Raw/preprocessed/README writers
│   └── sink_manager.py          # Manages lifecycle of writers
├── stages/
│   ├── base.py                  # Stage interface & lifecycle hooks
│   ├── discovery.py             # DiscoveryStage implementation
│   ├── load.py                  # LoadStage
│   ├── transform.py             # TransformStage orchestrating Transform modules
│   ├── extraction.py            # Runs registered EntityExtractors
│   └── output.py                # Writes artifacts/entities
├── plugins/                     # Optional entry points for external modules
│   └── __init__.py
└── utils/
    └── regexes.py               # Existing regexes reorganized by feature
```

## 6. Core Domain Model
```python
# extractor/core/document.py
@dataclass
class DocumentRef:
    path: Path
    doc_id: str
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Artifact:
    name: str
    content: Union[str, bytes]
    format: Literal["text", "json", "binary"]
    source: Literal["html", "preprocessed", "derived"]
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class EntityOffset:
    start: int
    end: int
    source: Literal["html", "text", "custom"]

@dataclass
class Entity:
    doc_id: str
    type: str
    value: str
    offsets: List[EntityOffset]
    attributes: Dict[str, Any] = field(default_factory=dict)
    confidence: Optional[float] = None

@dataclass
class Document:
    ref: DocumentRef
    html: Optional[str] = None
    artifacts: Dict[str, Artifact] = field(default_factory=dict)
    entities: List[Entity] = field(default_factory=list)
    flags: Dict[str, Any] = field(default_factory=dict)
```
- **Artifact naming**: Use constants (e.g., `ArtifactNames.RAW_TEXT`, `ArtifactNames.PREPROCESSED_TEXT`, `ArtifactNames.README_HTML`).
- **Document lifecycle**: `Document` starts with a `DocumentRef`, stages enrich it as processing proceeds.

## 7. Pipeline Contracts
### Stage Interface (`stages/base.py`)
```python
class Stage(Protocol):
    name: str
    def setup(self, ctx: PipelineContext) -> None: ...
    def process(self, doc: Document, ctx: PipelineContext) -> Optional[Document]: ...
    def teardown(self, ctx: PipelineContext) -> None: ...
```
- `setup` runs once before processing begins (initialize writers, caches).
- `process` may mutate the document and must return it (or `None` to skip downstream stages).
- `teardown` flushes resources/errors at the end.

### Pipeline Execution (`core/pipeline.py`)
1. Builder resolves enabled stages in order: discovery → load → transforms → extraction → outputs.
2. `DiscoveryStage.process` yields `DocumentRef` objects; pipeline wraps them in `Document` instances.
3. Later stages receive fully populated `Document` objects and may short-circuit on failure while recording stats.
4. Pipeline collects stats via `core/stats.py` and sinks them to logs/metrics.

## 8. Stage Responsibilities
- **DiscoveryStage**
  - Inputs: `ExtractorConfig.inputs` (paths, glob patterns, optional filters).
  - Uses `io.discovery.HtmlRepository` to enumerate HTML files, respecting `limit`, `sample`, `include/exclude` filters.
  - Produces `DocumentRef` with computed `doc_id` (hash or stem) and metadata (e.g., relative path).
- **LoadStage**
  - Injected with `io.readers.HtmlReader`.
  - Reads file lazily, handles encoding errors, populates `Document.html`.
  - Marks documents with `flags['empty_html']` for missing content.
- **TransformStage(s)**
  - Each transform implements `Transform` interface with `transform(doc: Document, ctx)`.
  - Built-in transforms:
    - `RawHtmlToTextTransform` (strip boilerplate off or not).
    - `PreprocessedTextTransform` (existing functionality).
    - `ReadmeExtractorTransform` (produces README artifact and/or entities).
  - Transforms can add artifacts, update metadata, or emit derived signals (e.g., language detection).
  - Ordering is configurable; transforms declare dependencies (e.g., requires `Document.html`).
- **ExtractionStage**
  - Loads enabled `EntityExtractor` plugins from registry.
  - Provides each extractor with a read-only view of required artifacts (html, preprocessed text, README, etc.).
  - Collects `Entity` objects, handles deduplication (configurable) and attaches them to `Document.entities`.
  - Supports optional parallel execution (thread pool) per extractor for CPU-bound regex scanning.
- **OutputStage(s)**
  - Entities sink: `outputs.entities.EntitiesWriter` handles TSV file or alternate sinks (Parquet, streaming) based on config.
  - Artifact writers: `outputs.text.TextArtifactWriter` writes `RAW_TEXT`, `PREPROCESSED_TEXT`, `README_TEXT` while mirroring directory structure.
  - Stage obtains writer instances from `outputs.sink_manager.SinkManager`, ensuring single initialization/cleanup.
  - Stage records success/failure per artifact for stats.

## 9. Extractor Plugin System
### Interface (`extraction/base.py`)
```python
class EntityExtractor(ABC):
    name: ClassVar[str]
    consumes: ClassVar[Set[str]]  # Required artifacts: {"html", "preprocessed_text", ...}
    produces: ClassVar[Set[str]] = set()  # Optional: additional artifacts or tags

    def setup(self, ctx: PipelineContext) -> None:
        ...

    @abstractmethod
    def extract(self, doc: Document, ctx: PipelineContext) -> Iterable[Entity]:
        ...

    def teardown(self, ctx: PipelineContext) -> None:
        ...
```
- Extractors declare dependencies via `consumes`; the pipeline enforces availability before invocation.
- Extractors can optionally add artifacts (e.g., JSON summary) via `doc.artifacts[...]`.
- Results support `attributes` for additional metadata (e.g., numeric counts, units).

### Registry (`extraction/registry.py`)
- Discovers extractors via:
  - Built-in module map (`builtin/*`).
  - Python entry points (`extractor.entity_extractors`).
  - Explicit config mapping (class path strings).
- Handles enable/disable lists, ordering, and per-module config (thresholds, regex overrides).
- Provides `get_extractors(ctx)` returning configured extractor instances.

### Adding a New Extractor
1. Create class deriving from `EntityExtractor` under `extraction/builtin/`.
2. Implement `extract`, using artifacts requested in `consumes`.
3. Register defaults in `registry.py` (or expose via plugin entry point).
4. Document configuration knobs in module docstring; add tests under `tests/test_extractors/`.

## 10. Artifact Writers & Outputs
- **SinkManager** maintains active writers keyed by artifact type, initialized during `OutputStage.setup`.
- Writers share a `FileLayoutStrategy` to mirror input directory structure and support alternative storage (e.g., S3, zipped).
- Entities writer supports both TSV and JSONL via config:
  ```yaml
  extractor:
    outputs:
      entities:
        path: workspace/store/entities/entities.tsv
        format: tsv
        flush_every: 1000
      raw_text:
        path: workspace/store/text
        format: text
  ```
- Writers emit structured events to `PipelineContext.stats` to update counters (written/skipped/failed).

## 11. Configuration Model
- `config/model.py` introduces structured config:
  ```python
  @dataclass
  class ExtractorConfig:
      input: InputConfig
      transforms: List[TransformConfig]
      extractors: List[ExtractorConfigEntry]
      outputs: OutputConfig
      limits: LimitConfig  # limit, sample, dry_run, force
      logging: LoggingConfig
  ```
- **Config priority**: CLI args override config file; both feed into builder.
- **Per-module config**: Each transform/extractor receives a typed config dict (validated via `pydantic` or custom schema).
- Provide `config_loader` integration that merges `workspace` roots as today.

## 12. Error Handling & Observability
- Replace bare `try/except` logging with custom exceptions:
  - `ExtractionError`, `IOError`, `TransformError`.
- Pipeline catches stage-level exceptions, records them on `Document.flags['errors']`, increments failure counters, and continues (unless `--fail-fast` is set).
- Add structured logging context (doc_id, stage, extractor name).
- `core/stats.StatsCollector` tracks per-stage timing, successes, skips, entity counts, enabling a summary similar to current pipeline plus per-extractor metrics.
- Optional hooks to emit metrics to Prometheus-compatible exporters or JSON logs.

## 13. Performance & Concurrency Strategy
- Primary execution remains sequential per document to preserve deterministic ordering.
- ExtractionStage may optionally run extractors concurrently via a thread pool when CPU-bound (config: `extractors.concurrent=true`).
- Add back-pressure controls: limit open writers, flush frequency, memory guard (cap on artifact sizes).
- Provide streaming-friendly artifacts: avoid storing large text in memory when writes can stream (e.g., preprocessed text).

## 14. Testing Strategy
- **Unit Tests**:
  - Stage tests with fixtures for `Document` in various states.
  - Extractor tests using sample HTML/markdown from `tests_regex_samples/`.
  - IO writer tests verifying path mirroring and overwrite semantics.
- **Integration Tests**:
  - Pipeline smoke test that runs discovery → load → built-in extractors on curated sample set.
  - Regression tests for new regex modules (positive/negative pairs).
- **Contract Tests**:
  - Validate plugin compliance (e.g., missing artifact raises descriptive error).
  - Ensure Config schema rejects unknown fields.
- **Golden Outputs**:
  - Optionally store expected TSV/text outputs for sample docs to detect drift.

## 15. Migration Plan
1. **Bootstrap Core**: Implement `core` models, pipeline, and context alongside existing code without changing CLI.
2. **Port IO Utilities**: Move `io_utils` functionality into `io/` package; update imports.
3. **Refactor HTML Cleaning**: Wrap existing functions into `Transform` implementations.
4. **Split Extractors**: Move each function from `entity_extractors.py` into dedicated `EntityExtractor` classes.
5. **Integrate Outputs**: Replace monolithic `outputs.py` with `outputs/` package; ensure compatibility with current TSV/text layouts.
6. **Wire New CLI**: Update `__main__` to delegate to `cli/main.py` and new pipeline builder; support legacy flags during transition.
7. **Deprecate Legacy Code**: Provide compatibility shims as needed, then remove old modules once tests pass.
8. **Documentation & Examples**: Update `USAGE.md` or docs to reflect new pipeline configuration and extension workflow.

## 16. Developer Guidelines
- Favor small, composable stages/extractors; if a module grows beyond ~200 lines consider splitting responsibilities.
- Ensure every extractor validates its inputs and gracefully handles absent artifacts (respect `consumes`).
- When writing new regex patterns, include fixtures and mention them in module docstrings.
- Use `PipelineContext` for shared services (e.g., caching compiled regex overrides) instead of module-level globals.
- Keep artifacts lightweight; avoid storing redundant large strings when existing artifacts suffice.

## 17. Future Extensions
- **Alternate Sources**: Add discovery strategies for compressed archives or remote storage (S3, GCS) without touching extraction logic.
- **Incremental Processing**: Introduce caching metadata (hashes, timestamps) to skip unchanged docs beyond manual `--force`.
- **Structured Outputs**: Support JSON/Parquet outputs for analytics; design `EntitySerializer` interface.
- **Language-Aware Pipelines**: Allow per-language transform/extractor bundles, configured via document metadata.
- **Monitoring Hooks**: Expose metrics endpoints or integrate with `workspace/logs` pipeline for long-running crawls.

---
This specification defines the target state for the extractor module. Implementation should proceed incrementally, tracking work items per section to ensure parity with existing behavior before expanding functionality.


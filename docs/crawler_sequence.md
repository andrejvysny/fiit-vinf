# Crawler sequence — end-to-end flow

This sequence diagram captures the full lifecycle of the Crawler from startup and seeding through the main crawl loop: policy and robots gates, rate limiting, proxy use, fetching with retries/backoff, HTML link extraction, frontier expansion, trajectory edges, discoveries emission, metrics flushing, back-pressure, and stop conditions.

```mermaid
sequenceDiagram
    autonumber

    %% Participants (modules and external actors)
    participant CLI as CLI/Manager
    participant C as CrawlerService
    participant CFG as CrawlerConfig
    participant ROB as RobotsCache
    participant POL as UrlPolicy
    participant SEEN as SeenSet (Bloom)
    participant FR as Frontier (LMDB)
    participant LIM as RateLimiter
    participant PRX as ProxyClient
    participant F as Fetcher
    participant X as Extractor
    participant DISC as DiscoveriesWriter
    participant TR as TrajectoryWriter
    participant MET as Metrics
    participant GH as github.com

    rect rgb(245,245,245)
        note over CLI,C: Startup & wiring
        CLI->>C: start(--config, --seeds, --workspace)
        C->>CFG: from_yaml(config.yaml)
        CFG-->>C: CrawlerConfig
        C->>ROB: init(user_agent, cache_ttl_sec)
        C->>POL: init(cfg.scope, ROB)
        C->>SEEN: init(bloom_path, capacity, error_rate)
        C->>FR: init(db_path, SEEN)
        C->>LIM: init(global_concurrency, per_host_concurrency)
        C->>PRX: set proxy client (optional, manager-provided)
        C->>MET: init(metrics.csv, flush_interval)
        C->>TR: init(edges.csv)
        C->>DISC: init(discoveries_dir, rotate_every_mb)
    end

    rect rgb(240,250,240)
        note over C,FR: Seeding frontier
        loop for each seed URL
            C->>POL: allowed(seed)
            alt rejected by policy
                POL-->>C: (False, reason)
                C->>MET: inc(policy_reject_<reason>)
            else allowed
                POL-->>C: (True, "ok")
                C->>FR: push({url: seed, depth: 0, priority: 0, referrer: null, discovered_at})
                FR-->>C: enqueued
                C->>MET: inc(frontier_push)
            end
        end
    end

    rect rgb(250,245,240)
        note over C: Main crawl loop (until stop)
        loop while not stop_condition
            C->>FR: pop_batch(n=batch_size)
            FR-->>C: items[]
            alt no items returned
                C->>MET: maybe_flush()
                C-->>CLI: idle check (frontier likely empty)
                note over C: exit loop when frontier empty
            else
                par for each item in items
                    C->>C: read it.url, it.depth, it.referrer
                    alt depth > cfg.caps.max_depth
                        C->>MET: inc(skip_depth)
                        note right of C: skip item due to depth cap
                    else within depth
                        C->>POL: allowed(it.url)
                        alt not allowed
                            POL-->>C: (False, reason)
                            C->>MET: inc(policy_reject_<reason>)
                            note right of C: skip item due to policy
                        else allowed
                            POL-->>C: (True, "ok")
                            C->>LIM: acquire(host(url))
                            activate LIM
                            note right of LIM: Enforces global + per-host concurrency

                            C->>PRX: pick() [optional]
                            PRX-->>C: proxy{ id?, url? } or None

                            C->>F: get_html(url) with headers, timeouts, proxy
                            activate F
                            loop retry <= cfg.limits.max_retries
                                F->>GH: HTTP GET url (HTTP/2, UA, Accept-Language, Accept-Encoding)
                                alt 2xx response
                                    GH-->>F: 200 OK + headers + body
                                    F-->>C: {ok, status=200, html, headers, latency_ms, proxy_id}
                                else 3xx
                                    GH-->>F: 3xx + Location
                                    F-->>C: {ok, status=3xx, headers, latency_ms, proxy_id}
                                else 304 (conditional)
                                    GH-->>F: 304 Not Modified
                                    F-->>C: {ok, status=304, not_modified: true, headers, latency_ms, proxy_id}
                                else 429 (rate limited)
                                    GH-->>F: 429 Too Many Requests (+ Retry-After?)
                                    F-->>PRX: report_result(proxy_id, 429, throttled=true)
                                    F-->>C: backoff = retry_after_or_backoff()
                                    note right of F: exponential backoff with jitter
                                    F-->>F: sleep(backoff)
                                    note right of F: will retry request
                                else 5xx / network error / timeout
                                    GH-->>F: 5xx or error
                                    F-->>PRX: report_result(proxy_id, status_or_none, throttled=false)
                                    F-->>C: backoff = _backoff(retries)
                                    F-->>F: sleep(backoff)
                                    note right of F: will retry request
                                end
                            end
                            deactivate F

                            C->>LIM: release(host)
                            deactivate LIM

                            alt fetch failed after retries
                                C->>MET: inc(fetch_fail_err)
                                note right of C: give up on this item after retries
                            else fetch ok
                                alt not text/html
                                    C->>MET: inc(fetch_not_html)
                                    note right of C: skip non-HTML content
                                else HTML available or 304
                                    alt status == 304 Not Modified
                                        C->>MET: inc(fetch_ok)
                                        MET-->>MET: maybe_flush()
                                        note right of C: No body parsing; item considered complete
                                    else status 2xx HTML
                                        C->>X: extract_links(html)
                                        X-->>C: links[]
                                        C->>MET: inc(bytes_in, +len(html)); inc(fetch_ok)

                                        %% Trajectory + Frontier expansion for each accepted link
                                        loop for link in links
                                            C->>POL: allowed(link)
                                            alt allowed
                                                POL-->>C: (True, "ok")
                                                C->>TR: append(src=it.url, dst=link, depth=it.depth+1)
                                                TR-->>C: appended
                                                C->>FR: push({url: link, depth: it.depth+1, priority, referrer: it.url, discovered_at})
                                                FR-->>C: enqueued or duplicate
                                                alt duplicate/seen
                                                    C->>MET: inc(duplicates)
                                                else enqueued
                                                    C->>MET: inc(frontier_push)
                                                end

                                                opt back-pressure on discoveries
                                                    note right of C: if spool size exceeds budget, pause discoveries
                                                end

                                                alt link.page_type in {repo_root, blob, issues, pull} and not paused
                                                    C->>DISC: append({url: link, page_type, depth, referrer: it.url, discovered_at})
                                                    DISC-->>C: written (JSONL; rotated)
                                                else not a target or paused
                                                    note right of C: skip emit
                                                end
                                            else rejected
                                                POL-->>C: (False, reason)
                                                C->>MET: inc(policy_reject_<reason>)
                                            end
                                        end
                                    end
                                end
                            end
                        end
                    end
                and
                    note over MET: Periodically flush metrics to CSV
                    C->>MET: maybe_flush()
                end
            end
        end
    end

    rect rgb(245,245,255)
        note over C,CLI: Stop & teardown
        alt stop because frontier empty
            C-->>CLI: done (frontier drained)
        else stop flag / signal
            C-->>CLI: stopping on flag
        end
        C->>DISC: close()
        C->>TR: close()
        C->>MET: maybe_flush(); finalize
        C-->>CLI: stopped
    end
```

## Notes

- Policy reasons mapped to metrics: host_not_allowed, subdomain_denied, denied_pattern, not_in_allowlist, robots_disallow.
- Back-pressure: discoveries emission can be paused if `spool/discoveries` exceeds a configured size budget, while still allowing frontier expansion to continue.
- Caps: depth limited by `caps.max_depth`; additional per-repo caps (pages/issues/PRs) can be enforced before emitting discoveries.
- Robots: both pre-queue (UrlPolicy.allowed uses RobotsCache) and pre-fetch checks are applied to stay safe.
- Fetch limits: low concurrency + req/sec enforced via RateLimiter and conservative timeouts; retries with exponential backoff and respect for Retry-After.
- Single-writer sinks: Trajectory (edges.csv), Metrics (crawler_metrics.csv), and Discoveries (rotating JSONL) are append-only and flushed to disk. CSV stands for Comma-Separated Values.

```text
Target page types for discoveries → { repo_root, blob, issues, pull }
```

# Telegram fallback pipeline

Status: 2026-08-27

## Contract

The Telegram fallback is available through both interfaces:

- HTTP: `GET /tg/fetch?n=1000&utc=false`
- MCP: `fetch_latest_non_reply_posts_tg(n=1000, utc=false)`

`n` is the initial number of unique Telegram-linked X posts to collect, before
reply filtering. When `n` is omitted, the raw dataset contains exactly 1,000
posts unless the source is exhausted. The aggregate normally contains fewer
than `n` posts because replies to other users are removed afterward.

The `utc` flag selects which aggregate is returned to the caller. Every refresh
writes both timezone variants regardless of that flag.

## Source and processing sequence

Telegram `@elonalert` is the source. TwitterAPI.io and the XTracker providers
are not used by this fallback.

```text
public @elonalert history
        |
        v
collect exactly n unique linked X post IDs, including replies
        |
        +--> write raw_elonmusk_tg.csv with source metadata
        |
        v
remove replies to other users; retain roots, quotes, reposts, and self-replies
        |
        v
derive each post time from its X Snowflake ID
        |
        v
convert to America/New_York and floor to a 15-minute boundary
        |
        +--> elonmusk_tg.csv
        +--> convert bucket starts to UTC --> elonmusk_tg_utc.csv
```

Credential-free Telegram channel history is exposed as a public HTML page. The
implementation parses that transport in memory but does not retain HTML response
bodies. Only the structured CSV is persisted.

## Reply filtering

All linked posts are written to the raw CSV before filtering.

| Telegram label/target | Aggregate action | `filter_reason` |
|---|---|---|
| `Tweet`, `Quote`, or `ReTweet` | Include | `non_reply` |
| `Reply` with a blank target | Include as self-reply | `self_reply` |
| `Reply` targeting `elonmusk` | Include as self-reply | `self_reply` |
| `Reply` targeting another username | Exclude | `reply_to_other` |
| Missing or unknown label | Exclude | `unlabeled` |

The blank-target rule follows the observed `@elonalert` convention: external
replies name their target, while replies in Elon's own thread omit it. Both
`included_in_15m` and `filter_reason` are persisted so the decision remains
auditable.

## Raw schema

`downloads/15m/tg/raw_elonmusk_tg.csv` contains:

| Column | Meaning |
|---|---|
| `id` | X post Snowflake ID, stored as a string |
| `created_at_utc` | UTC post time derived from `id` |
| `telegram_message_id` | Source `@elonalert` message ID |
| `telegram_datetime` | Time Telegram published the alert |
| `alert_delay_seconds` | Telegram time minus Snowflake post time |
| `label` | Normalized `Tweet`, `Quote`, `ReTweet`, or `Reply` |
| `reply_to_username` | Explicit reply target when Telegram supplies one |
| `alert_text` | Tweet text with the alert prefix removed |
| `rendered_text` | Complete normalized Telegram alert text |
| `telegram_url` | Canonical source-message URL |
| `x_url` | Canonical Elon status URL for the linked ID |
| `source_links_json` | All links found in the Telegram message |
| `included_in_15m` | `true` when used by the aggregates |
| `filter_reason` | Inclusion or exclusion classification |

## Bucketing

Telegram's alert timestamp is metadata only; it does not determine the bucket.
Post time is calculated from the Snowflake ID:

```text
timestamp_ms = (int(id) >> 22) + 1288834974657
```

That UTC instant is converted to `America/New_York`, floored to the wall-clock
quarter hour, and counted. The UTC artifact converts the ET bucket start back to
UTC and formats it with a trailing `Z`.

Outputs:

- `downloads/15m/tg/raw_elonmusk_tg.csv`
- `downloads/15m/tg/elonmusk_tg.csv`
- `downloads/15m/tg/elonmusk_tg_utc.csv`

## Default live-refresh evidence

The 2026-08-27 default refresh produced exactly 1,000 raw posts:

| Classification | Count |
|---|---:|
| Non-replies retained | 659 |
| Self-replies retained | 5 |
| Replies to other users excluded | 336 |
| Aggregate total | 664 |

The UTC aggregate contained 316 non-empty buckets from
`2026-08-06T02:15:00Z` through `2026-08-27T19:45:00Z`.

## Comparison with `by_15min_recent_utc.csv`

The first Telegram bucket is partial because the 1,000-post cutoff begins at
`2026-08-06T02:21:34.501Z`. Excluding that boundary bucket, the complete shared
window begins at `2026-08-06T02:30:00Z` and ends at
`2026-08-26T05:45:00Z`.

| Complete shared buckets | Telegram | Recent source |
|---|---:|---:|
| Posts | 639 | 634 |
| Matching buckets | 297 | 297 |
| Differing buckets | 7 | 7 |

Six differences are Telegram-only repost objects:

| UTC bucket | X repost ID | Current X status on 2026-08-27 |
|---|---|---|
| `2026-08-07T14:15:00Z` | [`2085735145152057685`](https://x.com/elonmusk/status/2085735145152057685) | Unavailable (`404`) |
| `2026-08-18T16:00:00Z` | [`2089745306975961273`](https://x.com/elonmusk/status/2089745306975961273) | Unavailable (`404`) |
| `2026-08-22T00:30:00Z` | [`2090960319195853083`](https://x.com/elonmusk/status/2090960319195853083) | Unavailable (`404`) |
| `2026-08-22T03:30:00Z` | [`2091005330755146122`](https://x.com/elonmusk/status/2091005330755146122) | Unavailable (`404`) |
| `2026-08-23T20:30:00Z` | [`2091625099543925048`](https://x.com/elonmusk/status/2091625099543925048) | Unavailable (`404`) |
| `2026-08-25T16:45:00Z` | [`2092293248312397941`](https://x.com/elonmusk/status/2092293248312397941) | Unavailable (`404`) |

These IDs represent Elon's repost objects. Their current unavailability most
likely means the repost was undone; it does not prove the original author's post
was deleted.

The recent-source-only difference is live:

| UTC bucket | X post ID | Current X status on 2026-08-27 |
|---|---|---|
| `2026-08-19T14:00:00Z` | [`2090078247392555471`](https://x.com/elonmusk/status/2090078247392555471) | Available (`200`) |

The excluded partial boundary bucket is `2026-08-06T02:15:00Z`: Telegram has
three posts and the recent source has five. This is a cutoff effect, not evidence
of deletion.

## Limitations

- `@elonalert` is a third-party alert mirror, not authenticated X API evidence.
- The blank-target self-reply rule depends on the channel's observed formatting
  convention and should remain covered by regression tests.
- A fixed-count download can begin inside a 15-minute bucket. Treat that first
  bucket as partial when comparing against a longer archive.
- An unavailable repost object does not establish whether its original post was
  deleted; it can also mean Elon removed the repost.

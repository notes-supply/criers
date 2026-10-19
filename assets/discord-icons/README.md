# Discord status icons

Optional custom emoji for Criers messages. Without them Criers uses Unicode
emoji, so this set is only needed for a consistent look.

| Tone | Emoji name | Lucide icon | Colour |
|---|---|---|---|
| critical | `criers_critical` | `octagon-alert` | `#E5484D` |
| warning | `criers_warning` | `triangle-alert` | `#F5A524` |
| info | `criers_info` | `info` | `#3E8ED0` |
| recovered | `criers_recovered` | `circle-check` | `#30A46C` |
| coverage | `criers_coverage` | `unplug` | `#8E4EC6` |

`lucide/` holds the unmodified sources from Lucide 1.59.0 (commit
`1ef60af944de207e0d5176905a0759ba12e6ee8d`). `build.sh` tints each with its
colour, widens the stroke from 2 to 2.5 for inline legibility and renders a
transparent 128x128 PNG with `rsvg-convert`. `manifest.json` records the source
URLs, checksums and rasterizer version. Lucide is ISC licensed, and its
Feather-derived icons are MIT licensed; see `LICENSE`.

## Use

Upload the PNGs as custom emoji in the server that receives the webhook, then
map each tone to the emoji markup Discord returns, for example:

```toml
[destinations.ops.icons]
critical = "<:criers_critical:123456789012345678>"
warning = "<:criers_warning:123456789012345678>"
info = "<:criers_info:123456789012345678>"
recovered = "<:criers_recovered:123456789012345678>"
coverage = "<:criers_coverage:123456789012345678>"
```

Every key is optional; a tone without one keeps its Unicode emoji. Emoji IDs
belong in your deployment's configuration, not in this repository.

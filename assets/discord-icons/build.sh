#!/usr/bin/env bash
# Builds the Criers status emoji from the pinned Lucide sources in lucide/:
# each icon is tinted with its tone's colour, its stroke widened from 2 to 2.5
# for inline legibility, and rendered to a transparent 128x128 PNG.
# Requires rsvg-convert (librsvg) and sha256sum. Run from any directory.
set -euo pipefail
cd "$(dirname "$0")"

revision=1ef60af944de207e0d5176905a0759ba12e6ee8d
release=1.59.0
icons=(
  "critical octagon-alert #E5484D"
  "warning triangle-alert #F5A524"
  "info info #3E8ED0"
  "recovered circle-check #30A46C"
  "coverage unplug #8E4EC6"
)

sha() { sha256sum "$1" | cut -d' ' -f1; }

entries=()
for icon in "${icons[@]}"; do
  read -r tone source colour <<<"$icon"
  name="criers_${tone}"
  sed -e "s/stroke=\"currentColor\"/stroke=\"${colour}\"/" \
      -e 's/stroke-width="2"/stroke-width="2.5"/' \
      "lucide/${source}.svg" > "${name}.svg"
  rsvg-convert --width 128 --height 128 --format png "${name}.svg" > "${name}.png"
  entries+=("    {
      \"tone\": \"${tone}\",
      \"emoji_name\": \"${name}\",
      \"lucide_icon\": \"${source}\",
      \"source_url\": \"https://github.com/lucide-icons/lucide/blob/${revision}/icons/${source}.svg\",
      \"source_sha256\": \"$(sha "lucide/${source}.svg")\",
      \"colour\": \"${colour}\",
      \"stroke_width\": 2.5,
      \"svg\": \"${name}.svg\",
      \"svg_sha256\": \"$(sha "${name}.svg")\",
      \"png\": \"${name}.png\",
      \"png_size\": [128, 128],
      \"png_sha256\": \"$(sha "${name}.png")\"
    }")
done

{
  printf '{\n'
  printf '  "source": "https://github.com/lucide-icons/lucide",\n'
  printf '  "release": "%s",\n  "revision": "%s",\n' "$release" "$revision"
  printf '  "license": "ISC (Lucide); MIT (Feather-derived icons); see LICENSE",\n'
  printf '  "rasterizer": "%s",\n' "$(rsvg-convert --version | head -n 1)"
  printf '  "icons": [\n'
  (IFS=$'\n'; printf '%s' "$(printf '%s,\n' "${entries[@]}" | sed '$ s/,$//')")
  printf '\n  ]\n}\n'
} > manifest.json

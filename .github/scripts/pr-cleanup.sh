#!/usr/bin/env bash
# Deletes from ghcr what a closed pull request published: the image tagged prN with the images of its
# platforms, and the charts X.Y.Z-prN. Then it collects what earlier pushes left behind: a push to a
# pull request takes the tag prN from the image before it, which stays untagged, and so does a chart
# pushed again under the same version.
#
# Env: GH_TOKEN, GITHUB_REPOSITORY_OWNER, OWNER_TYPE (User or Organization), PR (number).
set -Eeuo pipefail

owner="$GITHUB_REPOSITORY_OWNER"
if [ "$OWNER_TYPE" = Organization ]; then api="orgs/$owner"; else api="users/$owner"; fi
registry="ghcr.io/${owner,,}"
image=local-path-csi
chart=charts/local-path-csi
tag="pr$PR"
# Untagged versions younger than this stay: a release pushes its image by digest alone and tags it
# only after e2e has passed on it.
grace=$((24 * 3600))
now="$(date +%s)"

# versions <package>: "id digest updated tags" per version, tags comma-separated ("-" for none),
# updated in seconds since the epoch. Nothing for a package that does not exist.
versions() {
  local out
  if ! out="$(gh api --paginate "$api/packages/container/${1//\//%2F}/versions" \
    --jq '.[] | [.id, .name, .updated_at, (.metadata.container.tags | if length == 0 then "-" else join(",") end)] | @tsv' 2>&1)"; then
    if grep -q 'HTTP 404' <<<"$out"; then return 0; fi
    echo "$out" >&2
    return 1
  fi
  while IFS=$'\t' read -r id digest updated tags; do
    [ -n "$id" ] || continue
    echo "$id $digest $(date -d "$updated" +%s) $tags"
  done <<<"$out"
}

# delete <package> <id> <what>: a failure is reported and does not stop the rest.
delete() {
  if gh api --method DELETE "$api/packages/container/${1//\//%2F}/versions/$2" >/dev/null; then
    echo "deleted $1 $3"
  else
    echo "::warning::could not delete $1 $3"
  fi
}

# children <digest>: the manifests an image index of the package of the image lists.
children() {
  docker buildx imagetools inspect --raw "$registry/$image@$1" | jq -r '.manifests[]?.digest'
}

# The image. Kept: every tagged version but prN, and the images its index lists. Deleted: prN with the
# images of its index, and untagged versions past the grace period that no kept index lists.
declare -A keep=() doomed=()
listing="$(versions "$image")"
mapfile -t rows < <(sed "/^$/d" <<<"$listing")
for row in "${rows[@]}"; do
  read -r _ digest _ tags <<<"$row"
  [ "$tags" != - ] || continue
  # An index that cannot be read stops the script: its images would be taken for leftovers.
  listed="$(children "$digest")"
  if [ "$tags" = "$tag" ]; then
    for d in "$digest" $listed; do doomed[$d]=1; done
  else
    for d in "$digest" $listed; do keep[$d]=1; done
  fi
done
for row in "${rows[@]}"; do
  read -r id digest updated tags <<<"$row"
  [ -z "${keep[$digest]:-}" ] || continue
  if [ -n "${doomed[$digest]:-}" ]; then
    delete "$image" "$id" "$digest ($tags)"
  elif [ "$tags" = - ] && [ $((now - updated)) -gt "$grace" ]; then
    delete "$image" "$id" "$digest (untagged)"
  fi
done

# The chart: versions X.Y.Z-prN, and untagged versions past the grace period.
listing="$(versions "$chart")"
mapfile -t rows < <(sed "/^$/d" <<<"$listing")
for row in "${rows[@]}"; do
  read -r id digest updated tags <<<"$row"
  if [[ "$tags" != - && "$tags" =~ ^[^,]+-$tag(,[^,]+-$tag)*$ ]]; then
    delete "$chart" "$id" "$tags"
  elif [ "$tags" = - ] && [ $((now - updated)) -gt "$grace" ]; then
    delete "$chart" "$id" "$digest (untagged)"
  fi
done

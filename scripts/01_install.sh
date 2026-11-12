#!/usr/bin/env bash
# Install what one role needs, checked against config/versions.env. Safe to re-run.
# Usage: bash scripts/01_install.sh <ingest|storage|compute|serve>
set -euo pipefail
. "$(dirname "$0")/lib.sh"
ROLE="${1:?usage: 01_install.sh <ingest|storage|compute|serve>}"
role_node "$ROLE" >/dev/null
# shellcheck disable=SC1091
. "$REPO_ROOT/config/versions.env"
PREFIX="${INSTALL_PREFIX:-/opt}"
BDSE_VENV="${BDSE_VENV:-$HOME/.venvs/bdse}"
BDSE_HOME="${BDSE_HOME:-$HOME/bigdata}"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
SPARK_ROLE=0; [ "$ROLE" != serve ] && SPARK_ROLE=1

sudo_if_needed() { if [ -w "$1" ]; then shift; "$@"; else shift; sudo "$@"; fi; }

# Read the hex digest from any checksum file format (BSD, GNU or split into groups).
checksum_from() { tr -d ' \n\r\t' < "$1" | grep -oE "[0-9a-fA-F]{$2}" | head -1 | tr 'A-F' 'a-f'; }

download_verified() {  # <url> <dest> <sha1|sha256|sha512>
  local url="$1" dest="$2" algo="$3" len expected actual
  case "$algo" in sha1) len=40 ;; sha256) len=64 ;; sha512) len=128 ;; esac
  log "downloading $url"
  curl -fL --retry 3 -o "$dest" "$url"
  curl -fsL --retry 3 -o "$dest.$algo" "$url.$algo"
  expected="$(checksum_from "$dest.$algo" "$len")"
  actual="$("${algo}sum" "$dest" | cut -d' ' -f1)"
  [ -n "$expected" ] && [ "$expected" = "$actual" ] || die "$algo mismatch for $(basename "$dest"): expected $expected got $actual"
  log "$algo verified: $(basename "$dest")"
}

apt_install() {
  local missing=()
  for p in "$@"; do dpkg -s "$p" >/dev/null 2>&1 || missing+=("$p"); done
  [ ${#missing[@]} -eq 0 ] && return
  log "apt install ${missing[*]}"
  sudo apt-get update -qq && sudo apt-get install -y -qq "${missing[@]}"
}

java_major() { "$1" -version 2>&1 | awk -F'"' '/version/ {split($2, v, "."); print v[1]; exit}'; }

install_java() {
  local java=""
  if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then java="$JAVA_HOME/bin/java"
  elif command -v java >/dev/null; then java="$(command -v java)"; fi
  if [ -n "$java" ] && [ "$(java_major "$java")" = "$JAVA_MAJOR" ]; then
    log "java $JAVA_MAJOR found ($java), skipping"
  else
    apt_install "openjdk-${JAVA_MAJOR}-jdk-headless"
    java="/usr/lib/jvm/java-${JAVA_MAJOR}-openjdk-amd64/bin/java"
  fi
  if [ -z "${JAVA_HOME:-}" ] || [ "$(java_major "$JAVA_HOME/bin/java" 2>/dev/null)" != "$JAVA_MAJOR" ]; then
    JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$java")")")"
  fi
  export JAVA_HOME
}

# install_tarball <name> <wanted version> <home var> <url> <version cmd>
install_tarball() {
  local name="$1" want="$2" var="$3" url="$4" vcmd="$5" home have
  home="${!var:-$PREFIX/$name}"
  if [ -d "$home" ]; then
    have="$(eval "$vcmd" 2>/dev/null || true)"
    [ "$have" = "$want" ] || die "$name at $home is version '${have:-unknown}', expected $want. Fix it or set $var to a $want install."
    log "$name $want found at $home, skipping"
  else
    home="$PREFIX/$name"
    local file="$TMP/$(basename "$url")"
    download_verified "$url" "$file" sha512
    sudo_if_needed "$PREFIX" mkdir -p "$PREFIX"
    sudo_if_needed "$PREFIX" tar -xzf "$file" -C "$PREFIX"
    local extracted="$PREFIX/$(tar -tzf "$file" | head -1 | cut -d/ -f1)"
    sudo_if_needed "$PREFIX" ln -sfn "$extracted" "$home"
    sudo_if_needed "$PREFIX" chown -R "$USER:$(id -gn)" "$extracted"
    log "$name $want installed at $home"
  fi
  printf -v "$var" '%s' "$home"
  export "${var?}"
}

install_iceberg_jar() {
  local jars="$SPARK_HOME/jars" found
  found="$(cd "$jars" && ls iceberg-spark-runtime-*.jar 2>/dev/null || true)"
  if [ "$found" = "$ICEBERG_JAR" ]; then
    log "Iceberg $ICEBERG_VERSION runtime found in $jars, skipping"
  elif [ -n "$found" ]; then
    die "different Iceberg runtime in $jars: $found (expected $ICEBERG_JAR). Two runtimes conflict; remove it or change ICEBERG_VERSION for the whole team."
  else
    download_verified "$ICEBERG_URL" "$TMP/$ICEBERG_JAR" sha1
    sudo_if_needed "$jars" cp "$TMP/$ICEBERG_JAR" "$jars/"
    log "installed $jars/$ICEBERG_JAR"
  fi
}

install_uv() {
  if command -v uv >/dev/null && uv --version | grep -q " $UV_VERSION"; then log "uv $UV_VERSION found"; return; fi
  download_verified "$UV_URL" "$TMP/uv.tar.gz" sha256
  mkdir -p "$HOME/.local/bin"
  tar -xzf "$TMP/uv.tar.gz" -C "$TMP"
  install -m 755 "$TMP"/uv-*/uv "$HOME/.local/bin/uv"
  export PATH="$HOME/.local/bin:$PATH"
  log "uv $(uv --version) installed"
}

pyspark_works() {  # <python>: can this interpreter run a tiny PySpark job with the installed Spark?
  PYTHONPATH="$SPARK_HOME/python:$(ls "$SPARK_HOME"/python/lib/py4j-*-src.zip)" PYSPARK_PYTHON="$1" timeout 180 "$1" - <<'EOF' >/dev/null 2>&1
from pyspark.sql import SparkSession
s = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
assert s.createDataFrame([(1,)], "x int").count() == 1
s.stop()
EOF
}

install_python_env() {
  local minor sys_minor sys_ok=1
  install_uv
  if [ -x "$BDSE_VENV/bin/python" ]; then
    minor="$("$BDSE_VENV/bin/python" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
    [ "$minor" = "$PYTHON_VERSION" ] || die "$BDSE_VENV is Python $minor, expected $PYTHON_VERSION. Remove it and re-run."
    log "Python env $BDSE_VENV ($minor) found"
  else
    sys_minor="$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null || echo none)"
    if [ $SPARK_ROLE = 1 ]; then
      if pyspark_works python3; then log "PySpark test with system python3 ($sys_minor): PASS"
      else sys_ok=0; log "PySpark test with system python3 ($sys_minor): FAIL"; fi
    fi
    if [ "$sys_minor" = "$PYTHON_VERSION" ] && [ $sys_ok = 1 ]; then
      uv venv --python "$(command -v python3)" "$BDSE_VENV"
    else
      log "creating a uv-managed Python $PYTHON_VERSION env (same minor version on every node)"
      uv venv --python "$PYTHON_VERSION" "$BDSE_VENV"
    fi
  fi
  uv pip install --python "$BDSE_VENV/bin/python" -q -r "$REPO_ROOT/requirements.txt"
  [ $SPARK_ROLE = 0 ] || pyspark_works "$BDSE_VENV/bin/python" || die "PySpark does not work with $BDSE_VENV/bin/python"
  log "Python env ready: $("$BDSE_VENV/bin/python" --version)"
}

write_env_file() {
  cat > "$HOME/.bigdata_env" <<EOF
# Written by bd-search-engine/scripts/01_install.sh ($ROLE)
export BDSE_HOME="$BDSE_HOME"
export BDSE_VENV="$BDSE_VENV"
export JAVA_HOME="${JAVA_HOME:-}"
export HADOOP_HOME="${HADOOP_HOME:-}"
export SPARK_HOME="${SPARK_HOME:-}"
export HADOOP_CONF_DIR="$BDSE_HOME/conf/hadoop"
export SPARK_CONF_DIR="$BDSE_HOME/conf/spark"
export PYSPARK_PYTHON="$BDSE_VENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$BDSE_VENV/bin/python"
export PATH="\$HOME/.local/bin:${SPARK_HOME:+$SPARK_HOME/bin:$SPARK_HOME/sbin:}${HADOOP_HOME:+$HADOOP_HOME/bin:$HADOOP_HOME/sbin:}\$PATH"
EOF
  grep -q '\.bigdata_env' "$HOME/.bashrc" 2>/dev/null \
    || echo '[ -f "$HOME/.bigdata_env" ] && . "$HOME/.bigdata_env"' >> "$HOME/.bashrc"
  log "wrote ~/.bigdata_env (sourced from ~/.bashrc)"
}

log "installing role '$ROLE'"
apt_install curl ca-certificates rsync openssh-client gettext-base
[ "$ROLE" = compute ] && apt_install openssh-server   # node4 pulls exports from here over ssh

if [ $SPARK_ROLE = 1 ]; then
  install_java
  install_tarball hadoop "$HADOOP_VERSION" HADOOP_HOME "$HADOOP_URL" \
    '"$home/bin/hadoop" version | awk "NR==1{print \$2}"'
  install_tarball spark "$SPARK_VERSION" SPARK_HOME "$SPARK_URL" \
    'ls "$home"/jars/spark-core_${SCALA_BINARY_VERSION}-*.jar | sed -E "s/.*spark-core_[0-9.]+-([0-9.]+)\.jar/\1/"'
  install_iceberg_jar
fi
install_python_env
write_env_file
log "done. Next: bash scripts/02_zerotier.sh"

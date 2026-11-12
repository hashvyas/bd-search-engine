#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/lib.sh"
ROLE="${1:?usage: 04_configure.sh <ingest|storage|compute|serve>}"
N="$(role_node "$ROLE")"
load_env
ip_var="NODE${N}_IP"
export NODE_IP="${!ip_var:-}" NODE_HOSTNAME="node$N"
[ -n "$NODE_IP" ] || die "$ip_var is not set in config/cluster.env"
T="$REPO_ROOT/config/templates"
mkdir -p "$BDSE_HOME"/{hdfs/name,hdfs/data,hdfs/tmp,pids,logs/hadoop,logs/spark,spark-work,exports,serving}

if [ "$ROLE" != serve ]; then
  [ -n "${JAVA_HOME:-}" ] || die "JAVA_HOME not set (run 01_install.sh, then: source ~/.bigdata_env)"
  render "$T/core-site.xml.tpl" "$HADOOP_CONF_DIR/core-site.xml"
  render "$T/hdfs-site.xml.tpl" "$HADOOP_CONF_DIR/hdfs-site.xml"
  render "$T/hadoop-env.sh.tpl" "$HADOOP_CONF_DIR/hadoop-env.sh"
  cp -n "$HADOOP_HOME/etc/hadoop/log4j.properties" "$HADOOP_CONF_DIR/" 2>/dev/null || true
  render "$T/spark-defaults.conf.tpl" "$SPARK_CONF_DIR/spark-defaults.conf"
  render "$T/spark-env.sh.tpl" "$SPARK_CONF_DIR/spark-env.sh"
  sed 's/^rootLogger.level = info/rootLogger.level = warn/' \
    "$SPARK_HOME/conf/log4j2.properties.template" > "$SPARK_CONF_DIR/log4j2.properties"
  render "$T/iceberg.conf.tpl" "$CONF_DIR/iceberg.conf"
fi
log "role $ROLE configured (node$N, $NODE_IP). Next: bash scripts/05_start.sh $ROLE"

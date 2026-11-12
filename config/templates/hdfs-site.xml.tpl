<?xml version="1.0"?>
<configuration>
  <property><name>dfs.replication</name><value>${HDFS_REPLICATION}</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file://${BDSE_HOME}/hdfs/name</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file://${BDSE_HOME}/hdfs/data</value></property>
  <property><name>dfs.permissions.enabled</name><value>false</value></property>

  <!-- Bind on all interfaces. Advertise the ZeroTier hostnames. -->
  <property><name>dfs.namenode.rpc-bind-host</name><value>0.0.0.0</value></property>
  <property><name>dfs.namenode.servicerpc-bind-host</name><value>0.0.0.0</value></property>
  <property><name>dfs.namenode.http-bind-host</name><value>0.0.0.0</value></property>
  <property><name>dfs.namenode.http-address</name><value>node2:${HDFS_NAMENODE_HTTP_PORT}</value></property>
  <property><name>dfs.datanode.address</name><value>0.0.0.0:${HDFS_DATANODE_PORT}</value></property>
  <property><name>dfs.datanode.http.address</name><value>0.0.0.0:${HDFS_DATANODE_HTTP_PORT}</value></property>
  <property><name>dfs.datanode.ipc.address</name><value>0.0.0.0:${HDFS_DATANODE_IPC_PORT}</value></property>
  <property><name>dfs.datanode.hostname</name><value>${NODE_HOSTNAME}</value></property>
  <property><name>dfs.client.use.datanode.hostname</name><value>true</value></property>
  <property><name>dfs.datanode.use.datanode.hostname</name><value>true</value></property>
  <property><name>dfs.namenode.datanode.registration.ip-hostname-check</name><value>false</value></property>

  <!-- Fault demo: mark a stopped DataNode dead after about 70 s instead of 10.5 min. -->
  <property><name>dfs.namenode.heartbeat.recheck-interval</name><value>20000</value></property>
  <!-- With only 2 DataNodes, do not try to replace a failed node in the write pipeline. -->
  <property><name>dfs.client.block.write.replace-datanode-on-failure.policy</name><value>NEVER</value></property>
</configuration>

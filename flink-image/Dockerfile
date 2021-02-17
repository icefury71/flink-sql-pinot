###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
###############################################################################

FROM flink:1.12.0-scala_2.11

RUN mkdir /opt/flink/plugins/flink-s3-fs-hadoop; \
    mv /opt/flink/opt/flink-s3-fs-hadoop-1.12.0.jar /opt/flink/plugins/flink-s3-fs-hadoop/.; \
    echo "s3.access-key: dummy-access-key" >> /opt/flink/conf/flink-conf.yaml; \
    echo "s3.secret-key: dummy-secret-key" >> /opt/flink/conf/flink-conf.yaml; \
    echo "fs.s3a.access.key: dummy-access-key" >> /opt/flink/conf/flink-conf.yaml; \
    echo "fs.s3a.secret.key: dummy-secret-key" >> /opt/flink/conf/flink-conf.yaml; \
    echo "fs.s3a.endpoint: http://minio:80" >> /opt/flink/conf/flink-conf.yaml; \
    echo "fs.s3a.path.style.access: true" >> /opt/flink/conf/flink-conf.yaml; \
    sed -i -e 's/taskmanager.memory.process.size: 1568m/taskmanager.memory.process.size: 1728m/g' /opt/flink/conf/flink-conf.yaml; \
    echo "taskmanager.memory.jvm-metaspace.size: 256m" >> /opt/flink/conf/flink-conf.yaml;
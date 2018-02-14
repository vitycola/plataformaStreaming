#!/bin/bash -x
(cd /root/borrar/plataformaStreaming/; sbt package)
spark-submit --class getHBase --master yarn-cluster --driver-class-path `hbase classpath` --conf spark.yarn.appMasterEnv.ENTORNO=PRODUCCION --executor-memory 2G --driver-memory 4G --num-executors 4 --executor-cores 1 --driver-cores 4 /root/borrar/plataformaStreaming/target/scala-2.10/spark_2.10-1.0.jar

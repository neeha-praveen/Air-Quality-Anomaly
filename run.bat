@echo off
set JARS=C:\bda_project\jars\spark-sql-kafka-0-10_2.12-3.5.0.jar,C:\bda_project\jars\kafka-clients-3.4.1.jar,C:\bda_project\jars\spark-token-provider-kafka-0-10_2.12-3.5.0.jar,C:\bda_project\jars\commons-pool2-2.11.1.jar,C:\bda_project\jars\elasticsearch-spark-30_2.12-8.12.0.jar

spark-submit --jars "%JARS%" C:\bda_project\%1
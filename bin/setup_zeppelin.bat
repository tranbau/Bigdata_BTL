@echo off
docker exec -it zeppelin /bin/bash -c "cd /opt/zeppelin/conf && echo export PYSPARK_PYTHON=python3 >> zeppelin-env.sh && echo export PYSPARK_DRIVER_PYTHON=python3 >> zeppelin-env.sh"
docker restart zeppelin

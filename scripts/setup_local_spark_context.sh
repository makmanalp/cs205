export SPARK_HOME=`find_spark_home.py`
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="'lab' --notebook-dir=~/"
export SPARK_CONF_DIR=/Users/makmana/cs205/final_project/scripts/
alias gf="pyspark  --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 --driver-memory=3G --executor-memory=2G"

echo "Setting up the environment"
export PROJECT_PATH=$PWD
echo "Current working directory:" $PROJECT_PATH

# Java configurations
export JAVA_HOME=$PROJECT_PATH/dependencies/java/jre
export PATH=${PATH}:${JAVA_HOME}/bin
echo "Setting up JAVA_HOME to": $JAVA_HOME

# Spark Jars Path
export SPARK_JARS_PATH=$PROJECT_PATH/dependencies/spark_jars
echo "Setting Spark jars path to:" $SPARK_JARS_PATH
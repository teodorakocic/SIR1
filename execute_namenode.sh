docker exec -it namenode hdfs dfs -test -e /sedona
if [ $? -eq 1 ]
then
  echo "[INFO]: Creating /data folder on HDFS"
  docker exec -it namenode hdfs dfs -mkdir /data
fi

docker exec -it namenode hdfs dfs -test -e /data/belgrade_emission.csv
if [ $? -eq 1 ]
then
  echo "[INFO]: Adding csv file in the /sedona folder on the HDFS"
  docker exec -it namenode hdfs dfs -put /data/belgrade_emission.csv /data/belgrade_emission.csv
fi

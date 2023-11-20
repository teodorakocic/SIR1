from pyspark.sql import SparkSession
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.register import SedonaRegistrator 
from pyspark.sql.types import StructType, FloatType, StringType
from pyspark.sql.functions import *
from sedona.sql import *
from shapely.geometry import Point, LineString, Polygon
# from mosaic import enable_mosaic

def init_spark():
  spark = SparkSession. \
    builder. \
    appName("sedona-app"). \
    config("spark.network.timeout", "1000s"). \
    config("spark.driver.maxResultSize", "5g"). \
    config('spark.jars.packages',
         'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.1.0-incubating,org.datasyslab:geotools-wrapper:1.1.0-25.2,org.apache.spark:spark-avro_2.12:3.0.2'). \
    getOrCreate()

  sContext = spark.sparkContext
  sContext.setSystemProperty("spark.serializer", KryoSerializer.getName)
  sContext.setSystemProperty("spark.kryo.registrator", SedonaKryoRegistrator.getName)
  sContext.setSystemProperty("spark.sql.extensions", "org.apache.sedona.sql.SedonaSqlExtensions")

  # enable_mosaic(spark)

  SedonaRegistrator.registerAll(spark)

  return spark, sContext

def main():
  spark, sc = init_spark()

  #create schemas 
  belgradeLanesSchema = StructType().add("interval_begin", "float").add("interval_end", "float").add("interval_id", "string").add("edge_id", "string"). \
    add("lane_arrived", "integer").add("lane_density", "float").add("lane_departed", "integer").add("lane_entered", "integer").add("lane_id", "string"). \
      add("lane_laneChangedFrom", "integer").add("lane_laneChangedTo", "integer").add("lane_laneDensity", "float").add("lane_left", "integer"). \
        add("lane_occupancy", "float").add("lane_overlapTraveltime", "float").add("lane_sampledSeconds", "float").add("lane_speed", "float"). \
          add("lane_speedRelative", "float").add("lane_timeloss", "float").add("lane_traveltime", "float").add("lane_waitingTime", "float") 

  belgradeEmissionSchema = StructType().add("timestep_time", "float").add("vehicle_CO", "float").add("vehicle_CO2", "float").add("vehicle_HC", "float"). \
    add("vehicle_NOx", "float").add("vehicle_PMx", "float").add("vehicle_angle", "float").add("vehicle_eclass", "string").add("vehicle_electricity", "float"). \
      add("vehicle_fuel", "float").add("vehicle_id", "integer").add("lane_id", "string").add("vehicle_noise", "float"). \
        add("vehicle_pos", "float").add("vehicle_route", "string").add("vehicle_speed", "float").add("vehicle_type", "string"). \
          add("vehicle_waiting", "float").add("vehicle_x", "float").add("vehicle_y", "float") 

  #read files from HDFS
  belgradeLanesDf = spark.read.option("delimiter", ";").option("header", "true").csv("hdfs://namenode:9000/data/belgrade_lanes.csv", belgradeLanesSchema)  
  belgradeEmissionDf = spark.read.option("delimiter", ";").option("header", "true").csv("hdfs://namenode:9000/data/belgrade_emission.csv", belgradeEmissionSchema)
  belgradeEmissionForGeometryDf = spark.read.option("delimiter", ";").option("header", "true").csv("hdfs://namenode:9000/data/belgrade_emission.csv", belgradeEmissionSchema)
  belgradeEmissionForGeometryDf.createOrReplaceTempView("belgrade_data")
  
  #create Point type for vehile current location
  belgradeEmissionForGeometryDf.createOrReplaceTempView("points")
  point_udf = udf(lambda x,y: Point(x,y).wkt, returnType=StringType())
  belgradeEmissionForGeometryDf = belgradeEmissionForGeometryDf.withColumn('wkt', point_udf(belgradeEmissionForGeometryDf.vehicle_x, belgradeEmissionForGeometryDf.vehicle_y))
  belgradeEmissionForGeometryDf.createOrReplaceTempView("points_geometry")

  #calculate lane lenghts and traffic volume
  belgradeLanesPlusDf = belgradeLanesDf. \
    withColumn("lane_length", (col("lane_sampledSeconds") / 24.0 / col("lane_laneDensity")).cast("float")). \
      withColumn("total_distance_travelled", (col("lane_sampledSeconds") * col("lane_speedRelative")).cast("float")). \
        withColumn("traffic_volume_begin", (3600 * col("lane_entered") / 24000).cast("float")). \
          withColumn("traffic_volume_end", (3600 * col("lane_left") /  24000).cast("float")) 

  #create one dataframe for both datasets
  for column in [column for column in belgradeEmissionDf.columns if column not in belgradeLanesDf.columns]:
    belgradeLanesDf = belgradeLanesDf.withColumn(column, lit(None))

  for column in [column for column in belgradeLanesDf.columns if column not in belgradeEmissionDf.columns]:
      belgradeEmissionDf = belgradeEmissionDf.withColumn(column, lit(None))

  belgradeDf = belgradeLanesDf.unionByName(belgradeEmissionDf)

  #total emission for every lane
  belgradeDfWithEmissions = belgradeDf.groupBy("lane_id"). \
    agg(
      sum("vehicle_CO").alias("total_CO"),
      sum("vehicle_CO2").alias("total_CO2"),
      sum("vehicle_HC").alias("total_HC"),
      sum("vehicle_NOx").alias("total_NOx"),
      sum("vehicle_PMx").alias("total_PMx")). \
        filter((col("total_CO") > 0.0) & (col("total_CO2") > 0.0) & (col("total_HC") > 0.0) & (col("total_NOx") > 0.0) & (col("total_PMx") > 0.0)). \
          groupBy("lane_id"). \
          agg(
            sum(col("total_CO") + col("total_CO2") + col("total_HC") + col("total_NOx") + col("total_PMx")).alias("total_emission")
          ). \
            sort(col("total_emission"), ascending=False). \
              collect()

  laneWithMaxEmission = belgradeDfWithEmissions[0].asDict()["lane_id"]
  print('Lane with max emission =', laneWithMaxEmission)

  #vehicle count on every lane
  belgradeVehicleCountDf = belgradeDf. \
    select("lane_id", "vehicle_id"). \
      groupBy("lane_id"). \
        agg(countDistinct(col("vehicle_id")).alias("vehicle_count")). \
          sort(col("vehicle_count"), ascending=False). \
            collect()

  # create Points and LineSting for street with max polution
  belgradeLanePositionsSedonaDf = belgradeDf. \
  select("lane_id", "vehicle_pos", "vehicle_x", "vehicle_y"). \
    groupBy("lane_id"). \
    agg(
      min("vehicle_pos").alias("start_pos_lane"),
      max("vehicle_pos").alias("end_pos_lane")
    ). \
      filter(col("lane_id") == laneWithMaxEmission)

  belgradeLanePositionsSedonaDf.createOrReplaceTempView("positions")

  startPositionLane = belgradeLanePositionsSedonaDf.collect()[0].asDict()["start_pos_lane"]
  endPositionLane = belgradeLanePositionsSedonaDf.collect()[0].asDict()["end_pos_lane"]

  belgradeLaneStartPointGeomSedonaDf = belgradeDf. \
    select("lane_id", "vehicle_pos", "vehicle_x", "vehicle_y"). \
      filter((col("lane_id") == laneWithMaxEmission) & (col("vehicle_pos") == startPositionLane))

  belgradeLaneEndPointGeomSedonaDf = belgradeDf. \
    select("lane_id", "vehicle_pos", "vehicle_x", "vehicle_y"). \
      filter((col("lane_id") == laneWithMaxEmission) & (col("vehicle_pos") == endPositionLane))

  belgradeLaneStartPointGeomSedonaDf.createOrReplaceTempView("starting_position")
  belgradeLaneEndPointGeomSedonaDf.createOrReplaceTempView("ending_position")

  startX = belgradeLaneStartPointGeomSedonaDf.collect()[0].asDict()["vehicle_x"]
  startY = belgradeLaneStartPointGeomSedonaDf.collect()[0].asDict()["vehicle_y"]
  startLocation = "\'" + str(startX) + "," + str(startY) + "\' , \',\'"

  endX = belgradeLaneEndPointGeomSedonaDf.collect()[0].asDict()["vehicle_x"]
  endY = belgradeLaneEndPointGeomSedonaDf.collect()[0].asDict()["vehicle_y"]
  endLocation = "\'" + str(endX) + "," + str(endY) + "\' , \',\'"

  blegradeStartPoint = spark.sql(f"""SELECT lane_id, ST_PointFromText({startLocation}) AS location FROM starting_position""").collect()[0][1]
  blegradeEndPoint = spark.sql(f"""SELECT lane_id, ST_PointFromText({endLocation}) AS location FROM ending_position""").collect()[0][1]

  lineLocation = "\'" + str(startX) + "," + str(startY) + "," + str(endX) + "," + str(endY) + "\' , \',\'"

  belgradeLineString = spark.sql(f"""SELECT lane_id, ST_LineStringFromText({lineLocation}) AS location FROM positions""").collect()[0][1]
  belgradeLineStringArgument = "\'" + str(belgradeLineString) + "\'"

  # caluclation of buffered area around street with max polution 
  belgradeBuffer = spark.sql(f"""SELECT lane_id, ST_Buffer(ST_GeomFromText({belgradeLineStringArgument}), 175) AS areaPolution FROM positions""")
  belgradeBuffer.createOrReplaceTempView("belgrade_buffer")

  spark.sql("SELECT *, ST_Area(areaPolution) AS area FROM belgrade_buffer").show()
  belgradeBuffer = spark.sql(f"""SELECT lane_id, ST_Buffer(ST_GeomFromText({belgradeLineStringArgument}), 175) AS areaPolution FROM positions""").collect()[0][1]
  belgradeBufferArgument = "\'" + str(belgradeBuffer) + "\'"

  #create colums for distance (vehicle's current location and line with max pollution), intersection, contain logic (vehicle's current location and buffered area)
  belgradePointsDf = spark.sql(f"""SELECT *, ST_Intersects(ST_GeomFromText({belgradeBufferArgument}), ST_GeomFromWkt(wkt)) AS intersects, ST_Contains(ST_GeomFromText({belgradeBufferArgument}), ST_GeomFromWkt(wkt)) AS contains, ST_Distance(ST_GeomFromText({belgradeLineStringArgument}), ST_GeomFromWkt(wkt)) AS distance FROM points_geometry""")
  belgradePointsDf.createOrReplaceTempView("intersection")

  #number of vehicles that passed through the area
  belgradePointsFilteredDf = spark.sql("SELECT vehicle_id FROM intersection WHERE vehicle_id>0 AND contains==True").distinct().count()
  print(str(belgradePointsFilteredDf))

  #statistics for lanes near the 
  belgradePointsForLanesDf = spark.sql("SELECT * FROM intersection WHERE vehicle_id>0 AND contains==True")

  belgradeLanesPlusDf.join(belgradePointsForLanesDf, belgradeLanesPlusDf["lane_id"] == belgradePointsForLanesDf["lane_id"])
  belgradeLanesPlusDf.join(belgradePointsForLanesDf, ["lane_id"]).show()

  belgradeLanesPlusDf.groupBy("lane_id") \
    .agg(
      avg("lane_length").alias("avg_lane_length"), \
      sum("total_distance_travelled").alias("sum_total_distance_travelled"), \
      mean("traffic_volume_begin").alias("mean_traffic_volume_begin"), \
      mean("traffic_volume_end").alias("mean_traffic_volume_end") 
      ). \
        filter(col("avg_lane_length") > 0.0). \
          show() 

  #distance between vehicle and street with max emission
  belgradeVehicleDistanceDf = spark.sql("SELECT distance FROM intersection WHERE vehicle_id>0").sort(col("distance"), ascending=False).show()

  #loading and processing data from geofabrik
  geofabrikSchema = "type string, geometry string, properties string"
  geofabrikDf = spark.read.json("/opt/spark-apps/output.geojson", schema=geofabrikSchema). \
    withColumn("geometry", expr("ST_GeomFromGeoJSON(geometry)"))
  geofabrikDf.createOrReplaceTempView("geofabrik")

  #finding longest bridge
  # geofabrikBridgeLengthDf = spark.sql("SELECT *, ST_Length(ST_FlipCoordinates(ST_Transform(geometry, 'epsg:4326','epsg:3857'))) FROM geofabrik")
  geofabrikBridgeLengthDf = spark.sql("SELECT geometry AS bridge_location, properties, ST_Length(geometry) AS bridge_length FROM geofabrik")
  longestBridgeDf = geofabrikBridgeLengthDf. \
    select("bridge_location", "bridge_length"). \
      sort(col("bridge_length"), ascending=False). \
        collect()

  longestBridge = longestBridgeDf[0].asDict()["bridge_location"]
  longestBridgeArgument = "\'" + str(longestBridge) + "\'"

  #longest bridge statistics
  belgradeBridgeDf = spark.sql(f"""SELECT *, ST_Intersects(ST_GeomFromText({longestBridgeArgument}), ST_GeomFromWkt(wkt)) AS intersects, ST_Contains(ST_GeomFromText({longestBridgeArgument}), ST_GeomFromWkt(wkt)) AS contains, ST_Distance(ST_GeomFromText({longestBridgeArgument}), ST_GeomFromWkt(wkt)) AS distance FROM points_geometry""")
  belgradeBridgeDf.createOrReplaceTempView("bridge_geometry")

  bridgeStatisticsDf = spark.sql("SELECT * FROM bridge_geometry WHERE vehicle_id > 0 AND distance < 0.05")

  bridgeVehicleCount = bridgeStatisticsDf.select(countDistinct(col("vehicle_id")).alias("vehicle_on_bridge")).collect()
  print("Vehicle count passed on the longest bridge", str(bridgeVehicleCount[0].asDict()["vehicle_on_bridge"]))
  
  bridgeStatisticsNewDf = bridgeStatisticsDf. \
    groupBy("vehicle_id"). \
      agg(
        sum("vehicle_fuel").alias("sum_vehicle_fuel"), \
        avg("vehicle_speed").alias("avg_vehicle_speed"), \
        avg("vehicle_electricity").alias("avg_vehicle_electricity"), \
        max("vehicle_waiting").alias("max_vehicle_fuel")
      ). \
        sort(col("max_vehicle_fuel"), ascending=False). \
          show()


if __name__ == '__main__':
  main()

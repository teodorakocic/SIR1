from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, FloatType, StringType, DoubleType
from pyspark.sql.functions import *
from shapely import Point, LineString, Polygon
import mosaic as mos

mos.enable_mosaic(spark, dbutils)

#load data and create dataframes
emission_file_name = "/FileStore/tables/belgrade_emission.csv"
lanes_file_name = "/FileStore/tables/belgrade_lanes.csv"

belgradeEmissionSchema = StructType().add("timestep_time", "float").add("vehicle_CO", "float").add("vehicle_CO2", "float").add("vehicle_HC", "float"). \
    add("vehicle_NOx", "float").add("vehicle_PMx", "float").add("vehicle_angle", "float").add("vehicle_eclass", "string").add("vehicle_electricity", "float"). \
      add("vehicle_fuel", "float").add("vehicle_id", "integer").add("lane_id", "string").add("vehicle_noise", "float"). \
        add("vehicle_pos", "float").add("vehicle_route", "string").add("vehicle_speed", "float").add("vehicle_type", "string"). \
          add("vehicle_waiting", "float").add("vehicle_x", "double").add("vehicle_y", "double") 

belgradeLanesSchema = StructType().add("interval_begin", "float").add("interval_end", "float").add("interval_id", "string").add("edge_id", "string"). \
    add("lane_arrived", "integer").add("lane_density", "float").add("lane_departed", "integer").add("lane_entered", "integer").add("lane_id", "string"). \
      add("lane_laneChangedFrom", "integer").add("lane_laneChangedTo", "integer").add("lane_laneDensity", "float").add("lane_left", "integer"). \
        add("lane_occupancy", "float").add("lane_overlapTraveltime", "float").add("lane_sampledSeconds", "float").add("lane_speed", "float"). \
          add("lane_speedRelative", "float").add("lane_timeloss", "float").add("lane_traveltime", "float").add("lane_waitingTime", "float") 
                            
emissionDf = spark.read.csv(emission_file_name, sep=";", header=True, schema=belgradeEmissionSchema)
emissionForGeometryDf = spark.read.csv(emission_file_name, sep=";", header=True, schema=belgradeEmissionSchema)
lanesInitialDf = spark.read.csv(lanes_file_name, sep=";", header=True, schema=belgradeLanesSchema)

#create point for each vehicle's position in simulation
emissionPositionDf = emissionForGeometryDf.withColumn("wkt", mos.st_astext(mos.st_point(col("vehicle_x"), col("vehicle_y"))))

#find lane with max value of emission
totalEmissionDf = emissionForGeometryDf.groupBy("lane_id"). \
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

laneWithMaxEmission = totalEmissionDf[0].asDict()["lane_id"]
print('Lane with max emission =', laneWithMaxEmission)

#create geometry column for lane with max emission
belgradeLanePositionsSedonaDf = emissionDf. \
  select("lane_id", "vehicle_pos", "vehicle_x", "vehicle_y"). \
    groupBy("lane_id"). \
    agg(
      min("vehicle_pos").alias("start_pos_lane"),
      max("vehicle_pos").alias("end_pos_lane")
    ). \
      filter(col("lane_id") == laneWithMaxEmission)

startPositionLane = belgradeLanePositionsSedonaDf.collect()[0].asDict()["start_pos_lane"]
endPositionLane = belgradeLanePositionsSedonaDf.collect()[0].asDict()["end_pos_lane"]

belgradeLaneStartPointGeomSedonaDf = emissionDf. \
  select("lane_id", "vehicle_pos", "vehicle_x", "vehicle_y"). \
    filter((col("lane_id") == laneWithMaxEmission) & (col("vehicle_pos") == startPositionLane))

belgradeLaneEndPointGeomSedonaDf = emissionDf. \
  select("lane_id", "vehicle_pos", "vehicle_x", "vehicle_y"). \
    filter((col("lane_id") == laneWithMaxEmission) & (col("vehicle_pos") == endPositionLane))

#startLanePoint = belgradeLaneStartPointGeomSedonaDf.select(mos.st_astext(mos.st_point(col("vehicle_x"), col("vehicle_y"))).alias("location")).distinct().show()
#endLanePoint = belgradeLaneEndPointGeomSedonaDf.select(mos.st_astext(mos.st_point(col("vehicle_x"), col("vehicle_y"))).alias("location")).show()
startLaneDf = belgradeLaneStartPointGeomSedonaDf.withColumn("location", mos.st_point(col("vehicle_x"), col("vehicle_y"))).distinct()
endLaneDf = belgradeLaneEndPointGeomSedonaDf.withColumn("location", mos.st_point(col("vehicle_x"), col("vehicle_y")))

#merge dataframes for start and end position of lane 
for column in [column for column in endLaneDf.columns if column not in startLaneDf.columns]:
    startLaneDf = startLaneDf.withColumn(column, lit(None))

for column in [column for column in startLaneDf.columns if column not in endLaneDf.columns]:
    endLaneDf = endLaneDf.withColumn(column, lit(None))
  
laneGeometryDf = startLaneDf.unionByName(endLaneDf)
laneGeometryNewDf = laneGeometryDf.agg(collect_list("location").alias("point_array")). \
  select(mos.st_makeline("point_array").alias("line_geometry"))

#creating buffer around lane and calculation of area it covers
bufferGeometryDf = laneGeometryNewDf.select(mos.st_buffer(mos.st_astext(col("line_geometry")), lit(150.)).alias("buffer_geometry"), \
  mos.st_area(mos.st_astext(col("buffer_geometry"))).alias("buffer_area"))
bufferAroundLane = bufferGeometryDf.collect()[0].asDict()["buffer_geometry"]

#count of vehicles that passed through area with max polution
for column in [column for column in bufferGeometryDf.columns if column not in emissionPositionDf.columns]:
    emissionPositionDf = emissionPositionDf.withColumn(column, lit(bufferAroundLane))
for column in [column for column in emissionPositionDf.columns if column not in bufferGeometryDf.columns]:
    bufferGeometryDf = bufferGeometryDf.withColumn(column, lit(None))
positionWithAreaDf = emissionPositionDf.unionByName(bufferGeometryDf)

positionAreaCountDf = positionWithAreaDf.withColumn("vehicle_passed", mos.st_contains(mos.st_astext(col("buffer_geometry")), col("wkt"))). \
  withColumn("vehicle_intersection", mos.st_intersection(mos.st_astext(col("buffer_geometry")), col("wkt")))

vehicleInAreaDf = positionAreaCountDf.select("vehicle_id", "vehicle_passed").filter((col("vehicle_passed")==True) & (col("vehicle_id") >= 0))

vehiclesCountDf = vehicleInAreaDf.select(countDistinct("vehicle_id")).collect()
print("Number of vehicles passed through area is", str(vehiclesCountDf[0].asDict()["count(DISTINCT vehicle_id)"]))

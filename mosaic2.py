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
                            
emissionForGeometryDf = spark.read.csv(emission_file_name, sep=";", header=True, schema=belgradeEmissionSchema)

#create point for each vehicle's position in simulation
emissionPositionDf = emissionForGeometryDf.withColumn("wkt", mos.st_astext(mos.st_point(col("vehicle_x"), col("vehicle_y"))))
#index on every created point
indexedVehicleTripsDf = emissionPositionDf.withColumn("ix", mos.grid_longlatascellid(lon="vehicle_x", lat="vehicle_y", resolution=lit(10)))
indexedVehiclesWithRing = indexedVehicleTripsDf.withColumn("grid_area", mos.grid_cellarea("ix"))

#read data from OSM
geofabrikSchema = "type string, geometry string, properties string"
bridges = spark.read.json("/FileStore/tables/output.geojson", schema=geofabrikSchema). \
  withColumn("geometry", mos.st_geomfromgeojson("geometry"))

#select area around every bridge from dataset
bridgesAreaDf = bridges.withColumn("bridge_area", mos.st_buffer(mos.st_astext("geometry"), lit(150.)))

#create indexes on every bridge area
indexedBridgesDf = bridgesAreaDf.select(mos.grid_polyfill("bridge_area", lit(10)).alias("ix_set")).drop("geometry")
explodedIndexBridgesDf = indexedBridgesDf.withColumn("ix", explode("ix_set")).drop("ix_set")
explodedIndexesBridgeWithRing = explodedIndexBridgesDf.withColumn("grid_area", mos.grid_cellarea("ix"))

#inner join on indexes to find vehicle's count on every bridge
joinedIndexesDf = indexedVehicleTripsDf.alias("t").join(explodedIndexBridgesDf.alias("b"), on="ix", how="inner").count()

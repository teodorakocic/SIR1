import osmium
import sys
import json
import geojson
from shapely.geometry import Polygon, MultiPolygon, Point, LineString
import shapely.wkb as wkblib

wkbfab = osmium.geom.WKBFactory()
geojsonfab = osmium.geom.GeoJSONFactory()

class BoundingBoxHandler(osmium.SimpleHandler):
    def __init__(self, city):
        super(BoundingBoxHandler, self).__init__()
        self.city = city
        self.multipolygon = None

    def area(self, a):
        if 'place' in a.tags and a.tags['place'] == 'city':
            if 'name' in a.tags and a.tags['name'] ==  self.city:
                self.save_multipolygon(wkbfab.create_multipolygon(a))

    def save_multipolygon(self, mp):
        poly = wkblib.loads(mp, hex=True)
        self.multipolygon = poly
        
                
class BridgeFilter(osmium.SimpleHandler):

    def __init__(self, multipolygon):
        super(BridgeFilter, self).__init__()
        self.first = True
        self.bridges = set()
        self.multipolygon = multipolygon


    def add_object_to_geojson(self, geojson, tags):
        geom = json.loads(geojson)
        if geom:
            feature = {'type': 'Feature', 'geometry': geom, 'properties': dict(tags)}
            with open("my_geofabrik/output.geojson", 'a') as outfile:
                if self.first:
                    self.first = False
                    outfile.write('[')
                else:
                    outfile.write(',')
                outfile.write(json.dumps(feature))


    def way(self, w):
        if 'bridge' in w.tags and w.tags['bridge'] == 'yes' and 'highway' in w.tags and ('primary' in w.tags['highway'] or w.tags['highway'] == 'secondary'):
            line = wkbfab.create_linestring(w)
            self.add_object_to_geojson(geojsonfab.create_linestring(w), w.tags)
            way_line = wkblib.loads(line, hex=True)
            
            if way_line.intersects(self.multipolygon):
                self.bridges.add(w)
                print(str(way_line))


file = "my_geofabrik/serbia-latest.osm.pbf"

handler = BoundingBoxHandler('Београд')
handler.apply_file(file)

ways = BridgeFilter(handler.multipolygon)
ways.apply_file(file, locations=True, idx='flex_mem')

with open("my_geofabrik/output.geojson", 'a') as outfile:
    outfile.write(']')



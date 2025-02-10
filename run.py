import json
import math
import time
import zipfile

import geopandas as gpd
import requests

OUT_FILENAME = "new_zones"
PAGE_SIZE = 100


properties = ["old_zone", "new_zone"]
# properties = []

if properties:
  outFields = ",".join(properties)
else:
  outFields = "*"

r = requests.get(
    "https://services2.arcgis.com/cclAu9OKhOfjeUdr/ArcGIS/rest/services/Chatt_new_zones_1_18_25_strip/FeatureServer/0/query?where=1%3D1&objectIds=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=&returnGeometry=true&returnCentroid=false&returnEnvelope=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=true&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&collation=&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnTrueCurves=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token="
).json()

if "count" not in r:
  print(r)
  raise Exception("missing count")

count = r["count"]

num_pages = math.ceil(count / PAGE_SIZE)
print("number of pages:", num_pages)

def trim_coord(coord, num_digits=7):
    lon = str(coord[0])
    lat = str(coord[1])
    if "." in lon and len(lon.split(".")[1]) > num_digits:
        coord[0] = float(lon[: lon.index(".") + num_digits + 1])
    if "." in lat and len(lat.split(".")[1]) > num_digits:
        coord[1] = float(lat[: lat.index(".") + num_digits + 1])

features = []
for i in range(num_pages):
    print("i:", i)
    time.sleep(5)
    url = f"https://services2.arcgis.com/cclAu9OKhOfjeUdr/ArcGIS/rest/services/Chatt_new_zones_1_18_25_strip/FeatureServer/0/query?where=1%3D1&objectIds=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields={outFields}&returnGeometry=true&returnCentroid=false&returnEnvelope=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&collation=&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset={(i * PAGE_SIZE)}&resultRecordCount={PAGE_SIZE}&returnZ=false&returnM=false&returnTrueCurves=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="
    for feature in requests.get(url).json()["features"]:
        geometry = feature["geometry"]
        if geometry["type"] == "Polygon":
            for ring in geometry["coordinates"]:
                for coord in ring:
                    trim_coord(coord)
        elif geometry["type"] == "MultiPolygon":
            for polygon in geometry["coordinates"]:
                for ring in polygon:
                    for coord in ring:
                        trim_coord(coord)

        features.append(feature)

featureCollection = { "type": "FeatureCollection", "features": features }

# save zipped csv version
with zipfile.ZipFile(OUT_FILENAME + ".geojson.zip", mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip_file: 
    dumped = json.dumps(featureCollection, ensure_ascii=False, indent=4)
    zip_file.writestr(OUT_FILENAME + ".geojson", data=dumped)
  
gdf = gpd.GeoDataFrame.from_features(features)
gdf = gdf.set_crs(epsg=4326)

# save parquet version
gdf.to_parquet(OUT_FILENAME + ".parquet")

# save as gzipped csv
gdf.to_csv(OUT_FILENAME + ".csv.gz", compression='gzip')

# save as shapefile
gdf.to_file(OUT_FILENAME + ".shp")

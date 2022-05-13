from fastapi import FastAPI
from shapely import wkt,geometry
from fastapi.middleware.cors import CORSMiddleware
import geojson

from sample import Sample


web_service = FastAPI()
orgins = ["*"]
web_service.add_middleware(
    CORSMiddleware,
    allow_origins=orgins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
sample = Sample()
@web_service.get("/")
def read_root():
    seq=sample.sample(1)
    res = [geojson.Feature(geometry=geojson.loads(geojson.dumps(seq[0][0][0])))]
    for i in seq[0][1]:
        res.append(geojson.Feature(geometry=geojson.loads(geojson.dumps(i[0]))))
    return geojson.FeatureCollection(res)

@web_service.get("items/{item_id}")
def read_item(item_id:int):
    return {"item_id":item_id}
# app/config.py
# Hard-coded service/layer/field settings for Queensland datasets

# ── Parcels (DCDB)
# Source: PlanningCadastre / LandParcelPropertyFramework → layer 4 "Cadastral parcels"
# Fields include: lotplan, lot, plan
PARCEL_SERVICE_URL = "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/PlanningCadastre/LandParcelPropertyFramework/MapServer"
PARCEL_LAYER_ID = 4
PARCEL_LOTPLAN_FIELD = "lotplan"   # combined, e.g. 13SP181800
PARCEL_LOT_FIELD = "lot"           # split fallback
PARCEL_PLAN_FIELD = "plan"

# ── Land Types (GLM)
# Source: Environment / LandTypes → layer 1 "Land types"
LANDTYPES_SERVICE_URL = "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/Environment/LandTypes/MapServer"
LANDTYPES_LAYER_ID = 1
LANDTYPES_CODE_FIELD = "lt_code_1"
LANDTYPES_NAME_FIELD = "lt_name_1"

# ── Vegetation (Regulated Vegetation Management)
# Source: Biota / VegetationManagement → layer 109 "RVM - all"
VEG_SERVICE_URL_DEFAULT = "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/Biota/VegetationManagement/MapServer"
VEG_LAYER_ID_DEFAULT = 109
VEG_NAME_FIELD_DEFAULT = "rvm_cat"
VEG_CODE_FIELD_DEFAULT = "rvm_cat"

# ── HTTP / paging
ARCGIS_TIMEOUT = 45          # seconds
ARCGIS_MAX_RECORDS = 2000    # per page (server permits this on these layers)

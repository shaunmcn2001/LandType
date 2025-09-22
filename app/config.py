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

# ── Easements (DCDB easement parcels)
# Source: PlanningCadastre / LandParcelPropertyFramework → layer 9 "Easements"
EASEMENT_SERVICE_URL = "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/PlanningCadastre/LandParcelPropertyFramework/MapServer"
EASEMENT_LAYER_ID = 9
EASEMENT_LOTPLAN_FIELD = "lotplan"
EASEMENT_PARCEL_TYPE_FIELD = "parcel_typ"
EASEMENT_FEATURE_NAME_FIELD = "feat_name"
EASEMENT_TENURE_FIELD = "tenure"
EASEMENT_AREA_FIELD = "lot_area"

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

# ── Groundwater Bores (Registered water bores)
# Source: InlandWaters / GroundAndSurfaceWaterMonitoring → layer 1 "Registered water bores [RDMW and private]"
BORE_SERVICE_URL = "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/InlandWaters/GroundAndSurfaceWaterMonitoring/MapServer"
BORE_LAYER_ID = 1
BORE_NUMBER_FIELD = "rn_char"
BORE_STATUS_LABEL_FIELD = "facility_status_decode"
BORE_STATUS_CODE_FIELD = "facility_status"
BORE_TYPE_LABEL_FIELD = "facility_type_decode"
BORE_TYPE_CODE_FIELD = "facility_type"
BORE_DRILL_DATE_FIELD = "drilled_date"
BORE_REPORT_URL_FIELD = "bore_report_url"
BORE_ICON_MAP = {
    ("EX", "AB"): {
        "label": "Artesian bore",
        "symbol": {
            "type": "esriPMS",
            "url": "c0bd63a150090e7dad0f5d587d3fc664",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAAwAAAAUCAYAAAC58NwRAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAA7klEQVQokdWSMU/CYBCGn6N1c8A/YYybm6ldGpicSAyysrmVxIHRH0D4A00cmIgbg5uLg6UOzAzGkZFOXYwLR0psoMU2bcLCm9z35ZJ7Lu9995lUlHkYINBGfGLJTzlAeWNFC3gta6mGgVHeUoHMYwA+9GxzKw6+fmPLPB8ItI3i/WUugouvHhE9buU3DQR6hTLeayI8UCcC+mlAeSpY5CPvOsCRcLfgsmBWgxMuAH8XCIHzAmiZHXoEWDmWptjylQYsnvmkiXKfKV9Qo5skW0BEgQ5TnQB3KKfADJMh1xK/UgZIdCMvQBz/qvLXWAMkmzjSFUcVrAAAAABJRU5ErkJggg==",
            "contentType": "image/png",
            "width": 9,
            "height": 15,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("EX", "AF"): {
        "label": "Artesian bore",
        "symbol": {
            "type": "esriPMS",
            "url": "c0bd63a150090e7dad0f5d587d3fc664",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAAwAAAAUCAYAAAC58NwRAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAA7klEQVQokdWSMU/CYBCGn6N1c8A/YYybm6ldGpicSAyysrmVxIHRH0D4A00cmIgbg5uLg6UOzAzGkZFOXYwLR0psoMU2bcLCm9z35ZJ7Lu9995lUlHkYINBGfGLJTzlAeWNFC3gta6mGgVHeUoHMYwA+9GxzKw6+fmPLPB8ItI3i/WUugouvHhE9buU3DQR6hTLeayI8UCcC+mlAeSpY5CPvOsCRcLfgsmBWgxMuAH8XCIHzAmiZHXoEWDmWptjylQYsnvmkiXKfKV9Qo5skW0BEgQ5TnQB3KKfADJMh1xK/UgZIdCMvQBz/qvLXWAMkmzjSFUcVrAAAAABJRU5ErkJggg==",
            "contentType": "image/png",
            "width": 9,
            "height": 15,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("EX", "AS"): {
        "label": "Artesian bore",
        "symbol": {
            "type": "esriPMS",
            "url": "c0bd63a150090e7dad0f5d587d3fc664",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAAwAAAAUCAYAAAC58NwRAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAA7klEQVQokdWSMU/CYBCGn6N1c8A/YYybm6ldGpicSAyysrmVxIHRH0D4A00cmIgbg5uLg6UOzAzGkZFOXYwLR0psoMU2bcLCm9z35ZJ7Lu9995lUlHkYINBGfGLJTzlAeWNFC3gta6mGgVHeUoHMYwA+9GxzKw6+fmPLPB8ItI3i/WUugouvHhE9buU3DQR6hTLeayI8UCcC+mlAeSpY5CPvOsCRcLfgsmBWgxMuAH8XCIHzAmiZHXoEWDmWptjylQYsnvmkiXKfKV9Qo5skW0BEgQ5TnQB3KKfADJMh1xK/UgZIdCMvQBz/qvLXWAMkmzjSFUcVrAAAAABJRU5ErkJggg==",
            "contentType": "image/png",
            "width": 9,
            "height": 15,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("EX", "AU"): {
        "label": "Artesian bore",
        "symbol": {
            "type": "esriPMS",
            "url": "c0bd63a150090e7dad0f5d587d3fc664",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAAwAAAAUCAYAAAC58NwRAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAA7klEQVQokdWSMU/CYBCGn6N1c8A/YYybm6ldGpicSAyysrmVxIHRH0D4A00cmIgbg5uLg6UOzAzGkZFOXYwLR0psoMU2bcLCm9z35ZJ7Lu9995lUlHkYINBGfGLJTzlAeWNFC3gta6mGgVHeUoHMYwA+9GxzKw6+fmPLPB8ItI3i/WUugouvHhE9buU3DQR6hTLeayI8UCcC+mlAeSpY5CPvOsCRcLfgsmBWgxMuAH8XCIHzAmiZHXoEWDmWptjylQYsnvmkiXKfKV9Qo5skW0BEgQ5TnQB3KKfADJMh1xK/UgZIdCMvQBz/qvLXWAMkmzjSFUcVrAAAAABJRU5ErkJggg==",
            "contentType": "image/png",
            "width": 9,
            "height": 15,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("AU", "AB"): {
        "label": "Artesian bore (abandoned but useable)",
        "symbol": {
            "type": "esriPMS",
            "url": "42499f6021827379dec9e0ddba2bd61f",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAA4AAAAXCAYAAAA7kX6CAAAACXBIWXMAAA7EAAAOxAGVKw4bAAABUklEQVQ4jeXSPWuTYRTG8d8JbRQhFJ1Ep+6CiIrERlAEBUfBQTuKH0C6dKi4KjgqLro1SkeXIqKgkucRS6yfoYPg4iD4Qs3LEUtQaBo1xc0L7uGC878PF9eZsE1N/HtwOXc4F+vjg1MeaeWCRrTHA9k5eGNu/IP+d7AiVP8OzAyvXMG8NI2nynyhZ96JeD0aLN3BIemScFNo6uupWFbmrOPxeBgs87R0xicHnY3Pikzpi0YsKvK9dE87px2JzuaNF/HQbjUrWdNRFaas5F680fXRVzN4vhnch8s6Fn5lVtdx+6cP+7fKuIZrvrmx4aqeCfetezDwb6V3w2BYlJbsctex+DDI2Hcqusq8INV0tYbBehSKXNLzUpFXMYk9ipyTrqs4v/HJlnXMxJxWrgq3cABH8UTFSfVYHd3jDzWiiaZ2TjqsJ6I/NPPbkxv0NUrbPvLvMGJvI+ahDFAAAAAASUVORK5CYII=",
            "contentType": "image/png",
            "width": 10,
            "height": 17,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("AU", "AF"): {
        "label": "Artesian bore (abandoned but useable)",
        "symbol": {
            "type": "esriPMS",
            "url": "42499f6021827379dec9e0ddba2bd61f",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAA4AAAAXCAYAAAA7kX6CAAAACXBIWXMAAA7EAAAOxAGVKw4bAAABUklEQVQ4jeXSPWuTYRTG8d8JbRQhFJ1Ep+6CiIrERlAEBUfBQTuKH0C6dKi4KjgqLro1SkeXIqKgkucRS6yfoYPg4iD4Qs3LEUtQaBo1xc0L7uGC878PF9eZsE1N/HtwOXc4F+vjg1MeaeWCRrTHA9k5eGNu/IP+d7AiVP8OzAyvXMG8NI2nynyhZ96JeD0aLN3BIemScFNo6uupWFbmrOPxeBgs87R0xicHnY3Pikzpi0YsKvK9dE87px2JzuaNF/HQbjUrWdNRFaas5F680fXRVzN4vhnch8s6Fn5lVtdx+6cP+7fKuIZrvrmx4aqeCfetezDwb6V3w2BYlJbsctex+DDI2Hcqusq8INV0tYbBehSKXNLzUpFXMYk9ipyTrqs4v/HJlnXMxJxWrgq3cABH8UTFSfVYHd3jDzWiiaZ2TjqsJ6I/NPPbkxv0NUrbPvLvMGJvI+ahDFAAAAAASUVORK5CYII=",
            "contentType": "image/png",
            "width": 10,
            "height": 17,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("AU", "AS"): {
        "label": "Artesian bore (abandoned but useable)",
        "symbol": {
            "type": "esriPMS",
            "url": "42499f6021827379dec9e0ddba2bd61f",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAA4AAAAXCAYAAAA7kX6CAAAACXBIWXMAAA7EAAAOxAGVKw4bAAABUklEQVQ4jeXSPWuTYRTG8d8JbRQhFJ1Ep+6CiIrERlAEBUfBQTuKH0C6dKi4KjgqLro1SkeXIqKgkucRS6yfoYPg4iD4Qs3LEUtQaBo1xc0L7uGC878PF9eZsE1N/HtwOXc4F+vjg1MeaeWCRrTHA9k5eGNu/IP+d7AiVP8OzAyvXMG8NI2nynyhZ96JeD0aLN3BIemScFNo6uupWFbmrOPxeBgs87R0xicHnY3Pikzpi0YsKvK9dE87px2JzuaNF/HQbjUrWdNRFaas5F680fXRVzN4vhnch8s6Fn5lVtdx+6cP+7fKuIZrvrmx4aqeCfetezDwb6V3w2BYlJbsctex+DDI2Hcqusq8INV0tYbBehSKXNLzUpFXMYk9ipyTrqs4v/HJlnXMxJxWrgq3cABH8UTFSfVYHd3jDzWiiaZ2TjqsJ6I/NPPbkxv0NUrbPvLvMGJvI+ahDFAAAAAASUVORK5CYII=",
            "contentType": "image/png",
            "width": 10,
            "height": 17,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("AU", "AU"): {
        "label": "Artesian bore (abandoned but useable)",
        "symbol": {
            "type": "esriPMS",
            "url": "42499f6021827379dec9e0ddba2bd61f",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAA4AAAAXCAYAAAA7kX6CAAAACXBIWXMAAA7EAAAOxAGVKw4bAAABUklEQVQ4jeXSPWuTYRTG8d8JbRQhFJ1Ep+6CiIrERlAEBUfBQTuKH0C6dKi4KjgqLro1SkeXIqKgkucRS6yfoYPg4iD4Qs3LEUtQaBo1xc0L7uGC878PF9eZsE1N/HtwOXc4F+vjg1MeaeWCRrTHA9k5eGNu/IP+d7AiVP8OzAyvXMG8NI2nynyhZ96JeD0aLN3BIemScFNo6uupWFbmrOPxeBgs87R0xicHnY3Pikzpi0YsKvK9dE87px2JzuaNF/HQbjUrWdNRFaas5F680fXRVzN4vhnch8s6Fn5lVtdx+6cP+7fKuIZrvrmx4aqeCfetezDwb6V3w2BYlJbsctex+DDI2Hcqusq8INV0tYbBehSKXNLzUpFXMYk9ipyTrqs4v/HJlnXMxJxWrgq3cABH8UTFSfVYHd3jDzWiiaZ2TjqsJ6I/NPPbkxv0NUrbPvLvMGJvI+ahDFAAAAAASUVORK5CYII=",
            "contentType": "image/png",
            "width": 10,
            "height": 17,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("AD", "AB"): {
        "label": "Artesian bore (abandoned and destroyed)",
        "symbol": {
            "type": "esriPMS",
            "url": "f20c19f6d0ac0b416edde9bae2ce2f36",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAA4AAAAXCAYAAAA7kX6CAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAB40lEQVQ4jWNhIBOwUF/jtv/sDF6MP0nXyM+wkeHI/xoGG8YzpGlkYOCAYhJtJABGtEauPz/Yuf//431NjMbQ//+Z7+x8uOoOp7j/ZxYO5u8MDNtk9737qPr9Rck+b605ODVe2P38+ksuMXnl76/rI18c87nPIbzjoJC22WF+tVlOW66J7/PRasXQaLntVu05ThFl+W9vtC+4y93Inj3bVejX57szwl0btXc/nXWWV6HRbefFCbvc9b+iaHzPwh2n8v3l2ZJnez4wzJ8v8ffvX7b////zz58/X+LLm0MNxbIhyQx/WJIZGBgmoWj8ycwqYP7xrsrfv3+fw8QYGRkt//79O4Xz61cGkV+fGX7/Z1DBcCrnn19vzvHIP9C4tNMSxJeRkdnLyMg49/Hjx8vWKQWyv2Lj+2z4+fF1DI2i/75NP8qrNGGNZarZFXfpY7Nnz/7/////fw0NDX+Ud2esEfjz7Q/Xz5tzMTQe9FSbpLfrccItLvFDmrufzb71+rzgfU5RS/m9rwufsvEbur+9nLs6LOwX1ui45CZrZLXt1swHHCLx/XLunGz//ujJ/3j33Ovt5eB1AcbrccYjCBzzUktnYGBInzlzJuvz58//NjQ0/IN7DJ9GGEhPT/+NSw6vRkIAANlVxxPKfSdtAAAAAElFTkSuQmCC",
            "contentType": "image/png",
            "width": 10,
            "height": 17,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("AD", "AF"): {
        "label": "Artesian bore (abandoned and destroyed)",
        "symbol": {
            "type": "esriPMS",
            "url": "f20c19f6d0ac0b416edde9bae2ce2f36",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAA4AAAAXCAYAAAA7kX6CAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAB40lEQVQ4jWNhIBOwUF/jtv/sDF6MP0nXyM+wkeHI/xoGG8YzpGlkYOCAYhJtJABGtEauPz/Yuf//431NjMbQ//+Z7+x8uOoOp7j/ZxYO5u8MDNtk9737qPr9Rck+b605ODVe2P38+ksuMXnl76/rI18c87nPIbzjoJC22WF+tVlOW66J7/PRasXQaLntVu05ThFl+W9vtC+4y93Inj3bVejX57szwl0btXc/nXWWV6HRbefFCbvc9b+iaHzPwh2n8v3l2ZJnez4wzJ8v8ffvX7b////zz58/X+LLm0MNxbIhyQx/WJIZGBgmoWj8ycwqYP7xrsrfv3+fw8QYGRkt//79O4Xz61cGkV+fGX7/Z1DBcCrnn19vzvHIP9C4tNMSxJeRkdnLyMg49/Hjx8vWKQWyv2Lj+2z4+fF1DI2i/75NP8qrNGGNZarZFXfpY7Nnz/7/////fw0NDX+Ud2esEfjz7Q/Xz5tzMTQe9FSbpLfrccItLvFDmrufzb71+rzgfU5RS/m9rwufsvEbur+9nLs6LOwX1ui45CZrZLXt1swHHCLx/XLunGz//ujJ/3j33Ovt5eB1AcbrccYjCBzzUktnYGBInzlzJuvz58//NjQ0/IN7DJ9GGEhPT/+NSw6vRkIAANlVxxPKfSdtAAAAAElFTkSuQmCC",
            "contentType": "image/png",
            "width": 10,
            "height": 17,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("AD", "AS"): {
        "label": "Artesian bore (abandoned and destroyed)",
        "symbol": {
            "type": "esriPMS",
            "url": "f20c19f6d0ac0b416edde9bae2ce2f36",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAA4AAAAXCAYAAAA7kX6CAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAB40lEQVQ4jWNhIBOwUF/jtv/sDF6MP0nXyM+wkeHI/xoGG8YzpGlkYOCAYhJtJABGtEauPz/Yuf//431NjMbQ//+Z7+x8uOoOp7j/ZxYO5u8MDNtk9737qPr9Rck+b605ODVe2P38+ksuMXnl76/rI18c87nPIbzjoJC22WF+tVlOW66J7/PRasXQaLntVu05ThFl+W9vtC+4y93Inj3bVejX57szwl0btXc/nXWWV6HRbefFCbvc9b+iaHzPwh2n8v3l2ZJnez4wzJ8v8ffvX7b////zz58/X+LLm0MNxbIhyQx/WJIZGBgmoWj8ycwqYP7xrsrfv3+fw8QYGRkt//79O4Xz61cGkV+fGX7/Z1DBcCrnn19vzvHIP9C4tNMSxJeRkdnLyMg49/Hjx8vWKQWyv2Lj+2z4+fF1DI2i/75NP8qrNGGNZarZFXfpY7Nnz/7/////fw0NDX+Ud2esEfjz7Q/Xz5tzMTQe9FSbpLfrccItLvFDmrufzb71+rzgfU5RS/m9rwufsvEbur+9nLs6LOwX1ui45CZrZLXt1swHHCLx/XLunGz//ujJ/3j33Ovt5eB1AcbrccYjCBzzUktnYGBInzlzJuvz58//NjQ0/IN7DJ9GGEhPT/+NSw6vRkIAANlVxxPKfSdtAAAAAElFTkSuQmCC",
            "contentType": "image/png",
            "width": 10,
            "height": 17,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("AD", "AU"): {
        "label": "Artesian bore (abandoned and destroyed)",
        "symbol": {
            "type": "esriPMS",
            "url": "f20c19f6d0ac0b416edde9bae2ce2f36",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAA4AAAAXCAYAAAA7kX6CAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAB40lEQVQ4jWNhIBOwUF/jtv/sDF6MP0nXyM+wkeHI/xoGG8YzpGlkYOCAYhJtJABGtEauPz/Yuf//431NjMbQ//+Z7+x8uOoOp7j/ZxYO5u8MDNtk9737qPr9Rck+b605ODVe2P38+ksuMXnl76/rI18c87nPIbzjoJC22WF+tVlOW66J7/PRasXQaLntVu05ThFl+W9vtC+4y93Inj3bVejX57szwl0btXc/nXWWV6HRbefFCbvc9b+iaHzPwh2n8v3l2ZJnez4wzJ8v8ffvX7b////zz58/X+LLm0MNxbIhyQx/WJIZGBgmoWj8ycwqYP7xrsrfv3+fw8QYGRkt//79O4Xz61cGkV+fGX7/Z1DBcCrnn19vzvHIP9C4tNMSxJeRkdnLyMg49/Hjx8vWKQWyv2Lj+2z4+fF1DI2i/75NP8qrNGGNZarZFXfpY7Nnz/7/////fw0NDX+Ud2esEfjz7Q/Xz5tzMTQe9FSbpLfrccItLvFDmrufzb71+rzgfU5RS/m9rwufsvEbur+9nLs6LOwX1ui45CZrZLXt1swHHCLx/XLunGz//ujJ/3j33Ovt5eB1AcbrccYjCBzzUktnYGBInzlzJuvz58//NjQ0/IN7DJ9GGEhPT/+NSw6vRkIAANlVxxPKfSdtAAAAAElFTkSuQmCC",
            "contentType": "image/png",
            "width": 10,
            "height": 17,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("EX", "AC"): {
        "label": "Artesian bore, ceased to flow",
        "symbol": {
            "type": "esriPMS",
            "url": "f451f30f2a7caeaaccca0a5273bc0462",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAAwAAAAeCAYAAAAYa/93AAAACXBIWXMAAA7EAAAOxAGVKw4bAAABCklEQVQ4jdXTsUrDUBjF8f+pEYfaQUTpC3Sugy5pETo5uohPILSr+AQKDk7u3Zysm1O34tJoQSdfwcFBFylC0JpP7KBJNJd0kNJvu5fz455Lcj0mHG9KoG8l6hrmBwUeubZtfPXyAaOIKOU/wTHeLAFjBbMCUpQNBlYhoo2NV20Cjghsn5o6v0Fgq3xwAyx/74kycE7fFqjrLAnEIRYLx0eccmUXNBTGK2067rrEPGtfpX+A8e4AMMdbulIXo5oRfyDkPgmGHLPIDlBJhUfAHg2NkmBLr9zZBiEniCaMKw6AA2q6/fs7rOsFaBFYE7GLr8uZ/vn+GzwR8Zwf+JTT78ANMsKuSpkzMfgEe+NC6t+sJTAAAAAASUVORK5CYII=",
            "contentType": "image/png",
            "width": 9,
            "height": 22,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("AU", "AC"): {
        "label": "Artesian bore, ceased to flow (abandoned but useable",
        "symbol": {
            "type": "esriPMS",
            "url": "48efbc59853a2d4eb45612edd8501dbf",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAA4AAAAgCAYAAAAi7kmXAAAACXBIWXMAAA7EAAAOxAGVKw4bAAABaUlEQVQ4jeXTP2yNURQA8N95XmrQRERYEJPFIPVnEO/TLkQiYSUGi73pYuukk4WxJMYOBosYRAwkfV+HepEaVAwqVtMThEr1iJdvefl4zetUcbZzb36559x7btMmo7mF4EIe1bXsfKwOe+Itu0yjHA6msC6GL3WD+H9hmQexA2Pa+UoRnwfDTu60ahbnenvhOm5o54wibv8ZPsumH55gUdgvPcY96YXwUJkjWnGzDre7Kn3XismqXL0o4q35vKhhUSfvOBGf+mE6g/fKvFyt7JVOKnOtyj/6ZhyP+mEYlY5Lh6p8n3BWGqvyPbYZrZealrGiiKmq1Oe9HouY8zpHdL2T3tThmrualrRzThGdvtvumpE+KGKpDifid3/XhKcWclY6gHFlXsJhP3t38Jd3bMUD8/lSw5SwG8dw37orJvqHoD45p2MFk8o8Ik0ror2Vh/yfg180fB0ennJBRPVFhoED0GC4QWwa/gI+R3JZWkIxTwAAAABJRU5ErkJggg==",
            "contentType": "image/png",
            "width": 10,
            "height": 24,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("AD", "AC"): {
        "label": "Artesian bore, ceased to flow (abandoned and destroyed)",
        "symbol": {
            "type": "esriPMS",
            "url": "059c7d983f9a0d45b3ff99d638188e13",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAA4AAAAgCAYAAAAi7kmXAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAB80lEQVQ4jWNhIBOwDCKNx/4bMnxguMbgxfiTVBv7GAQZahgYGI6SpvE/AyPDPwZG0p1KAIxYjXbbbjg9//ZCRvD3tyiutRcfHAjWf4JXo+XGqyrPucUOHeEUlhT5+enfczaBtF98rBlm2+9uOeWp7IdVo/aqqzz3eKUvsv7/81r1yyulkkeb5zMwMMyZKOfOdo1beqbF9jtbT3iqeGNo5OHlmP6ciYXxJQ+/FoMJ4zeG2bPB4lfc5ebp7HzEcI5XYY7LqjNye8JMHqFofMvM5aD27dntlDuH/BjmzAEJiTEyMlrMmTPnD8PjXd8KVKP/MXByJjEwMDSgaPzLyMQu9vuzJAMDQw5USPr///+uDAwMBiAO19+fjH/+/+fHcCrf3x8PLnLLCm/yN7YB8WfPnn0A5MfU1NQlSke+y71hZH/I+/HHQQyNQr+/VF7hlt6jvftJzVVXmRZ48J35z/Xr0/v9Sv8+vd8caLIBQ+N+X+29VltvTDzJp9Isu+d13Kavj4VesIWC375NuPvf0ZWkw+3re/gisdj3hoFLluubXnHwtV1lVtGkfX/H2btL492qX57nT4/zO41zgQAAnt8tPYwMDAYMRz9f4DhP0PNTVepI0cGbyIfehq/MDAxfCVdoxWDLwMj43/SNeLRhF8jAUC2RgBjebVnHieHhwAAAABJRU5ErkJggg==",
            "contentType": "image/png",
            "width": 10,
            "height": 24,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("EX", "SF"): {
        "label": "Sub-artesian facility",
        "symbol": {
            "type": "esriPMS",
            "url": "cb8b43c612c8b64764bf3cddcf98c0f6",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAAwAAAATCAYAAACk9eypAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAA8ElEQVQokb3SPWoCURSG4fdEjYU2IUGygalNoQR/kFhlA5IVCNqKK7BIYWVvZxXtrGxtMigoFskSbNKKBAYd54gKZsZkRCHJ6e7le+C7P0HOnODvgL7ecEmENFNEHH8wVAOHJsrDdm3ygakVMtL+DkyNsWIAXO/3hFvghVcNk5WWFwg11BV2j9Cgrx3yYrkr5Y6c9YoQd5vSX0BZHgEQYHFYqYcS94lPsXjzgjnPRCkAxkHYBorkxfaCR/lkrEks6ggl2FYcAlUyMvr5HRIyA8qYWkJ4Ii3dP/oa/wFsnN29nwYuuCfE++kgJZNzK/nOGlnKOo+NZwWaAAAAAElFTkSuQmCC",
            "contentType": "image/png",
            "width": 9,
            "height": 14,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("AU", "SF"): {
        "label": "Sub-artesian facility (abandoned but useable)",
        "symbol": {
            "type": "esriPMS",
            "url": "761fe361d5b1925437742ea640aa6ad6",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAA4AAAAXCAYAAAA7kX6CAAAACXBIWXMAAA7EAAAOxAGVKw4bAAABTklEQVQ4jeXRsUuVYRQH4Odcr/4DTiE0xYUcAitCrzjYFIVDS4uziNDQ0GbQ0NBUUtAitDnoUG0RUUjZd2lIiqClocmWILSyQeHeE3xlw71e06ag33bew8N53/NW/WWq/wB8noN6XMYQtvHQtlvGY6M7fJGTwk1hTssJNIQj+ryxkqeNxYdOuJI14baWEWPxXpFnsGo0FhR5RcUiTnXCMCPNl6g9H103YEojh9TjdTusSccVef7XyWHUymk/+/04ijbIZ+GBivtl1XSjXEyPp2XdKp/xbberPpIuGXZRREuR63hnJJ54mcdwSK9nnXDNkgHTGha8yhlbvzd9UtM9XDMcXzvhhWhaznP6zNmyJsveoFARrqrHne7/OB6bmFLkrPAY876462zszO8CdzIanxS5Ib3dDXWH+0j1P4ebKr4fHNZNiMiDwz3Q3vAP+QH64mbnLUrRKQAAAABJRU5ErkJggg==",
            "contentType": "image/png",
            "width": 10,
            "height": 17,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("AD", "SF"): {
        "label": "Sub-artesian facility (abandoned and destroyed)",
        "symbol": {
            "type": "esriPMS",
            "url": "f3c7bf2131e5365300af9ec635d08ab2",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAA4AAAAXCAYAAAA7kX6CAAAACXBIWXMAAA7EAAAOxAGVKw4bAAABz0lEQVQ4jWNhIBOwDLxGy623ot6y8bZ8ZOGSZPn356/Y70/nRH99it/lp38fp0bTHffnn+KRi1f+/uqkx9tLzM/Y+O9c4ZHVvc8pdstz0yWX7X56BzE02m67GXCMRy5B69vTxMtucgtKZ88+wMDAMH+3r+4y5b2vzlzmkd7KwMDAg6HxPTNXg8L319dAmlDczsj47/fhD87Pmfjeum28GLHLX38FisavzGzSRp8fslfOmXMDxP///78cIyOj2pw5c2oYbq5hqFUJ+f+HhdWCgYEBVSPr/79fH3KKPGFiYioF8f/9+9fLyMi4jZGRcS+I/4GZawfLvz9vMJwq/OvLjiu8skkFUsHnPnnwv5s9e/b7////X01NTd2jtudFJSPDfyY5TsbZGBoFGBnzBX5/i/zNznJLYPerAIZHmxk+M7Kxqu581vKAXajK9sOtlXN8dV9iaNzupfrTa+tV9fus/IducksdzlGLZfjNxGLP8/fnP9uPtxbu9dVJxBmP27y1XzAwMKgFrjisdkVYdYvo74+rdR7fap2V7vuNqCS3PsL2FsPR/89u/xfffixdHUMTTo3EAJYRrvELAxPDV9I1WjH4MjAy/iddIx5N+DUSAACpH7Pndo3rmQAAAABJRU5ErkJggg==",
            "contentType": "image/png",
            "width": 10,
            "height": 17,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("EX", "SW"): {
        "label": "Surface water facility",
        "symbol": {
            "type": "esriPMS",
            "url": "99f5e74a0163286a4216b61c5580c6e5",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAAwAAAAMCAYAAABWdVznAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAAzUlEQVQokZWRPQ4BARCFv4m/gkaIuICaggYRKgfgCAqtOIFCodLrVLiAVmNDQuUKGq2IZONvZEXYZW3iJZPJTN6XvMn4+VN+1+1M4wQJk2eLyO03sNQUNwYo5cdssMPQFgUZfwOGJriyAGKvnZAERsw1RFGGTkDooDazXUKfmU6oiGmPVPK4NUqAjBX6DShnDwB8nD4jTVHSP+xbTDZO4ECXCHUg9WG+AA0qYnUbUJUja81h0kOoARFgBbQpiNUfcv4hK3ug+SxXuX/aQ3fQNTFhOE2XWAAAAABJRU5ErkJggg==",
            "contentType": "image/png",
            "width": 9,
            "height": 9,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("AU", "SW"): {
        "label": "Surface water facility (abandoned but useable)",
        "symbol": {
            "type": "esriPMS",
            "url": "2f7922595b8e38165b0f3379241ddc13",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAA4AAAAOCAYAAAAfSC3RAAAACXBIWXMAAA7EAAAOxAGVKw4bAAABJklEQVQoka2SsS9DYRTFf7deaqmYaiExMZiaMEjeRxdisxKDxS7+h04mIxJjR4sYGjGQtM9QjXRBDCpWgzwxoClH+rzl5dGhcbb7ffkl595zPPqU979goHGggGgDNZy99gYbGuaDXWAp+jNCYIiaSjjb+R08k0ebE6COMYaoAAeIS4wjAmXxbTsNDrKOeMe3zdgukZzdUdUyGeo0tMeMvSRBsQA8EGg1fhlBzBKoE89PvDEPHCdBI4eYRkzE8yjGIqIQz3kGyKWtihughbOt2Op5tKOzMtfKEnKPuE2DHfbxaFJTGWeNxLVDSohHnDXTYNG6+21gnHKhbiR5wCfQCjDFZ3SDP3L07ZCqrsjQtfsMTGJU+GKNYrIE6ebMWQv4iaSH+u7qN0kHYEjHOlSbAAAAAElFTkSuQmCC",
            "contentType": "image/png",
            "width": 10,
            "height": 10,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
    ("AD", "SW"): {
        "label": "Surface water facility (abandoned and destroyed)",
        "symbol": {
            "type": "esriPMS",
            "url": "2f7397245aca2589188202baf0ba9fbe",
            "imageData": "iVBORw0KGgoAAAANSUhEUgAAAA4AAAAOCAYAAAAfSC3RAAAACXBIWXMAAA7EAAAOxAGVKw4bAAABpklEQVQokWNhIBOwUFWj3bYbTv/+MbixMvz7+v/n7/kHgvWf4NVoufGqynNusUNHOIUlRX5++veNmf3/LwHWRrPtd7ec8lT2w6pRe9VVnnu80hdZ//95rfrllVLJo83zGRgY5kyUc2e7xi0902L7na0nPFW8MTTy8HJMf87EwviSh1+LwYTxG8Ps2WDxK+5y83R2PmI4x6swx2XVGbk9YSaPUDS+ZeZyUPv27HbKnUN+DHPMgITEGBkZLebMmfOH4fGubwWq0f8YODmTGBgYGlA0/mVkYhf7/VmSgYEhByok/f//f1cGBgYDEIfr70/GP///82M4le/vjwcXuWWFN/kb24D4s2fPPgDyY2pq6hKlI9/l3jCyP+T9+OMghkah318qr3BL79He/aTmqqtMCzz4zvzn+vXp/X6lf5/ebw402YChcb+v9l6rrTcmnuRTaZbd8zpu89fHfC/Z+AoFv3yb8fc/I6vJh9vWd3DF4zFvjQKXLde2vGPh6rrKLS3C+u+vrPaXR7tUv71Onx9m9xpnAgCBPT5aexgYGIyQxY6gK6IkrQIAg+qiuH6oVMUAAAAASUVORK5CYII=",
            "contentType": "image/png",
            "width": 10,
            "height": 10,
            "angle": 0,
            "xoffset": 0,
            "yoffset": 0,
        },
    },
}

# ── Surface Water (Water courses and bodies)
# Source: InlandWaters / WaterCoursesAndBodies → layers 20–37
WATER_SERVICE_URL = "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/InlandWaters/WaterCoursesAndBodies/MapServer"
WATER_LAYER_CONFIG = {
    20: {
        "title": "Farm Dams",
        "service_name": "Farm dam",
        "geometry_type": "esriGeometryPoint",
    },
    21: {
        "title": "Pools or Rockholes",
        "service_name": "Pool or rockhole",
        "geometry_type": "esriGeometryPoint",
    },
    22: {
        "title": "Waterholes",
        "service_name": "Waterhole",
        "geometry_type": "esriGeometryPoint",
    },
    23: {
        "title": "Waterfalls",
        "service_name": "Waterfall",
        "geometry_type": "esriGeometryPoint",
    },
    24: {
        "title": "Coastline",
        "service_name": "Coastline",
        "geometry_type": "esriGeometryPolyline",
    },
    25: {
        "title": "Flats or Swamps",
        "service_name": "Flat or swamp",
        "geometry_type": "esriGeometryPolygon",
    },
    26: {
        "title": "Pondage Areas",
        "service_name": "Pondage area",
        "geometry_type": "esriGeometryPolygon",
    },
    27: {
        "title": "Lakes",
        "service_name": "Lake",
        "geometry_type": "esriGeometryPolygon",
    },
    28: {
        "title": "Reservoirs",
        "service_name": "Reservoir",
        "geometry_type": "esriGeometryPolygon",
    },
    30: {
        "title": "Canal Lines",
        "service_name": "Canal line",
        "geometry_type": "esriGeometryPolyline",
    },
    31: {
        "title": "Canal Areas",
        "service_name": "Canal area",
        "geometry_type": "esriGeometryPolygon",
    },
    33: {
        "title": "Watercourse Lines",
        "service_name": "Watercourse lines",
        "geometry_type": "esriGeometryPolyline",
    },
    34: {
        "title": "Watercourse Areas",
        "service_name": "Watercourse area",
        "geometry_type": "esriGeometryPolygon",
    },
    35: {
        "title": "Water Area Edges",
        "service_name": "Water area edge",
        "geometry_type": "esriGeometryPolyline",
    },
    37: {
        "title": "Watercourse Stream Orders",
        "service_name": "Watercourse stream order",
        "geometry_type": "esriGeometryPolyline",
    },
}
WATER_LAYER_IDS = tuple(WATER_LAYER_CONFIG.keys())
WATER_LAYER_TITLES = {layer_id: meta["title"] for layer_id, meta in WATER_LAYER_CONFIG.items()}

# ── HTTP / paging
ARCGIS_TIMEOUT = 45          # seconds
ARCGIS_MAX_RECORDS = 2000    # per page (server permits this on these layers)

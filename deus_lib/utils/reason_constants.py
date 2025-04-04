# REASONS PRIORITY:
# 1 - Exclusion rules --> NOT_POSSIBLE__FILTERED_SETTINGS__{{FIELD_NAME}}
# 2 - Missing required fields --> FAULTY_DATA__MISSING_REQUIRED_VALUES
# 3 - Not possible by calculation constraints --> NOT_POSSIBLE__TANKERED_FLIGHT
# 4 - Outlier results --> FAULTY_DATA__OUTLIER_VALUES

# See more at Wiki: https://github.com/Deusteam/spitfire_pandas_pipeline/wiki/Behaviour-Reasons

# GENERIC
NOT_POSSIBLE__FILTERED_SETTINGS = 'NOT_POSSIBLE__FILTERED_SETTINGS'
FAULTY_DATA__MISSING_REQUIRED_VALUES = 'FAULTY_DATA__MISSING_REQUIRED_VALUES'
FAULTY_DATA__MISSING_COMPARISON_VALUES = 'FAULTY_DATA__MISSING_COMPARISON_VALUES'
FAULTY_DATA__OUTLIER_VALUES = 'FAULTY_DATA__OUTLIER_VALUES'
NOT_POSSIBLE__COVID_PERIOD = 'NOT_POSSIBLE__COVID_PERIOD'
NOT_POSSIBLE__4_ENGINE_VEHICLE = 'NOT_POSSIBLE__4_ENGINE_VEHICLE'

# AVIATION
FAULTY_DATA__INVALID_ROLLOUT = 'FAULTY_DATA__INVALID_ROLLOUT'
FAULTY_DATA__INVALID_FUEL_TANK = 'FAULTY_DATA__INVALID_FUEL_TANK'
FAULTY_DATA__INVALID_TAXI_FUEL = 'FAULTY_DATA__INVALID_TAXI_FUEL'
FAULTY_DATA__INVALID_TOW = 'FAULTY_DATA__INVALID_TOW'
FAULTY_DATA__INVALID_AIRBORNE_TIME = 'FAULTY_DATA__INVALID_AIRBORNE_TIME'
FAULTY_DATA__INVALID_FUEL_FLOW = 'FAULTY_DATA__INVALID_FUEL_FLOW'

NOT_POSSIBLE__INVALID_LEG = 'NOT_POSSIBLE__INVALID_LEG'
NOT_POSSIBLE__DELAYED_FLIGHT = 'NOT_POSSIBLE__DELAYED_FLIGHT'
NOT_POSSIBLE__TANKERED_FLIGHT = 'NOT_POSSIBLE__TANKERED_FLIGHT'
NOT_POSSIBLE__UNDER_MIN_COOLDOWN = 'NOT_POSSIBLE__UNDER_MIN_COOLDOWN'
NOT_POSSIBLE__UNDER_MIN_WARMUP = 'NOT_POSSIBLE__UNDER_MIN_WARMUP'
NOT_POSSIBLE__TAXI_OVER_LIMIT = 'NOT_POSSIBLE__TAXI_OVER_LIMIT'
NOT_POSSIBLE__TAXI_TOO_SHORT = 'NOT_POSSIBLE__TAXI_TOO_SHORT'
NOT_POSSIBLE__DISCRETIONARY_FUEL_REQUIRED = 'NOT_POSSIBLE__DISCRETIONARY_FUEL_REQUIRED'

# SHIPPING
FAULTY_DATA__INVALID_FUEL_REPORT = 'FAULTY_DATA__INVALID_FUEL_REPORT'
FAULTY_DATA__INVALID_SPEED = 'FAULTY_DATA__INVALID_SPEED'
FAULTY_DATA__INVALID_SFOC = 'FAULTY_DATA__INVALID_SFOC'
FAULTY_DATA__INVALID_DFOC = 'FAULTY_DATA__INVALID_DFOC'
FAULTY_DATA__INVALID_DELTA_SFOC = 'FAULTY_DATA__INVALID_DELTA_SFOC'
FAULTY_DATA__INVALID_RUNNING_HOURS = 'FAULTY_DATA__INVALID_RUNNING_HOURS'
FAULTY_DATA__INVALID_KWHRS = 'FAULTY_DATA__INVALID_KWHRS'
FAULTY_DATA__INVALID_AUX_ENGINES_CONSUMPTION = 'FAULTY_DATA__INVALID_AUX_ENGINES_CONSUMPTION'
FAULTY_DATA__INVALID_PROPULSION_CONSUMPTION = 'FAULTY_DATA__INVALID_PROPULSION_CONSUMPTION'
FAULTY_DATA__INVALID_GENERATOR_CONSUMPTION = 'FAULTY_DATA__INVALID_GENERATOR_CONSUMPTION'
FAULTY_DATA__INVALID_MEAN_DRAFT = 'FAULTY_DATA__INVALID_MEAN_DRAFT'
FAULTY_DATA__EAEU_NOT_USED = 'FAULTY_DATA__EAEU_NOT_USED'
FAULTY_DATA__LOAD_OVER_LIMIT = 'FAULTY_DATA__LOAD_OVER_LIMIT'
NOT_POSSIBLE__TRIM_OUT_OF_LIMITS = 'NOT_POSSIBLE__TRIM_OUT_OF_LIMITS'
NOT_POSSIBLE__EAEU_OVER_THRESHOLD = 'NOT_POSSIBLE__EAEU_OVER_THRESHOLD'
NOT_POSSIBLE__NOT_AT_SEA = 'NOT_POSSIBLE__NOT_AT_SEA'
NOT_POSSIBLE__VOYAGE_TOO_SHORT = 'NOT_POSSIBLE__VOYAGE_TOO_SHORT'
NOT_POSSIBLE__DELAYED_VOYAGE = 'NOT_POSSIBLE__DELAYED_VOYAGE'
NOT_POSSIBLE__SPEED_UNDER_MIN = 'NOT_POSSIBLE__SPEED_UNDER_MIN'
NOT_POSSIBLE__TRIM_NOT_AVAILABLE = 'NOT_POSSIBLE__TRIM_NOT_AVAILABLE'
FAULTY_DATA__EAEU_LOAD_NOT_AVAILABLE = 'FAULTY_DATA__EAEU_LOAD_NOT_AVAILABLE'
NOT_POSSIBLE__BAD_WEATHER = 'NOT_POSSIBLE__BAD_WEATHER'
NOT_POSSIBLE__TANK_CLEANING = 'NOT_POSSIBLE__TANK_CLEANING'
FAULTY_DATA__SFOC_NOT_AVAILABLE = 'FAULTY_DATA__SFOC_NOT_AVAILABLE'
NOT_POSSIBLE__TRIM_NOT_ACHIEVABLE_WHEN_BALLAST = 'NOT_POSSIBLE__TRIM_NOT_ACHIEVABLE_WHEN_BALLAST'
NOT_POSSIBLE__MANOEUVRE_TIME_OVER_LIMIT = 'NOT_POSSIBLE__MANOEUVRE_TIME_OVER_LIMIT'
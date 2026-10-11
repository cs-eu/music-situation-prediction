"""Known situational inputs and music outcomes in raw and legacy schemas."""

TARGET_PREFIXES = ("liwc_", "topic_", "genius_", "music_track_")
START_COLUMNS = ("start_time", "session_start", "session_start_time")
END_COLUMNS = ("end_time", "session_stop", "session_end_time")
CYCLIC_COLUMNS = ("hour_sin", "hour_cos", "dow_sin", "dow_cos")
CATEGORICAL_COLUMNS = ("gps_landuse_type", "time_bin")
GPS_COORDINATES = ("gps_first_latitude", "gps_first_longitude")
GPS_MISSING_COLUMNS = tuple(
    f"gps_missing__missingindicator_{column}" for column in GPS_COORDINATES
)

COUNT_COLUMNS = frozenset(
    ["gps_count", "session_duration_sec", "duration_sec"]
    + [f"headset_{state}" for state in ("PLUGGED", "UNPLUGGED")]
    + [
        f"detected_{state}"
        for state in (
            "STILL", "WALKING", "RUNNING", "ON_BICYCLE", "ON_FOOT",
            "IN_VEHICLE", "IN_ROAD_VEHICLE", "IN_RAIL_VEHICLE",
            "IN_FOUR_WHEELER_VEHICLE",
        )
    ]
    + [
        f"phone_{state}"
        for state in (
            "AIRPLANE", "APPS", "BATTERYSAVINGMODE", "BLUETOOTH", "CAMERA",
            "CALENDAR", "GPS", "NOTIFICATION", "PHONE", "POWER", "SCREEN", "SMS",
        )
    ]
    + [
        f"app_{category}"
        for category in (
            "Audio_Entertainment", "Career", "Communication", "Creativity", "Dating",
            "Finance", "Food", "Gaming", "Health", "Internet", "Knowledge", "News",
            "Orientation", "Photo", "Reading", "Security", "Settings", "Shopping",
            "Social_Media", "Spirituality", "System", "Time", "Tools", "Transportation",
            "Unknown", "Visual_Entertainment", "Weather",
        )
    ]
    + [
        f"screen_{state}"
        for state in ("ON_UNLOCKED", "OFF_UNLOCKED", "OFF_LOCKED", "ON_LOCKED", "UNKNOWN")
    ]
)
CONTEXT_COLUMNS = COUNT_COLUMNS | frozenset(
    START_COLUMNS + END_COLUMNS + CYCLIC_COLUMNS + CATEGORICAL_COLUMNS
    + GPS_COORDINATES + GPS_MISSING_COLUMNS + ("gps_home", "gps_work")
)


def normalize_column(column: str) -> str:
    name = str(column)
    while "__" in name and name.split("__", 1)[0] in {"num", "bin", "cat", "pass"}:
        name = name.split("__", 1)[1]
    return name


def is_outcome_column(column: str) -> bool:
    return normalize_column(column).startswith(("liwc_", "topic_", "genius_", "music_"))


def is_target_column(column: str) -> bool:
    name = normalize_column(column)
    return (
        name.startswith(TARGET_PREFIXES)
        and not (name == "music_track_key" or name.startswith("music_track_key_"))
        and not str(column).startswith("cat__")
    )


def is_context_column(column: str) -> bool:
    name = normalize_column(column)
    if is_outcome_column(column):
        return False
    return name in CONTEXT_COLUMNS or (
        str(column).startswith("cat__")
        and any(name.startswith(f"{base}_") for base in CATEGORICAL_COLUMNS)
    )

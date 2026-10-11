from pathlib import Path
import sys
from types import SimpleNamespace

import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.base import clone

from data.dataset import SituationDataset, get_dataloader
from data.preprocess import ContextFeatures, DataPreprocessor, build_preprocessor, load_data
from data.split import DataSplitter
from utils.training import get_feature_columns, get_group_column, get_target_columns


@pytest.mark.parametrize("prefix", ["", "num__"])
def test_target_subset_never_leaks_other_outcome_families(prefix):
    frame = pd.DataFrame({
        "user_id": [1, 2],
        f"{prefix}gps_count": [1.0, 2.0],
        f"{prefix}music_track_energy": [0.1, 0.2],
        f"{prefix}liwc_anger": [2.0, np.nan],
        f"{prefix}liwc_empty": [np.nan, np.nan],
        f"{prefix}topic_Topic_0": [0.4, 0.6],
        f"{prefix}genius_joy": [0.2, 0.3],
        f"{prefix}music_track_key": [1.5, 3.5],
        "cat__music_track_key_1": [1.0, 0.0],
        "bin__music_track_non_music": [0.0, 1.0],
        "music_session_track_count": [10.0, 20.0],
        "record_id": [99, 100],
        "upload_timestamp": [1700, 1800],
        "unidentified_metadata": [1.0, 2.0],
        "app_user_id": [1.0, 2.0],
    })
    selected = [f"{prefix}music_track_energy"]
    assert get_feature_columns(frame, selected, "user_id") == [f"{prefix}gps_count"]
    targets = get_target_columns(frame, {})
    assert f"{prefix}music_track_key" not in targets
    assert "cat__music_track_key_1" not in targets
    assert f"{prefix}liwc_empty" in targets
    assert "bin__music_track_non_music" in targets
    assert get_target_columns(frame, {"target_columns": selected}) == selected
    assert get_target_columns(frame, {"target_prefixes": ["num__liwc_"]}) == [
        f"{prefix}liwc_anger", f"{prefix}liwc_empty",
    ]


@pytest.mark.parametrize("target, message", [
    ("absent", "missing"),
    ("gps_count", "not a valid"),
    ("music_track_key", "not a valid"),
    ("cat__music_track_key_0", "not a valid"),
    ("genius_text", "numeric"),
])
def test_requested_targets_are_validated(target, message):
    frame = pd.DataFrame({
        "gps_count": [1.0], "music_track_key": [1.5],
        "cat__music_track_key_0": [1.0], "genius_text": ["not numeric"],
    })
    with pytest.raises(ValueError, match=message):
        get_target_columns(frame, {"target_columns": [target]})


def test_group_default_and_normalized_drop_columns():
    assert get_group_column({}) == "user_id"
    assert get_group_column({"dataset": {"groupsplit_column": "pass__user_id"}}) == "pass__user_id"
    frame = pd.DataFrame({
        "pass__user_id": [1], "num__gps_count": [1.0], "bin__headset_PLUGGED": [1.0],
        "num__music_track_energy": [0.2], "cat__gps_landuse_type_road": [1.0],
    })
    assert get_feature_columns(frame, ["num__music_track_energy"], "pass__user_id", ["gps_count"]) == [
        "bin__headset_PLUGGED", "cat__gps_landuse_type_road",
    ]
    with pytest.raises(ValueError, match="Group column"):
        get_feature_columns(frame, ["num__music_track_energy"], "user_id")


def test_csv_loading_preserves_observed_and_missing_labels(tmp_path):
    frame = pd.DataFrame({
        "user_id": [1, 2], "gps_count": [1.0, np.nan],
        "music_track_energy": [0.2, np.nan], "liwc_empty": [np.nan, np.nan],
        "start_time": ["2024-01-01 06:00:00", "2024-01-02 06:00:00"],
    })
    path = tmp_path / "raw.csv"
    frame.to_csv(path, index=False)
    loaded = load_data(path)
    pd.testing.assert_frame_equal(loaded, frame)
    assert "hour_sin" not in loaded


def test_rds_loading_preserves_data_and_supports_dataframe_adapters(monkeypatch):
    frame = pd.DataFrame({"user_id": [1, 2], "music_track_energy": [0.2, np.nan]})
    adapter = SimpleNamespace(to_pandas=lambda: frame)
    monkeypatch.setitem(sys.modules, "pyreadr", SimpleNamespace(read_r=lambda path: {None: adapter}))
    pd.testing.assert_frame_equal(load_data(Path("raw.rds")), frame)


def test_loading_rejects_unknown_format_or_multiple_rds_objects(monkeypatch):
    with pytest.raises(ValueError, match="CSV or RDS"):
        load_data(Path("data.parquet"))
    monkeypatch.setitem(sys.modules, "pyreadr", SimpleNamespace(read_r=lambda path: {"a": 1, "b": 2}))
    with pytest.raises(ValueError, match="exactly one"):
        load_data(Path("raw.rds"))


def test_preprocessor_fits_statistics_on_training_only_and_unknown_categories():
    train = pd.DataFrame({
        "gps_first_latitude": [1.0, np.nan, 3.0],
        "gps_first_longitude": [-3.0, -2.0, np.nan],
        "gps_landuse_type": ["home", "home", None],
    })
    test = pd.DataFrame({
        "gps_first_latitude": [1000.0, np.nan],
        "gps_first_longitude": [-1000.0, np.inf],
        "gps_landuse_type": ["unseen", pd.NA],
    })
    preprocessor = build_preprocessor(train)
    assert not hasattr(preprocessor["context"], "feature_names_in_")
    train_values = preprocessor.fit_transform(train)
    imputer = preprocessor["columns"].named_transformers_["num"]["imputer"]
    scaler = preprocessor["columns"].named_transformers_["num"]["scaler"]
    np.testing.assert_allclose(imputer.statistics_, [2.0, -2.5])
    means = scaler.mean_.copy()
    test_values = preprocessor.transform(test)
    np.testing.assert_allclose(scaler.mean_, means)
    assert isinstance(train_values, np.ndarray)
    assert train_values.shape[1] == test_values.shape[1] == len(preprocessor.get_feature_names_out())
    assert np.isfinite(test_values).all()
    names = list(preprocessor.get_feature_names_out())
    category = names.index("cat__gps_landuse_type_home")
    np.testing.assert_allclose(test_values[:, category], [0.0, 1.0])
    missing = names.index("gps_missing__missingindicator_gps_first_latitude")
    np.testing.assert_allclose(test_values[:, missing], [0.0, 1.0])


def test_empty_training_features_and_new_gps_missingness_are_retained():
    train = pd.DataFrame({
        "gps_first_latitude": [1.0, 2.0], "gps_first_longitude": [np.nan, np.nan],
        "gps_landuse_type": [None, pd.NA], "phone_APPS": [np.nan, np.nan],
    })
    test = pd.DataFrame({
        "gps_first_latitude": [np.nan], "gps_first_longitude": [-30.0],
        "gps_landuse_type": ["unseen"], "phone_APPS": [5.0],
    })
    preprocessor = build_preprocessor(train)
    initial = preprocessor.fit_transform(train)
    transformed = preprocessor.transform(test)
    assert transformed.shape == (1, initial.shape[1])
    assert np.isfinite(initial).all() and np.isfinite(transformed).all()
    names = list(preprocessor.get_feature_names_out())
    assert "num__gps_first_longitude" in names
    assert "num__phone_APPS" in names
    assert transformed[0, names.index("gps_missing__missingindicator_gps_first_latitude")] == 1


def test_deterministic_cyclic_time_duration_and_log_counts_do_not_mutate_input():
    frame = pd.DataFrame({
        "start_time": pd.to_datetime(["2024-01-01 06:30:00", "2024-01-07 23:30:00"]),
        "end_time": pd.to_datetime(["2024-01-01 07:30:00", "2024-01-08 00:30:00"]),
        "headset_PLUGGED": [3.0, 15.0], "app_Communication": [0.0, 30.0],
        "gps_first_longitude": [-20.0, -10.0],
    })
    original = frame.copy(deep=True)
    values = ContextFeatures().fit_transform(frame)
    pd.testing.assert_frame_equal(frame, original)
    assert "start_time" not in values and "end_time" not in values
    np.testing.assert_allclose(values["headset_PLUGGED"], np.log1p([3.0, 15.0]))
    np.testing.assert_allclose(values["app_Communication"], np.log1p([0.0, 30.0]))
    np.testing.assert_allclose(values["gps_first_longitude"], [-20.0, -10.0])
    np.testing.assert_allclose(values["session_duration_sec"], np.log1p([3600.0, 3600.0]))
    np.testing.assert_allclose(values["hour_sin"], np.sin(2 * np.pi * np.array([6.5, 23.5]) / 24))
    np.testing.assert_allclose(values["dow_cos"], np.cos(2 * np.pi * np.array([0.0, 6.0]) / 7))


def test_configured_drops_are_not_recreated_from_timestamps():
    frame = pd.DataFrame({
        "start_time": ["2026-01-01T08:00:00", "2026-01-02T20:00:00"],
        "end_time": ["2026-01-01T09:00:00", "2026-01-02T21:00:00"],
        "gps_count": [1, 2],
    })
    preprocessor = build_preprocessor(frame, drop_features=["session_duration_sec", "hour_sin"])
    preprocessor.fit(frame)
    names = preprocessor.get_feature_names_out()
    assert not any("session_duration" in name or "hour_sin" in name for name in names)
    assert any("hour_cos" in name for name in names)
    assert np.isfinite(preprocessor.transform(frame)).all()


def test_legacy_standardized_counts_are_not_logged_again():
    frame = pd.DataFrame({"num__phone_APPS": [-2.0, 2.0], "bin__headset_PLUGGED": [0.0, 1.0]})
    transformed = ContextFeatures().fit_transform(frame)
    pd.testing.assert_frame_equal(transformed, frame)


@pytest.mark.parametrize("column", ["music_track_energy", "cat__music_track_key_0", "user_id", "unknown"])
def test_preprocessor_rejects_labels_ids_and_unknown_metadata(column):
    with pytest.raises(ValueError, match="forbidden"):
        build_preprocessor(pd.DataFrame({column: [1.0]}))


def test_preprocessor_validates_negative_counts_missing_schema_and_bad_values():
    with pytest.raises(ValueError, match="negative"):
        build_preprocessor(pd.DataFrame({"phone_APPS": [-1.0]})).fit_transform(pd.DataFrame({"phone_APPS": [-1.0]}))
    train = pd.DataFrame({"gps_count": [0.0, 1.0]})
    preprocessor = build_preprocessor(train).fit(train)
    with pytest.raises(ValueError, match="Missing context"):
        preprocessor.transform(pd.DataFrame({"phone_APPS": [1.0]}))
    with pytest.raises(ValueError, match="numeric"):
        preprocessor.transform(pd.DataFrame({"gps_count": ["bad"]}))


def test_preprocessor_is_cloneable_and_persists_inference(tmp_path):
    train = pd.DataFrame({"gps_count": [0.0, 2.0], "gps_landuse_type": ["home", None]})
    test = pd.DataFrame({"gps_count": [np.nan], "gps_landuse_type": ["unseen"]})
    preprocessor = clone(build_preprocessor(train)).fit(train)
    path = tmp_path / "preprocessor.joblib"
    joblib.dump(preprocessor, path)
    restored = joblib.load(path)
    np.testing.assert_allclose(restored.transform(test), preprocessor.transform(test))
    np.testing.assert_array_equal(restored.get_feature_names_out(), preprocessor.get_feature_names_out())


def test_legacy_preprocessor_wrapper_never_imputes_outcomes():
    frame = pd.DataFrame({"user_id": [1, 2], "gps_count": [1.0, 2.0], "music_track_energy": [0.2, np.nan]})
    values = DataPreprocessor({"dataset": {"drop_features": []}}).fit_transform(frame)
    assert values.shape == (2, 1)
    assert pd.isna(frame.loc[1, "music_track_energy"])


def test_group_split_is_disjoint_reproducible_and_uses_total_population_fractions():
    # Unequal window counts deliberately make participant fractions differ from row fractions.
    frame = pd.DataFrame({"user_id": np.repeat(np.arange(11), np.arange(1, 12))})
    frame["row_id"] = np.arange(len(frame))
    splitter = DataSplitter(test_size=0.2, val_size=0.2, random_state=42)
    train, val, test = splitter.group_split(frame)
    assert [part.user_id.nunique() for part in (train, val, test)] == [5, 3, 3]
    assert set(train.user_id).isdisjoint(val.user_id)
    assert set(train.user_id).isdisjoint(test.user_id)
    assert set(val.user_id).isdisjoint(test.user_id)
    assert sorted(pd.concat([train, val, test]).row_id) == list(frame.row_id)
    for actual, expected in zip(splitter.group_split(frame), (train, val, test)):
        pd.testing.assert_frame_equal(actual, expected)


def test_group_split_is_independent_of_row_order():
    frame = pd.DataFrame({"user_id": np.repeat(np.arange(20), 3), "gps_count": np.arange(60)})
    splitter = DataSplitter(test_size=0.2, val_size=0.1, random_state=42)
    original = splitter.group_split(frame)
    reordered = splitter.group_split(frame.sample(frac=1, random_state=99))
    for first, second in zip(original, reordered):
        assert set(first["user_id"]) == set(second["user_id"])


def test_group_split_without_validation_and_mixed_scalar_identifiers():
    frame = pd.DataFrame({"user_id": [1, 1, "two", "two", 3, 3]})
    train, val, test = DataSplitter(0.2).group_split(frame)
    assert val is None
    assert set(train.user_id).isdisjoint(test.user_id)
    assert len(train) + len(test) == len(frame)


@pytest.mark.parametrize("test_size,val_size", [
    (0, 0), (1, 0), (-0.1, 0), (np.nan, 0), (0.2, np.inf),
    (0.2, -0.1), (0.5, 0.5), (True, 0), ("0.2", 0),
])
def test_invalid_split_fractions_fail(test_size, val_size):
    with pytest.raises(ValueError):
        DataSplitter(test_size, val_size)


@pytest.mark.parametrize("groups", [[1], [1, np.nan], [1, np.inf], ["one", " "], []])
def test_invalid_groups_fail(groups):
    with pytest.raises(ValueError):
        DataSplitter().group_split(pd.DataFrame({"user_id": groups}))


def test_too_few_groups_for_all_splits_fails():
    with pytest.raises(ValueError, match="Too few unique"):
        DataSplitter(0.4, 0.4).group_split(pd.DataFrame({"user_id": [1, 2, 3]}))


def test_unhashable_participant_ids_fail_clearly():
    with pytest.raises(ValueError, match="hashable"):
        DataSplitter().group_split(pd.DataFrame({"user_id": [[1], [2]]}))


def test_dataloader_default_features_exclude_all_other_outcomes():
    frame = pd.DataFrame({
        "user_id": [1, 2], "num__gps_count": [0.0, 1.0],
        "music_track_energy": [0.2, 0.4], "genius_joy": [0.5, 0.6],
        "cat__music_track_key_0": [1.0, 0.0], "unidentified_metadata": [1.0, 2.0],
    })
    loader = get_dataloader(frame, {"target_columns": "music_track_energy"}, batch_size=2, shuffle=False, num_workers=0)
    features, targets = next(iter(loader))
    assert features.shape == (2, 1) and targets.shape == (2, 1)
    np.testing.assert_allclose(features.numpy().ravel(), [0.0, 1.0])


def test_dataloader_accepts_preprocessor_output_schema_including_gps_indicators():
    raw = pd.DataFrame({"gps_first_latitude": [np.nan, 1.0], "gps_landuse_type": ["home", "work"]})
    preprocessor = build_preprocessor(raw)
    frame = pd.DataFrame(preprocessor.fit_transform(raw), columns=preprocessor.get_feature_names_out())
    frame["music_track_energy"] = [0.2, 0.3]
    dataset = SituationDataset(frame, {"target_columns": ["music_track_energy"]})
    assert dataset.X.shape == (2, len(preprocessor.get_feature_names_out()))


def test_dataloader_rejects_configured_group_column_even_if_named_like_context():
    frame = pd.DataFrame({"gps_count": [1.0], "music_track_energy": [0.2]})
    with pytest.raises(ValueError, match="forbidden"):
        SituationDataset(frame, {
            "target_columns": ["music_track_energy"], "feature_columns": ["gps_count"],
            "group_column": "gps_count",
        })


@pytest.mark.parametrize("column,value,message", [
    ("gps_count", np.nan, "finite"), ("gps_count", np.inf, "finite"),
    ("music_track_energy", np.nan, "finite"), ("music_track_energy", np.inf, "finite"),
    ("gps_count", "bad", "numeric"),
])
def test_dataloader_rejects_missing_labels_nonfinite_and_nonnumeric_input(column, value, message):
    frame = pd.DataFrame({"gps_count": [1.0], "music_track_energy": [0.2]})
    frame[column] = [value]
    with pytest.raises(ValueError, match=message):
        SituationDataset(frame, {"target_columns": ["music_track_energy"]})


@pytest.mark.parametrize("feature", ["genius_joy", "cat__music_track_key_0", "user_id", "unidentified_metadata"])
def test_dataloader_explicit_features_cannot_reintroduce_outcomes_or_metadata(feature):
    frame = pd.DataFrame({"music_track_energy": [0.2], feature: [1.0]})
    with pytest.raises(ValueError, match="forbidden"):
        SituationDataset(frame, {"target_columns": ["music_track_energy"], "feature_columns": [feature]})

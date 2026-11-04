from pathlib import Path

import polars as pl
import pytest

from climate_finance import schema
from climate_finance.build.btr_institutions import (
    institution_key,
    institution_map,
    read_institution_map,
)
from climate_finance.build.normalise import btr_multilateral


@pytest.mark.parametrize(
    ("name", "key"),
    [
        ("World Bank (IDA)", "world bank ida"),
        ("  Adaptation Fund ", "adaptation fund"),
        (
            "IDA/International Development Association/The World Bank",
            "ida international development association the world bank",
        ),
        ("Alliance forêts tropicales", "alliance forets tropicales"),
        ("United Nations Children's Fund", "united nations children s fund"),
        ("GCF_Green Climate Fund", "gcf green climate fund"),
    ],
)
def test_an_institution_key_folds_case_accents_spaces_and_punctuation(
    name: str, key: str
) -> None:
    assert institution_key(name) == key


def test_the_shipped_map_groups_spellings_and_leaves_unknown_names_out() -> None:
    names = institution_map()

    assert names.group("World Bank (International Development Association)") == (
        "World Bank"
    )
    assert (
        names.group("Worldbank Group: IDA/AID International Development Association")
        == "World Bank"
    )
    assert names.groups["World Bank"] == (44001, 44002, 44003, 44007)
    assert names.group("Climate Risk and Early Warning Systems Initiative") is None


@pytest.mark.parametrize(
    ("text", "problem"),
    [
        ('[A]\nchannel_codes = []\nnames = ["a"]\n', "A: no channel codes"),
        ('[A]\nchannel_codes = [1]\nnames = ["World Bank"]\n', "not written as its"),
        (
            '[A]\nchannel_codes = [1]\nnames = ["a"]\n'
            '[B]\nchannel_codes = [2]\nnames = ["a"]\n',
            "name 'a' is in several groups",
        ),
        (
            '[A]\nchannel_codes = [1]\nnames = ["a"]\n'
            '[B]\nchannel_codes = [1]\nnames = ["b"]\n',
            "channel 1 is in several groups",
        ),
    ],
)
def test_a_map_with_a_shared_or_unkeyed_entry_is_refused(
    tmp_path: Path, text: str, problem: str
) -> None:
    path = tmp_path / "map.toml"
    path.write_text(text, encoding="utf-8")

    with pytest.raises(ValueError, match=problem):
        read_institution_map(path)


def test_table_iii_2_rows_take_their_group_and_the_dac_rate() -> None:
    # Germany converts at 0.85 euros a dollar, against a DAC rate of 0.8; its LDCF
    # row gives no core amount and its initiative is in no group.
    read = {
        "row_id": ["BTRM-DEU-2021-00012", "BTRM-DEU-2021-00013", "BTRM-DEU-2021-00014"],
        "party": "DEU",
        "provider_code": 5,
        "year": 2021,
        "institution": [
            "World Bank (IDA)",
            "Least Developed Country Fund",
            "Climate Risk and Early Warning Systems Initiative",
        ],
        "core_amount_domestic": [8e6, None, None],
        "core_face_usd": [8e6 / 0.85, None, None],
        "climate_specific_amount_domestic": [2e6, 4e6, 1e6],
        "climate_specific_face_usd": [2e6 / 0.85, 4e6 / 0.85, 1e6 / 0.85],
        "currency": "EUR",
        "source_file": "DEU.xlsx#Table2_2021",
    }
    dtypes = schema.BTR_MULTILATERAL.dtypes
    columns = [
        name
        for name in dtypes
        if name not in {"institution_group", "dac_rate"} and "_at_dac" not in name
    ]
    rows = pl.DataFrame(read, schema={name: dtypes[name] for name in read}).select(
        pl.col(name) if name in read else pl.lit(None, dtypes[name]).alias(name)
        for name in columns
    )
    exchange = pl.DataFrame(
        {"iso_code": ["EUI"], "year": [2021], "exchange_rate": [0.8]}
    )

    table = btr_multilateral(rows, exchange=exchange)

    schema.validate(schema.BTR_MULTILATERAL, table)
    assert table.select(
        "institution_group",
        "dac_rate",
        "core_face_usd_at_dac",
        "climate_specific_face_usd_at_dac",
    ).rows() == [
        ("World Bank", 0.8, pytest.approx(1e7), pytest.approx(2.5e6)),
        ("Least Developed Countries Fund", 0.8, None, pytest.approx(5e6)),
        (None, 0.8, None, pytest.approx(1.25e6)),
    ]
    # No grant equivalent given: the party's own USD, null here, stands.
    assert table["core_grant_equiv_usd_at_dac"].is_null().all()

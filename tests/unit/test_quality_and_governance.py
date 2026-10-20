from bikeshare_lakehouse.governance.functions import mask_ride_id_sql
from bikeshare_lakehouse.quality.checks import CHECKS


def test_every_check_targets_the_given_catalog():
    for check in CHECKS:
        sql = check.render("my_catalog")
        assert "`my_catalog`." in sql
        assert "{c}" not in sql


def test_mask_function_reveals_ride_id_only_to_engineers():
    sql = mask_ride_id_sql("my_catalog", "data_engineers")
    assert "`my_catalog`.silver.mask_ride_id" in sql
    assert "is_account_group_member('data_engineers') THEN ride_id" in sql
    assert "sha2(ride_id, 256)" in sql

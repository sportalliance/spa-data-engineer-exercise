from conftest import df_from_dict, dict_from_df

from exercise.canary import hello_world


def test_hello_world(spark_session):
    df = df_from_dict(spark_session, {"col_a": [1, 2, 3]})
    expected = {
        "col_a": [1, 2, 3],
        "new_col": ["a"] * 3,
    }

    actual = hello_world(df)
    assert dict_from_df(actual.sort("col_a")) == expected

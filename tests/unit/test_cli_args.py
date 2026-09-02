import argparse

from splunk_rest_extractor.cli import join_time_values


def _parser():
    p = argparse.ArgumentParser()
    p.add_argument("--earliest", required=True)
    p.add_argument("--latest", required=True)
    p.add_argument("--spl")
    return p


def test_dash_leading_relative_times_parse_on_every_python():
    argv = join_time_values(["--spl", "index=x", "--earliest", "-1d@d", "--latest", "@d"])
    a = _parser().parse_args(argv)
    assert (a.earliest, a.latest, a.spl) == ("-1d@d", "@d", "index=x")


def test_join_leaves_other_forms_alone():
    assert join_time_values(["--earliest=-7d", "--latest", "now"]) == ["--earliest=-7d", "--latest=now"]
    assert join_time_values(["--earliest", "1534723200", "--latest", "2018-08-21T00:00:00"]) == [
        "--earliest=1534723200", "--latest=2018-08-21T00:00:00"]
    assert join_time_values(["run", "--earliest"]) == ["run", "--earliest"]  # argparse still reports the missing value

import pytest

from funding_top10.config import load_config


def _write(path, text):
    path.write_text(text)
    return path


def test_load_config_valid(tmp_path):
    p = _write(
        tmp_path / "config.yaml",
        """
qijia:
  host: db.example
  port: 5432
  user: alice
  password: secret
  database: mydb
slack:
  webhook: https://hooks.example/abc
  channel: C123
""",
    )
    cfg = load_config(p)
    assert cfg.qijia.host == "db.example"
    assert cfg.qijia.port == 5432
    assert cfg.qijia.user == "alice"
    assert cfg.qijia.database == "mydb"
    assert cfg.slack.webhook == "https://hooks.example/abc"
    assert cfg.slack.channel == "C123"


def test_to_dsn_url_encodes_special_chars(tmp_path):
    p = _write(
        tmp_path / "config.yaml",
        """
qijia:
  host: h
  port: 5432
  user: "a@lice"
  password: "p@ss/word"
  database: db
slack:
  webhook: https://x
""",
    )
    cfg = load_config(p)
    dsn = cfg.qijia.to_dsn()
    # special chars must be percent-encoded so they don't break the URL grammar
    assert "p%40ss%2Fword" in dsn
    assert "a%40lice" in dsn
    # only one literal '@' (the userinfo / host separator)
    assert dsn.count("@") == 1


def test_missing_qijia_field_no_longer_raises(tmp_path):
    """qijia is deprecated since biyi moved to API; partial/empty qijia is now OK."""
    p = _write(
        tmp_path / "config.yaml",
        """
qijia:
  host: h
  port: 5432
  user: ""
  password: pwd
  database: db
slack:
  webhook: https://x
""",
    )
    cfg = load_config(p)
    # Should load without raising. qijia fields can be empty.
    assert cfg.qijia.user == ""


def test_missing_slack_webhook_raises(tmp_path):
    p = _write(
        tmp_path / "config.yaml",
        """
qijia:
  host: h
  port: 5432
  user: u
  password: p
  database: db
slack:
  webhook: ""
""",
    )
    with pytest.raises(RuntimeError, match="slack.webhook"):
        load_config(p)


def test_file_not_found_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "nope.yaml")


def test_optional_channel_defaults_to_empty(tmp_path):
    p = _write(
        tmp_path / "config.yaml",
        """
qijia:
  host: h
  port: 5432
  user: u
  password: p
  database: db
slack:
  webhook: https://x
""",
    )
    cfg = load_config(p)
    assert cfg.slack.channel == ""

from __future__ import annotations

from pathlib import Path

import pytest
from icetl.conf import IcetlConf

from icefleet.config import FleetSettings, parse_bytes, resolve_fleet_settings
from icefleet.errors import ClusterConfigurationError

NO_DOTENV = Path("/nonexistent/.env")


def resolve(conf: dict[str, str] | None = None, env: dict[str, str] | None = None) -> FleetSettings:
    return resolve_fleet_settings(IcetlConf(conf or {}), env=env or {}, dotenv_path=NO_DOTENV)


def test_defaults() -> None:
    settings = resolve()
    assert settings == FleetSettings()
    assert settings.master == "local[2]" and settings.fallback == "single-node"


def test_conf_beats_environment_beats_dotenv(tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text("ICEFLEET_MASTER=local[9]\nICEFLEET_SPLIT_MAX=7\nICEFLEET_FALLBACK=error\n")
    settings = resolve_fleet_settings(
        IcetlConf({"icefleet.master": "local[3]"}),
        env={"ICEFLEET_SPLIT_MAX": "11"},
        dotenv_path=dotenv,
    )
    assert settings.master == "local[3]"  # conf
    assert settings.split_max == 11  # environment
    assert settings.fallback == "error"  # .env


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("4096", 4096),
        ("128MiB", 128 * 1024**2),
        ("2GB", 2 * 1000**3),
        ("1.5 KiB", 1536),
        ("10b", 10),
    ],
)
def test_parse_bytes(text: str, expected: int) -> None:
    assert parse_bytes(text) == expected


@pytest.mark.parametrize(
    "conf",
    [
        {"icefleet.fallback": "maybe"},
        {"icefleet.split.targetBytes": "lots"},
        {"icefleet.split.max": "0"},
        {"icefleet.exchange.keep": "perhaps"},
        {"icefleet.task.timeoutSeconds": "-1"},
    ],
)
def test_bad_values_are_refused(conf: dict[str, str]) -> None:
    with pytest.raises(ClusterConfigurationError):
        resolve(conf)


def test_typed_values() -> None:
    settings = resolve(
        {
            "icefleet.exchange.keep": "true",
            "icefleet.split.targetBytes": "64MiB",
            "icefleet.local.threads": "3",
            "icefleet.task.timeoutSeconds": "30",
        }
    )
    assert settings.exchange_keep is True
    assert settings.split_target_bytes == 64 * 1024**2
    assert settings.local_threads == 3
    assert settings.task_timeout_seconds == 30.0

import json

import pytest

from rightsizer_runtime.env import (
    ATTEMPT_ENV,
    DECIDED_ENV,
    DECISION_ENV,
    DecisionEnv,
    attempt_context,
    config_env_vars,
    current_attempt,
    decision_env_vars,
    has_decision_marker,
    mark_decided,
    parse_config_env,
    read_attempt,
    read_decided,
    read_decision,
    set_attempt_context,
)
from rightsizer_runtime.wire import AttemptContext, Limits, Mode, Requests, ResourceSpec, WireError, new_ulid


def dec(**kw):
    base = dict(decision_id=new_ulid(), key="flyte://o/p/d/env.t", mode="enforce",
                requested=ResourceSpec(Requests(cpu=1000, memory=2 << 30), Limits(memory=2 << 30)), measured=True)
    base.update(kw)
    return DecisionEnv(**base)


def test_decision_env_roundtrip():
    d = dec()
    env = decision_env_vars(d)
    payload = json.loads(env[DECISION_ENV])
    assert payload["v"] == 1 and payload["mode"] == "enforce" and payload["measured"] is True
    assert payload["requested"]["requests"]["memory"] == 2 << 30
    assert " " not in env[DECISION_ENV]  # compact
    assert read_decision(env) == d
    assert has_decision_marker(env) and not has_decision_marker({})


def test_decision_env_malformed_is_none():
    assert read_decision({}) is None
    assert read_decision({DECISION_ENV: "{"}) is None
    assert read_decision({DECISION_ENV: json.dumps({"v": 1, "decision_id": "x", "key": "flyte://o/p/d/t", "mode": "enforce"})}) is None
    with pytest.raises(WireError):
        DecisionEnv.from_json(json.dumps({"v": 1, "decision_id": new_ulid(), "key": "flyte://o/p/d/t", "mode": "max"}))


def test_decision_env_minimal():
    d = DecisionEnv(decision_id=new_ulid(), key="ray://t//f", mode=Mode.OBSERVE)
    assert "requested" not in d.to_dict() and d.to_dict()["measured"] is False


def test_attempt_env_and_contextvar():
    env = {}
    ctx = AttemptContext("dag/run/task/3", 2, 4)
    tok = set_attempt_context(ctx, env)
    try:
        assert json.loads(env[ATTEMPT_ENV])["attempt"] == 2
        assert read_attempt(env) == ctx
        assert current_attempt({}) == ctx  # contextvar wins
    finally:
        attempt_context.reset(tok)
    assert current_attempt(env) == ctx  # falls back to the env mirror
    tok = set_attempt_context(None, env)
    attempt_context.reset(tok)
    assert ATTEMPT_ENV not in env
    assert read_attempt({ATTEMPT_ENV: "garbage"}) is None


def test_decided():
    env = {}
    u = new_ulid()
    mark_decided(u.lower(), env)
    assert env[DECIDED_ENV] == u and read_decided(env) == u
    assert read_decided({DECIDED_ENV: "nope"}) is None and read_decided({}) is None


def test_config_env_parsing():
    env = {
        "RIGHTSIZER__HISTORY__URI": "s3://bucket/rightsizer/",
        "RIGHTSIZER__POLICY__MODE": "enforce",
        "RIGHTSIZER__HISTORY__CACHE_TTL_S": "600",
        "RIGHTSIZER__HISTORY__ALLOW_UNVERIFIED_FOR_REDUCTIONS": "true",
        "RIGHTSIZER__DECIDE__POINT_SIGMA": "0.35",
        "RIGHTSIZER__HISTORY__SHARE": '["cluster-a","cluster-b"]',
        "RIGHTSIZER__A__B__C": "deep",
        "RIGHTSIZER_DECISION": "not config",
        "RIGHTSIZER__": "empty",
        "RIGHTSIZER__X____Y": "empty part",
        "OTHER": "x",
    }
    assert parse_config_env(env) == {
        "history": {"uri": "s3://bucket/rightsizer/", "cache_ttl_s": 600, "allow_unverified_for_reductions": True,
                    "share": ["cluster-a", "cluster-b"]},
        "policy": {"mode": "enforce"},
        "decide": {"point_sigma": 0.35},
        "a": {"b": {"c": "deep"}},
    }
    assert parse_config_env(env, parse_values=False)["history"]["cache_ttl_s"] == "600"


def test_config_env_conflict_skipped():
    env = {"RIGHTSIZER__HISTORY": "flat", "RIGHTSIZER__HISTORY__URI": "s3://x"}
    out = parse_config_env(env)
    assert out == {"history": "flat"}


def test_config_env_roundtrip():
    cfg = {"history": {"uri": "s3://b/p", "keep_last": 64, "share": ["a"]}, "policy": {"mode": "observe", "strict": False}}
    assert parse_config_env(config_env_vars(cfg)) == cfg

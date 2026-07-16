from pystarport.utils import build_cli_args, parse_amount


def test_parse_amount():
    assert parse_amount("1000000.01uatom") == 1000000.01
    assert parse_amount({"amount": "1000000.01", "denom": "uatom"}) == 1000000.01


def test_build_cli_args():
    for safe in [False, True]:
        assert build_cli_args(safe=safe, order_by="desc") == ["--order_by", "desc"]
        assert build_cli_args(safe=safe, chain_id="12") == ["--chain-id", "12"]
        res = build_cli_args("tx_search", safe=safe, order_by="desc", chain_id="12")
        assert res == ["tx_search", "--order_by", "desc", "--chain-id", "12"]
        assert build_cli_args(safe=safe, unordered=True) == ["--unordered"]
        # omitting it would leave a flag defaulting to true switched on
        assert build_cli_args(safe=safe, unordered=False) == ["--unordered=false"]
        # None stays "unset", and other values keep the flag/value pair
        assert build_cli_args(safe=safe, unordered=None) == []
        res = build_cli_args("send", safe=safe, unordered=True, timeout_duration="60s")
        assert res == ["send", "--unordered", "--timeout-duration", "60s"]

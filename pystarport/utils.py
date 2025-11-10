import asyncio
import configparser
import socket
import subprocess
import sys
import time
from enum import Enum
from itertools import takewhile
from urllib.parse import urlparse

from dateutil.parser import isoparse


class BondStatus(Enum):
    UNSPECIFIED = "BOND_STATUS_UNSPECIFIED"
    UNBONDED = "BOND_STATUS_UNBONDED"
    UNBONDING = "BOND_STATUS_UNBONDING"
    BONDED = "BOND_STATUS_BONDED"

    def to_int(self):
        mapping = {
            BondStatus.UNSPECIFIED: 0,
            BondStatus.UNBONDED: 1,
            BondStatus.UNBONDING: 2,
            BondStatus.BONDED: 3,
        }
        return mapping[self]


PRESERVE_UNDERSCORE_FLAGS = {
    "order_by",
    "log_format",
    "log_level",
    "log_no_color",
    "verbose_log_level",
}


def interact(cmd, ignore_error=False, input=None, **kwargs):
    kwargs.setdefault("stderr", subprocess.STDOUT)
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        shell=True,
        **kwargs,
    )
    # begin = time.perf_counter()
    (stdout, _) = proc.communicate(input=input)
    # print('[%.02f] %s' % (time.perf_counter() - begin, cmd))
    if not ignore_error:
        assert proc.returncode == 0, f'{stdout.decode("utf-8")} ({cmd})'
    return stdout


def write_ini(fp, cfg):
    ini = configparser.RawConfigParser()
    for section, items in cfg.items():
        ini.add_section(section)
        sec = ini[section]
        sec.update(items)
    ini.write(fp)


def safe_cli_string(s):
    'wrap string in "", used for cli argument when contains spaces'
    if len(f"{s}".split()) > 1:
        return f"'{s}'"
    return f"{s}"


def build_cli_args_safe(*args, **kwargs):
    return build_cli_args(*args, safe=True, **kwargs)


def build_cli_args(*args, safe=False, **kwargs):
    if safe:
        args = [safe_cli_string(arg) for arg in args if arg]
    else:
        args = [arg for arg in args if arg is not None]
    for k, v in kwargs.items():
        if v is None:
            continue
        flag = "--" + (
            k if k in PRESERVE_UNDERSCORE_FLAGS else k.strip("_").replace("_", "-")
        )
        args.append(flag)
        args.append(safe_cli_string(v) if safe else v)
    return list(map(str, args))


def format_doc_string(**kwargs):
    def decorator(target):
        target.__doc__ = target.__doc__.format(**kwargs)
        return target

    return decorator


def get_sync_info(s):
    return s.get("SyncInfo") or s.get("sync_info")


def parse_amount(coin):
    """
    parse amount from coin representation, compatible with multiple sdk versions:
    - pre-sdk-50: {"denom": "uatom", "amount": "1000000.00"}
    - post-sdk-50: "1000000.00uatom"
    """
    if isinstance(coin, dict):
        return float(coin["amount"])
    else:
        return float("".join(takewhile(is_float, coin)))


def is_float(s):
    return str.isdigit(s) or s == "."


def wait_for_fn(name, fn, *, timeout=120, interval=1):
    for i in range(int(timeout / interval)):
        result = fn()
        if result:
            return result
        time.sleep(interval)
    else:
        raise TimeoutError(f"wait for {name} timeout")


async def wait_for_fn_async(name, fn, *, timeout=120, interval=1):
    for i in range(int(timeout / interval)):
        result = await fn()
        if result:
            return result
        await asyncio.sleep(interval)
    else:
        raise TimeoutError(f"wait for {name} timeout")


def wait_for_block_time(cli, t):
    print("wait for block time", t)
    while True:
        now = isoparse(get_sync_info(cli.status())["latest_block_time"])
        print("block time now:", now)
        if now >= t:
            break
        time.sleep(0.5)


def w3_wait_for_block(w3, height, timeout=120):
    for _ in range(timeout * 2):
        try:
            current_height = w3.eth.block_number
        except Exception as e:
            print(f"get json-rpc block number failed: {e}", file=sys.stderr)
        else:
            if current_height >= height:
                break
            print("current block height", current_height)
        time.sleep(0.5)
    else:
        raise TimeoutError(f"wait for block {height} timeout")


async def w3_wait_for_block_async(w3, height, timeout=120):
    for _ in range(timeout * 2):
        try:
            current_height = await w3.eth.block_number
        except Exception as e:
            print(f"get json-rpc block number failed: {e}", file=sys.stderr)
        else:
            if current_height >= height:
                break
            print("current block height", current_height)
        await asyncio.sleep(0.1)
    else:
        raise TimeoutError(f"wait for block {height} timeout")


def wait_for_new_blocks(cli, n, sleep=0.5, timeout=120):
    cur_height = begin_height = int(get_sync_info(cli.status())["latest_block_height"])
    start_time = time.time()
    while cur_height - begin_height < n:
        time.sleep(sleep)
        cur_height = int(get_sync_info(cli.status())["latest_block_height"])
        if time.time() - start_time > timeout:
            raise TimeoutError(f"wait for block {begin_height + n} timeout")
    return cur_height


def wait_for_block(cli, height, timeout=120):
    for i in range(timeout * 2):
        try:
            status = cli.status()
        except AssertionError as e:
            print(f"get sync status failed: {e}", file=sys.stderr)
        else:
            current_height = int(get_sync_info(status)["latest_block_height"])
            print("current block height", current_height)
            if current_height >= height:
                break
        time.sleep(0.5)
    else:
        raise TimeoutError(f"wait for block {height} timeout")


def wait_for_port(port, host="127.0.0.1", timeout=40.0):
    print("wait for port", port, "to be available")
    start_time = time.perf_counter()
    while True:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                break
        except OSError as ex:
            time.sleep(0.1)
            if time.perf_counter() - start_time >= timeout:
                raise TimeoutError(
                    "Waited too long for the port {} on host {} to start accepting "
                    "connections.".format(port, host)
                ) from ex


def wait_for_url(url, timeout=40.0):
    print("wait for url", url, "to be available")
    start_time = time.perf_counter()
    while True:
        try:
            parsed = urlparse(url)
            host = parsed.hostname
            port = parsed.port
            with socket.create_connection((host, int(port or 80)), timeout=timeout):
                break
        except OSError as ex:
            time.sleep(0.1)
            if time.perf_counter() - start_time >= timeout:
                raise TimeoutError(
                    "Waited too long for the port {} on host {} to start accepting "
                    "connections.".format(port, host)
                ) from ex


def w3_wait_for_new_blocks(w3, n, sleep=0.5):
    begin_height = w3.eth.block_number
    while True:
        time.sleep(sleep)
        cur_height = w3.eth.block_number
        if cur_height - begin_height >= n:
            break

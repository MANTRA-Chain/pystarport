import enum
import hashlib
import json
import subprocess
import tempfile

import bech32
import durations
from dateutil.parser import isoparse

from .app import CHAIN
from .utils import (
    build_cli_args_safe,
    format_doc_string,
    get_sync_info,
    interact,
    parse_amount,
)


class ModuleAccount(enum.Enum):
    FeeCollector = "fee_collector"
    Mint = "mint"
    Gov = "gov"
    Distribution = "distribution"
    BondedPool = "bonded_tokens_pool"
    NotBondedPool = "not_bonded_tokens_pool"
    IBCTransfer = "transfer"


@format_doc_string(
    options=",".join(v.value for v in ModuleAccount.__members__.values())
)
def module_address(name):
    """
    get address of module accounts

    :param name: name of module account, values: {options}
    """
    data = hashlib.sha256(ModuleAccount(name).value.encode()).digest()[:20]
    return bech32.bech32_encode("cro", bech32.convertbits(data, 8, 5))


class ChainCommand:
    def __init__(self, cmd=None):
        self.cmd = cmd or CHAIN

    def prob_genesis_subcommand(self):
        'test if the command has "genesis" subcommand, introduced in sdk 0.50'
        try:
            output = self("genesis")
        except AssertionError:
            # non-zero return code
            return False

        return "Available Commands" in output.decode()

    def prob_icaauth_subcommand(self):
        'test if the command has "icaauth" subcommand, removed after ibc 8.3'
        try:
            output = self("q", "icaauth")
        except AssertionError:
            # non-zero return code
            return False

        return "Available Commands" in output.decode()

    def prob_tendermint_subcommand(self):
        'test if the command has "tendermint" subcommand, removed in evm 0.4.x'
        try:
            output = self("tendermint")
        except AssertionError:
            # non-zero return code
            return False

        return "Available Commands" in output.decode()

    def __call__(self, cmd, *args, stdin=None, stderr=subprocess.STDOUT, **kwargs):
        "execute chain-maind"
        args = " ".join(build_cli_args_safe(cmd, *args, **kwargs))
        return interact(f"{self.cmd} {args}", input=stdin, stderr=stderr)


class CosmosCLI:
    "the apis to interact with wallet and blockchain"

    def __init__(
        self,
        data_dir,
        node_rpc,
        chain_id=None,
        cmd=None,
        gas=250000,
        gas_prices=None,
    ):
        self.data_dir = data_dir
        if chain_id is None:
            src = (self.data_dir / "config" / "genesis.json").read_text()
            self._genesis = json.loads(src)
            self.chain_id = self._genesis["chain_id"]
        else:
            self.chain_id = chain_id
        self.node_rpc = node_rpc
        self.raw = ChainCommand(cmd)
        self.gas = gas
        self.gas_prices = gas_prices
        self.output = None
        self.error = None
        self.has_genesis_subcommand = self.raw.prob_genesis_subcommand()
        self.has_icaauth_subcommand = self.raw.prob_icaauth_subcommand()
        self.has_tendermint_subcommand = self.raw.prob_tendermint_subcommand()

    def node_id(self):
        "get tendermint node id"
        subcmd = "tendermint" if self.has_tendermint_subcommand else "comet"
        output = self.raw(subcmd, "show-node-id", home=self.data_dir)
        return output.decode().strip()

    def get_base_kwargs(self):
        return {
            "home": self.data_dir,
            "node": self.node_rpc,
            "output": "json",
        }

    def get_kwargs(self):
        return self.get_base_kwargs() | {
            "keyring_backend": "test",
            "chain_id": self.chain_id,
        }

    def get_kwargs_with_gas(self):
        gas_kwargs = {}
        if self.gas is not None:
            gas_kwargs["gas"] = self.gas
        if self.gas_prices is not None:
            gas_kwargs["gas_prices"] = self.gas_prices
        return self.get_kwargs() | gas_kwargs

    def delete_account(self, name):
        "delete wallet account in node's keyring"
        return self.raw(
            "keys",
            "delete",
            name,
            "-y",
            "--force",
            home=self.data_dir,
            output="json",
            keyring_backend="test",
        )

    def create_account(self, name, mnemonic=None, ledger=False, **kwargs):
        if kwargs.get("coin_type", 60) == 60:
            kwargs.update({"coin_type": 60, "key_type": "eth_secp256k1"})
        args = {
            "home": self.data_dir,
            "output": "json",
            "keyring_backend": "test",
        }
        cmd = ["keys", "add", name]
        if mnemonic is not None:
            cmd.append("--recover")
        if ledger:
            cmd.append("--ledger")
        if mnemonic is None and kwargs.get("source"):
            cmd.append("--recover")
        output = self.raw(
            *cmd,
            stdin=(mnemonic.encode() + b"\n") if mnemonic else None,
            **(args | kwargs),
        )
        return json.loads(output)

    def list_accounts(self, **kwargs):
        return json.loads(
            self.raw(
                "keys",
                "list",
                **(self.get_base_kwargs() | kwargs),
            )
        )

    def init(self, moniker):
        "the node's config is already added"
        return self.raw(
            "init",
            moniker,
            chain_id=self.chain_id,
            home=self.data_dir,
        )

    def genesis_subcommand(self, *args, **kwargs):
        if self.has_genesis_subcommand:
            return self.raw("genesis", *args, **kwargs)
        else:
            return self.raw(*args, **kwargs)

    def validate_genesis(self, *args):
        return self.genesis_subcommand("validate-genesis", *args, home=self.data_dir)

    def add_genesis_account(self, addr, coins, **kwargs):
        return self.genesis_subcommand(
            "add-genesis-account",
            addr,
            coins,
            home=self.data_dir,
            output="json",
            **kwargs,
        )

    def gentx(self, name, coins, *args, min_self_delegation=1, pubkey=None, **kwargs):
        return self.genesis_subcommand(
            "gentx",
            name,
            coins,
            *args,
            min_self_delegation=str(min_self_delegation),
            home=self.data_dir,
            chain_id=self.chain_id,
            keyring_backend="test",
            pubkey=pubkey,
            **kwargs,
        )

    def collect_gentxs(self, gentx_dir):
        return self.genesis_subcommand("collect-gentxs", gentx_dir, home=self.data_dir)

    def status(self):
        return json.loads(self.raw("status", node=self.node_rpc))

    def block_height(self):
        return int(get_sync_info(self.status())["latest_block_height"])

    def block_time(self):
        return isoparse(get_sync_info(self.status())["latest_block_time"])

    def balances(self, addr, height=0, **kwargs):
        return json.loads(
            self.raw(
                "q",
                "bank",
                "balances",
                addr,
                height=height,
                **(self.get_base_kwargs() | kwargs),
            )
        )["balances"]

    def balance(self, addr, denom=None, height=0):
        denoms = {
            coin["denom"]: int(coin["amount"])
            for coin in self.balances(addr, height=height)
        }
        return denoms.get(denom, 0)

    def query_bank_send(self, *denoms, **kwargs):
        return json.loads(
            self.raw(
                "q",
                "bank",
                "send-enabled",
                *denoms,
                **(self.get_base_kwargs() | kwargs),
            )
        ).get("send_enabled", [])

    def query_bank_denom_metadata(self, denom, **kwargs):
        return json.loads(
            self.raw(
                "q",
                "bank",
                "denom-metadata",
                denom,
                **(self.get_base_kwargs() | kwargs),
            )
        ).get("metadata")

    def query_tx(self, tx_type, tx_value):
        tx = self.raw(
            "query",
            "tx",
            "--type",
            tx_type,
            tx_value,
            home=self.data_dir,
            node=self.node_rpc,
        )
        return json.loads(tx)

    def query_all_txs(self, addr, **kwargs):
        txs = self.raw(
            "q",
            "txs-all",
            addr,
            **(self.get_base_kwargs() | kwargs),
        )
        return json.loads(txs)

    def fund_community_pool(self, amt, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "distribution",
                "fund-community-pool",
                "-y",
                amt,
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def fund_validator_rewards_pool(self, val_addr, amt, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "distribution",
                "fund-validator-rewards-pool",
                "-y",
                val_addr,
                amt,
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def set_withdraw_addr(self, addr, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "distribution",
                "set-withdraw-addr",
                "-y",
                addr,
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def distribution_commission(self, addr, **kwargs):
        res = (
            json.loads(
                self.raw(
                    "q",
                    "distribution",
                    "commission",
                    addr,
                    **(self.get_base_kwargs() | kwargs),
                )
            )
            .get("commission")
            .get("commission")
        )
        if not res or not res[0]:
            return 0
        return parse_amount(res[0])

    def distribution_community_pool(self, **kwargs):
        for module in ["distribution", "protocolpool"]:
            try:
                res = json.loads(
                    self.raw(
                        "query",
                        module,
                        "community-pool",
                        output="json",
                        node=self.node_rpc,
                        **kwargs,
                    )
                )
                return parse_amount(res["pool"][0])
            except Exception as e:
                if (
                    module == "distribution"
                    and "CommunityPool query exposed by the external community pool"
                    in str(e)
                ):
                    continue
                raise

    def distribution_rewards(self, delegator_addr, **kwargs):
        res = json.loads(
            self.raw(
                "q",
                "distribution",
                "rewards",
                delegator_addr,
                **(self.get_base_kwargs() | kwargs),
            )
        )
        total = res.get("total")
        if not total or total[0] is None:
            return 0
        return parse_amount(total[0])

    def address(self, name, bech="acc"):
        output = self.raw(
            "keys",
            "show",
            name,
            "-a",
            home=self.data_dir,
            keyring_backend="test",
            bech=bech,
        )
        return output.strip().decode()

    def account(self, addr, **kwargs):
        return json.loads(
            self.raw("q", "auth", "account", addr, **(self.get_base_kwargs() | kwargs))
        )

    def account_by_num(self, num, **kwargs):
        return json.loads(
            self.raw(
                "q",
                "auth",
                "address-by-acc-num",
                num,
                **(self.get_base_kwargs() | kwargs),
            )
        )

    def create_periodic_vesting_acct(self, to_address, amount, end_time, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "vesting",
                "create-vesting-account",
                to_address,
                amount,
                end_time,
                "-y",
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp["code"] == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def create_periodic_vesting_account(self, to_address, periods, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "vesting",
                "create-periodic-vesting-account",
                to_address,
                periods,
                "-y",
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp["code"] == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def supply(self, supply_type):
        return json.loads(
            self.raw("query", "supply", supply_type, output="json", node=self.node_rpc)
        )

    def validator(self, addr, **kwargs):
        res = json.loads(
            self.raw(
                "q",
                "staking",
                "validator",
                addr,
                **(self.get_base_kwargs() | kwargs),
            )
        )
        return res.get("validator") or res

    def validators(self):
        return json.loads(
            self.raw("q", "staking", "validators", output="json", node=self.node_rpc)
        )["validators"]

    def get_params(self, module, **kwargs):
        default_kwargs = self.get_base_kwargs()
        res = json.loads(self.raw("q", module, "params", **(default_kwargs | kwargs)))
        return res.get("params") or res

    def staking_pool(self, bonded=True, **kwargs):
        res = self.raw("q", "staking", "pool", **(self.get_base_kwargs() | kwargs))
        res = json.loads(res)
        res = res.get("pool") or res
        return int(res["bonded_tokens" if bonded else "not_bonded_tokens"])

    def transfer(
        self,
        from_,
        to,
        coins,
        generate_only=False,
        event_query_tx=True,
        ledger=False,
        fees=None,
        **kwargs,
    ):
        rsp = json.loads(
            self.raw(
                "tx",
                "bank",
                "send",
                from_,
                to,
                coins,
                "-y",
                "--generate-only" if generate_only else None,
                "--ledger" if ledger else None,
                fees=fees,
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if not generate_only and rsp.get("code") == 0 and event_query_tx:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def delegation(self, del_addr, val_addr, **kwargs):
        try:
            res = json.loads(
                self.raw(
                    "q",
                    "staking",
                    "delegation",
                    del_addr,
                    val_addr,
                    **(self.get_base_kwargs() | kwargs),
                )
            )
            return res.get("delegation_response") or res
        except AssertionError as e:
            if "delegation with delegator" in str(e) and "not found" in str(e):
                return {"balance": {"amount": 0}}
            raise

    def delegations(self, del_addr, **kwargs):
        res = json.loads(
            self.raw(
                "q",
                "staking",
                "delegations",
                del_addr,
                **(self.get_base_kwargs() | kwargs),
            )
        )
        return res.get("delegation_responses") or res

    def delegate_amount(self, to_addr, amt, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "staking",
                "delegate",
                to_addr,
                amt,
                "-y",
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def unbond_amount(self, to_addr, amt, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "staking",
                "unbond",
                to_addr,
                amt,
                "-y",
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def redelegate(self, from_addr, to_addr, amt, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "staking",
                "redelegate",
                from_addr,
                to_addr,
                amt,
                "-y",
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    # from_delegator can be account name or address
    def withdraw_all_rewards(self, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "distribution",
                "withdraw-all-rewards",
                "-y",
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def withdraw_rewards(self, val_addr, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "distribution",
                "withdraw-rewards",
                val_addr,
                "-y",
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def withdraw_validator_commission(self, val_addr, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "distribution",
                "withdraw-validator-commission",
                val_addr,
                "-y",
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def make_multisig(self, name, signer1, signer2, **kwargs):
        self.raw(
            "keys",
            "add",
            name,
            multisig=f"{signer1},{signer2}",
            multisig_threshold="2",
            **(self.get_kwargs() | kwargs),
        )

    def sign_multisig_tx(self, tx_file, multi_addr, signer_name, **kwargs):
        return json.loads(
            self.raw(
                "tx",
                "sign",
                tx_file,
                from_=signer_name,
                multisig=multi_addr,
                **(self.get_kwargs() | kwargs),
            )
        )

    def sign_batch_multisig_tx(
        self,
        tx_file,
        multi_addr,
        signer_name,
        account_number,
        sequence_number,
        **kwargs,
    ):
        r = self.raw(
            "tx",
            "sign-batch",
            "--offline",
            tx_file,
            account_number=account_number,
            sequence=sequence_number,
            from_=signer_name,
            multisig=multi_addr,
            home=self.data_dir,
            keyring_backend="test",
            chain_id=self.chain_id,
            node=self.node_rpc,
            **kwargs,
        )
        return r.decode("utf-8")

    def encode_signed_tx(self, signed_tx, **kwargs):
        return self.raw(
            "tx",
            "encode",
            signed_tx,
            **kwargs,
        )

    def sign_single_tx(self, tx_file, signer_name, **kwargs):
        return json.loads(
            self.raw(
                "tx",
                "sign",
                tx_file,
                from_=signer_name,
                home=self.data_dir,
                keyring_backend="test",
                chain_id=self.chain_id,
                node=self.node_rpc,
                **kwargs,
            )
        )

    def combine_multisig_tx(
        self, tx_file, multi_name, signer1_file, signer2_file, **kwargs
    ):
        return json.loads(
            self.raw(
                "tx",
                "multisign",
                tx_file,
                multi_name,
                signer1_file,
                signer2_file,
                **(self.get_kwargs() | kwargs),
            )
        )

    def combine_batch_multisig_tx(
        self, tx_file, multi_name, signer1_file, signer2_file, **kwargs
    ):
        r = self.raw(
            "tx",
            "multisign-batch",
            tx_file,
            multi_name,
            signer1_file,
            signer2_file,
            home=self.data_dir,
            keyring_backend="test",
            chain_id=self.chain_id,
            node=self.node_rpc,
            **kwargs,
        )
        return r.decode("utf-8")

    def broadcast_tx(self, tx_file, **kwargs):
        kwargs.setdefault("broadcast_mode", "sync")
        kwargs.setdefault("output", "json")
        rsp = json.loads(
            self.raw("tx", "broadcast", tx_file, node=self.node_rpc, **kwargs)
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"], **kwargs)
        return rsp

    def broadcast_tx_json(self, tx, **kwargs):
        with tempfile.NamedTemporaryFile("w") as fp:
            json.dump(tx, fp)
            fp.flush()
            return self.broadcast_tx(fp.name, **kwargs)

    def sign_tx(self, tx_file, signer, **kwargs):
        return json.loads(
            self.raw(
                "tx",
                "sign",
                tx_file,
                from_=signer,
                **(self.get_kwargs() | kwargs),
            )
        )

    def sign_tx_json(self, tx, signer, max_priority_price=None, **kwargs):
        if max_priority_price is not None:
            tx["body"]["extension_options"].append(
                {
                    "@type": "/cosmos.evm.ante.v1.ExtensionOptionDynamicFeeTx",
                    "max_priority_price": str(max_priority_price),
                }
            )
        with tempfile.NamedTemporaryFile("w") as fp:
            json.dump(tx, fp)
            fp.flush()
            return self.sign_tx(fp.name, signer, **kwargs)

    def unjail(self, addr, event_query_tx=True, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "slashing",
                "unjail",
                "-y",
                from_=addr,
                home=self.data_dir,
                node=self.node_rpc,
                keyring_backend="test",
                chain_id=self.chain_id,
                **kwargs,
            )
        )
        if rsp["code"] == 0 and event_query_tx:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def create_validator(
        self,
        amount,
        options,
        event_query_tx=True,
        **kwargs,
    ):
        options = {
            "commission-max-change-rate": "0.01",
            "commission-rate": "0.1",
            "commission-max-rate": "0.2",
            "min-self-delegation": "1",
            "amount": amount,
        } | options

        if "pubkey" not in options:
            subcmd = "tendermint" if self.has_tendermint_subcommand else "comet"
            pubkey = (
                self.raw(
                    subcmd,
                    "show-validator",
                    home=self.data_dir,
                )
                .strip()
                .decode()
            )
            options["pubkey"] = json.loads(pubkey)

        with tempfile.NamedTemporaryFile("w") as fp:
            json.dump(options, fp)
            fp.flush()
            raw = self.raw(
                "tx",
                "staking",
                "create-validator",
                fp.name,
                "-y",
                from_=self.address("validator"),
                # basic
                home=self.data_dir,
                node=self.node_rpc,
                keyring_backend="test",
                chain_id=self.chain_id,
                **kwargs,
            )
        rsp = json.loads(raw)
        if rsp["code"] == 0 and event_query_tx:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def create_validator_legacy(
        self,
        amount,
        moniker=None,
        commission_max_change_rate="0.01",
        commission_rate="0.1",
        commission_max_rate="0.2",
        min_self_delegation="1",
        event_query_tx=True,
        **kwargs,
    ):
        """MsgCreateValidator
        create the node with create_node before call this"""
        subcmd = "tendermint" if self.has_tendermint_subcommand else "comet"
        pubkey = (
            self.raw(
                subcmd,
                "show-validator",
                home=self.data_dir,
            )
            .strip()
            .decode()
        )
        options = {
            "amount": amount,
            "min-self-delegation": min_self_delegation,
            "commission-rate": commission_rate,
            "commission-max-rate": commission_max_rate,
            "commission-max-change-rate": commission_max_change_rate,
            "moniker": moniker,
        }
        options["pubkey"] = "'" + pubkey + "'"
        raw = self.raw(
            "tx",
            "staking",
            "create-validator",
            "-y",
            from_=self.address("validator"),
            # basic
            home=self.data_dir,
            node=self.node_rpc,
            keyring_backend="test",
            chain_id=self.chain_id,
            **{k: v for k, v in options.items() if v is not None},
            **kwargs,
        )
        rsp = json.loads(raw)
        if rsp["code"] == 0 and event_query_tx:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def edit_validator(
        self,
        commission_rate=None,
        new_moniker=None,
        identity=None,
        website=None,
        security_contact=None,
        details=None,
        event_query_tx=True,
        **kwargs,
    ):
        """MsgEditValidator"""
        options = dict(
            commission_rate=commission_rate,
            # description
            new_moniker=new_moniker,
            identity=identity,
            website=website,
            security_contact=security_contact,
            details=details,
        )
        rsp = json.loads(
            self.raw(
                "tx",
                "staking",
                "edit-validator",
                "-y",
                from_=self.address("validator"),
                home=self.data_dir,
                node=self.node_rpc,
                keyring_backend="test",
                chain_id=self.chain_id,
                **{k: v for k, v in options.items() if v is not None},
                **kwargs,
            )
        )
        if rsp["code"] == 0 and event_query_tx:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def software_upgrade(self, proposer, proposal, **kwargs):
        default_kwargs = self.get_kwargs()
        rsp = json.loads(
            self.raw(
                "tx",
                "upgrade",
                "software-upgrade",
                proposal["name"],
                "-y",
                "--no-validate",
                from_=proposer,
                # content
                title=proposal.get("title"),
                note=proposal.get("note"),
                upgrade_height=proposal.get("upgrade-height"),
                upgrade_time=proposal.get("upgrade-time"),
                upgrade_info=proposal.get("upgrade-info"),
                summary=proposal.get("summary"),
                deposit=proposal.get("deposit"),
                # basic
                **(default_kwargs | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def gov_vote(self, voter, proposal_id, option, event_query_tx=True, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "gov",
                "vote",
                proposal_id,
                option,
                "-y",
                from_=voter,
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0 and event_query_tx:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def gov_deposit(
        self, depositor, proposal_id, amount, event_query_tx=True, **kwargs
    ):
        rsp = json.loads(
            self.raw(
                "tx",
                "gov",
                "deposit",
                proposal_id,
                amount,
                "-y",
                from_=depositor,
                home=self.data_dir,
                node=self.node_rpc,
                keyring_backend="test",
                chain_id=self.chain_id,
                **kwargs,
            )
        )
        if rsp["code"] == 0 and event_query_tx:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def submit_gov_proposal(self, proposal, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "gov",
                "submit-proposal",
                proposal,
                "-y",
                stderr=subprocess.DEVNULL,
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def query_proposals(self, **kwargs):
        res = json.loads(
            self.raw(
                "q",
                "gov",
                "proposals",
                **(self.get_base_kwargs() | kwargs),
            )
        )
        return res.get("proposals") or res

    def query_proposal(self, proposal_id, **kwargs):
        res = json.loads(
            self.raw(
                "q",
                "gov",
                "proposal",
                proposal_id,
                **(self.get_base_kwargs() | kwargs),
            )
        )
        return res.get("proposal") or res

    def query_tally(self, proposal_id, **kwargs):
        res = json.loads(
            self.raw(
                "q",
                "gov",
                "tally",
                proposal_id,
                **(self.get_base_kwargs() | kwargs),
            )
        )
        return res.get("tally") or res

    def ibc_transfer(self, to, amount, src_channel, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "ibc-transfer",
                "transfer",
                "transfer",
                src_channel,
                to,
                amount,
                "-y",
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def export(self, **kwargs):
        raw = self.raw("export", home=self.data_dir, **kwargs)
        if isinstance(raw, bytes):
            raw = raw.decode()
        # skip client log
        idx = raw.find("{")
        if idx == -1:
            raise ValueError("No JSON object found in export output")
        return json.loads(raw[idx:])

    def unsaferesetall(self):
        return self.raw("unsafe-reset-all")

    def create_nft(
        self,
        from_addr,
        denomid,
        denomname,
        schema,
        fees,
        event_query_tx=True,
        **kwargs,
    ):
        rsp = json.loads(
            self.raw(
                "tx",
                "nft",
                "issue",
                denomid,
                "-y",
                fees=fees,
                name=denomname,
                schema=schema,
                home=self.data_dir,
                from_=from_addr,
                keyring_backend="test",
                chain_id=self.chain_id,
                node=self.node_rpc,
                **kwargs,
            )
        )
        if rsp["code"] == 0 and event_query_tx:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def query_nft(self, denomid):
        return json.loads(
            self.raw(
                "query",
                "nft",
                "denom",
                denomid,
                output="json",
                home=self.data_dir,
                node=self.node_rpc,
            )
        )

    def query_denom_by_name(self, denomname):
        return json.loads(
            self.raw(
                "query",
                "nft",
                "denom-by-name",
                denomname,
                output="json",
                home=self.data_dir,
                node=self.node_rpc,
            )
        )

    def create_nft_token(
        self,
        from_addr,
        to_addr,
        denomid,
        tokenid,
        uri,
        fees,
        event_query_tx=True,
        **kwargs,
    ):
        rsp = json.loads(
            self.raw(
                "tx",
                "nft",
                "mint",
                denomid,
                tokenid,
                "-y",
                uri=uri,
                recipient=to_addr,
                home=self.data_dir,
                from_=from_addr,
                keyring_backend="test",
                chain_id=self.chain_id,
                node=self.node_rpc,
                **kwargs,
            )
        )
        if rsp["code"] == 0 and event_query_tx:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def query_nft_token(self, denomid, tokenid):
        return json.loads(
            self.raw(
                "query",
                "nft",
                "token",
                denomid,
                tokenid,
                output="json",
                home=self.data_dir,
                node=self.node_rpc,
            )
        )

    def burn_nft_token(
        self, from_addr, denomid, tokenid, event_query_tx=True, **kwargs
    ):
        rsp = json.loads(
            self.raw(
                "tx",
                "nft",
                "burn",
                denomid,
                tokenid,
                "-y",
                from_=from_addr,
                keyring_backend="test",
                home=self.data_dir,
                chain_id=self.chain_id,
                node=self.node_rpc,
                **kwargs,
            )
        )
        if rsp["code"] == 0 and event_query_tx:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def edit_nft_token(
        self,
        from_addr,
        denomid,
        tokenid,
        newuri,
        newname,
        event_query_tx=True,
        **kwargs,
    ):
        rsp = json.loads(
            self.raw(
                "tx",
                "nft",
                "edit",
                denomid,
                tokenid,
                "-y",
                from_=from_addr,
                uri=newuri,
                name=newname,
                keyring_backend="test",
                home=self.data_dir,
                chain_id=self.chain_id,
                node=self.node_rpc,
                **kwargs,
            )
        )
        if rsp["code"] == 0 and event_query_tx:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def transfer_nft_token(
        self,
        from_addr,
        to_addr,
        denomid,
        tokenid,
        event_query_tx=True,
        **kwargs,
    ):
        rsp = json.loads(
            self.raw(
                "tx",
                "nft",
                "transfer",
                to_addr,
                denomid,
                tokenid,
                "-y",
                from_=from_addr,
                keyring_backend="test",
                home=self.data_dir,
                chain_id=self.chain_id,
                node=self.node_rpc,
                **kwargs,
            )
        )
        if rsp["code"] == 0 and event_query_tx:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def event_query_tx_for(self, hash, **kwargs):
        return json.loads(
            self.raw(
                "q",
                "event-query-tx-for",
                hash,
                **(self.get_base_kwargs() | kwargs),
            )
        )

    def migrate_keystore(self):
        return self.raw("keys", "migrate", home=self.data_dir)

    def ibc_query_channels(self, connid, **kwargs):
        default_kwargs = {
            "node": self.node_rpc,
            "output": "json",
        }
        return json.loads(
            self.raw(
                "q",
                "ibc",
                "channel",
                "connections",
                connid,
                **(default_kwargs | kwargs),
            )
        )

    def ibc_query_channel(self, port_id, channel_id, **kwargs):
        default_kwargs = {
            "node": self.node_rpc,
            "output": "json",
        }
        return json.loads(
            self.raw(
                "q",
                "ibc",
                "channel",
                "end",
                port_id,
                channel_id,
                **(default_kwargs | kwargs),
            )
        )

    def ica_register_account(self, connid, event_query_tx=True, **kwargs):
        "execute on host chain to attach an account to the connection"
        default_kwargs = {
            "home": self.data_dir,
            "node": self.node_rpc,
            "chain_id": self.chain_id,
            "keyring_backend": "test",
        }
        args = (
            ["icaauth", "register-account"]
            if self.has_icaauth_subcommand
            else ["ica", "controller", "register"]
        )
        rsp = json.loads(
            self.raw(
                "tx",
                *args,
                connid,
                "-y",
                **(default_kwargs | kwargs),
            )
        )
        if rsp["code"] == 0 and event_query_tx:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def ica_query_account(self, connid, owner, **kwargs):
        default_kwargs = {
            "node": self.node_rpc,
            "output": "json",
        }
        args = (
            ["icaauth", "interchain-account-address", connid, owner]
            if self.has_icaauth_subcommand
            else ["ica", "controller", "interchain-account", owner, connid]
        )
        return json.loads(
            self.raw(
                "q",
                *args,
                **(default_kwargs | kwargs),
            )
        )

    def ica_submit_tx(
        self,
        connid,
        tx,
        timeout_duration="1h",
        event_query_tx=True,
        **kwargs,
    ):
        default_kwargs = {
            "home": self.data_dir,
            "node": self.node_rpc,
            "chain_id": self.chain_id,
            "keyring_backend": "test",
        }
        if self.has_icaauth_subcommand:
            args = ["icaauth", "submit-tx"]
        else:
            args = ["ica", "controller", "send-tx"]

        duration_args = []
        if timeout_duration:
            if self.has_icaauth_subcommand:
                duration_args = ["--timeout-duration", timeout_duration]
            else:
                timeout = int(durations.Duration(timeout_duration).to_seconds() * 1e9)
                duration_args = ["--relative-packet-timeout", timeout]

        rsp = json.loads(
            self.raw(
                "tx",
                *args,
                connid,
                tx,
                *duration_args,
                "-y",
                **(default_kwargs | kwargs),
            )
        )
        if rsp["code"] == 0 and event_query_tx:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def ica_generate_packet_data(self, tx, memo=None, encoding="proto3", **kwargs):
        return json.loads(
            self.raw(
                "tx",
                "interchain-accounts",
                "host",
                "generate-packet-data",
                tx,
                memo=memo,
                encoding=encoding,
                home=self.data_dir,
                **kwargs,
            )
        )

    def ibc_upgrade_channels(self, version, from_addr, **kwargs):
        return json.loads(
            self.raw(
                "tx",
                "ibc",
                "channel",
                "upgrade-channels",
                json.dumps(version),
                "-y",
                "--json",
                from_=from_addr,
                keyring_backend="test",
                chain_id=self.chain_id,
                home=self.data_dir,
                stderr=subprocess.DEVNULL,
                **kwargs,
            )
        )

    def register_counterparty_payee(
        self, port_id, channel_id, relayer, counterparty_payee, **kwargs
    ):
        rsp = json.loads(
            self.raw(
                "tx",
                "ibc-fee",
                "register-counterparty-payee",
                port_id,
                channel_id,
                relayer,
                counterparty_payee,
                "-y",
                home=self.data_dir,
                **kwargs,
            )
        )
        if rsp["code"] == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def pay_packet_fee(self, port_id, channel_id, packet_seq, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "ibc-fee",
                "pay-packet-fee",
                port_id,
                channel_id,
                str(packet_seq),
                "-y",
                home=self.data_dir,
                **kwargs,
            )
        )
        if rsp["code"] == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def ibc_denom(self, denom_hash, **kwargs):
        return json.loads(
            self.raw(
                "q",
                "ibc-transfer",
                "denom",
                denom_hash,
                **(self.get_base_kwargs() | kwargs),
            )
        ).get("denom")

    def ibc_denom_hash(self, path, **kwargs):
        return json.loads(
            self.raw(
                "q",
                "ibc-transfer",
                "denom-hash",
                path,
                **(self.get_base_kwargs() | kwargs),
            )
        ).get("hash")

    def comet_validator_set(self, height, **kwargs):
        return json.loads(
            self.raw(
                "q",
                "comet-validator-set",
                height,
                **(self.get_base_kwargs() | kwargs),
            )
        )

    def query_grant(self, granter, grantee, **kwargs):
        res = json.loads(
            self.raw(
                "q",
                "feegrant",
                "grant",
                granter,
                grantee,
                **(self.get_base_kwargs() | kwargs),
            )
        )
        return res.get("allowance") or res

    def grant_fee_allowance(self, granter, grantee, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "feegrant",
                "grant",
                granter,
                grantee,
                "-y",
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def revoke_fee_grant(self, granter, grantee, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "feegrant",
                "revoke",
                granter,
                grantee,
                "-y",
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def tx_search(self, events: str, **kwargs):
        return json.loads(
            self.raw(
                "q", "txs", query=f'"{events}"', **(self.get_base_kwargs() | kwargs)
            )
        )

    def query_erc20_token_pair(self, token, **kwargs):
        return json.loads(
            self.raw(
                "q",
                "erc20",
                "token-pair",
                token,
                **(self.get_base_kwargs() | kwargs),
            )
        ).get("token_pair", {})

    def query_erc20_token_pairs(self, **kwargs):
        return json.loads(
            self.raw(
                "q",
                "erc20",
                "token-pairs",
                **(self.get_base_kwargs() | kwargs),
            )
        ).get("token_pairs", [])

    def convert_erc20(self, contract, amt, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "erc20",
                "convert-erc20",
                contract,
                amt,
                "-y",
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def register_erc20(self, contract, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "erc20",
                "register-erc20",
                contract,
                "-y",
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def rollback(self):
        self.raw("rollback", home=self.data_dir)

    def prune(self, kind="everything"):
        return self.raw("prune", kind, home=self.data_dir).decode()

    def grant_authorization(self, grantee, authz_type, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "authz",
                "grant",
                grantee,
                authz_type,
                "-y",
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def exec_tx_by_grantee(self, tx_file, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "authz",
                "exec",
                tx_file,
                "-y",
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def revoke_authorization(self, grantee, msg_type, **kwargs):
        rsp = json.loads(
            self.raw(
                "tx",
                "authz",
                "revoke",
                grantee,
                msg_type,
                "-y",
                **(self.get_kwargs_with_gas() | kwargs),
            )
        )
        if rsp.get("code") == 0:
            rsp = self.event_query_tx_for(rsp["txhash"])
        return rsp

    def query_grants(self, granter, grantee, **kwargs):
        return json.loads(
            self.raw(
                "q",
                "authz",
                "grants",
                granter,
                grantee,
                **(self.get_base_kwargs() | kwargs),
            )
        ).get("grants", [])

    def query_base_fee(self, **kwargs):
        return json.loads(
            self.raw(
                "q",
                "feemarket",
                "base-fee",
                **(self.get_base_kwargs() | kwargs),
            )
        )["base_fee"]

    def build_evm_tx(self, raw_tx: str, **kwargs):
        return json.loads(
            self.raw(
                "tx",
                "evm",
                "raw",
                raw_tx,
                "-y",
                "--generate-only",
                **(self.get_kwargs() | kwargs),
            )
        )

    def consensus_address(self):
        output = self.raw("comet", "show-address", home=self.data_dir)
        return output.decode().strip()

import argparse

from lume_pva.runner import PutMode


def add_common_test_args(parser: argparse.ArgumentParser):
    parser.add_argument("-v", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--put-mode",
        dest="put_mode",
        type=str,
        choices=list(PutMode.__members__.values()),
        default=PutMode.Immediate.value,
        help="Put mode to use. 'immediate' acks put requests immediately and 'complete' acks them only after the model has finished simulating.",
    )
    parser.add_argument(
        "--pv-prefix",
        dest="pv_prefix",
        default="example:",
        type=str,
        help="Add this prefix to all PVs added by the model",
    )
    parser.add_argument(
        "--pv-server-protocol",
        dest="pv_server_protocol",
        nargs="+",
        choices=["ca", "pva"],
        default=["ca", "pva"],
        help=(
            "Which protocol(s) the Runner's SERVER uses to SERVE output PVs. "
            "'pva' = PVA only (p4p SharedPVs, no pcaspy CA server). "
            "'ca pva' = both CA and PVA servers (default, uses pcaspy + p4p). "
            "This ONLY affects the serving side — which listeners the server creates. "
            "The Runner's client side (subscribing to remote PVs via pvua) is unaffected; "
            "pvua always auto-detects the remote server's protocol (CA or PVA)."
        ),
    )

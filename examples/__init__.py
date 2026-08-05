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

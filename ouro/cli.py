"""``ouro`` command line: follow route actions from a shell.

``ouro action wait <id>`` blocks until an action finishes and exits with its
outcome, so a script (or an agent running it as a background task) is told the
moment a long route completes instead of polling for it. ``ouro action cancel
<id>`` stops a run on a route that supports it.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, List, Optional

from ouro._exceptions import OuroError
from ouro.models import Action

__all__ = ["main"]

EXIT_OK = 0
EXIT_FAILED = 1  # the action ended in error / timed-out
EXIT_USAGE = 2  # bad arguments, auth, or API failure (incl. a refused cancel)
EXIT_CANCELLED = 3  # the action was cancelled: final, but no result
EXIT_WAIT_TIMEOUT = 124  # --timeout passed; the action is still running

RUNNING_STATUSES = ["queued", "in-progress"]

TIMED_OUT_NOTE = (
    "Ouro gave up on this run after the service went silent. It did not "
    "succeed; nothing was recorded or charged."
)

CANCELLED_NOTE = (
    "Cancelled on request. A runtime-priced run is billed for the seconds it "
    "ran up to the cancel; anything else is free."
)

CANCELLING_NOTE = "Cancel requested; the service is stopping the run."


def _duration(action: Action) -> Optional[str]:
    start = action.started_at or action.created_at
    if not start or not action.finished_at:
        return None
    seconds = int((action.finished_at - start).total_seconds())
    minutes, seconds = divmod(max(seconds, 0), 60)
    return f"{minutes}m{seconds:02d}s" if minutes else f"{seconds}s"


def _error_message(response: Any) -> Optional[str]:
    if not isinstance(response, dict):
        return None
    error = response.get("error") if isinstance(response.get("error"), dict) else response
    message = error.get("message") or error.get("detail")
    return str(message) if message else None


def _outputs(action: Action) -> List[dict]:
    rows = []
    for output in action.output_assets or []:
        asset = output.get("asset") or {}
        rows.append(
            {
                "name": output.get("name"),
                "asset_type": asset.get("asset_type") or output.get("asset_type"),
                "id": asset.get("id") or output.get("asset_id"),
                "asset_name": asset.get("name"),
            }
        )
    if not rows and action.output_asset:
        rows.append(
            {
                "name": None,
                "asset_type": action.output_asset.asset_type,
                "id": str(action.output_asset.id),
                "asset_name": action.output_asset.name,
            }
        )
    return rows


def _note(action: Action) -> Optional[str]:
    if action.is_timed_out:
        return TIMED_OUT_NOTE
    if action.is_cancelled:
        return CANCELLED_NOTE
    if action.is_cancelling:
        return CANCELLING_NOTE
    return None


def _summary(action: Action) -> dict:
    summary = {
        "action_id": str(action.id),
        "status": action.status,
        "route_id": str(action.route_id),
        "route_name": (action.route or {}).get("name"),
        "created_at": action.created_at.isoformat() if action.created_at else None,
        "duration": _duration(action),
        "outputs": _outputs(action),
        "error": (
            _error_message(action.response)
            if not (action.is_success or action.is_cancelled)
            else None
        ),
        "cancel_requested_at": (
            action.cancel_requested_at.isoformat()
            if action.cancel_requested_at
            else None
        ),
        "note": _note(action),
    }
    return {k: v for k, v in summary.items() if v not in (None, [])}


def _line(summary: dict) -> str:
    parts = [
        summary["status"],
        summary.get("route_name") or summary["route_id"],
        summary["action_id"],
        summary.get("duration") or summary.get("created_at"),
    ]
    return "  ".join(str(part) for part in parts if part)


def _print_action(action: Action, as_json: bool) -> None:
    summary = _summary(action)
    if as_json:
        print(json.dumps(summary))
        return
    print(_line(summary))
    for output in summary.get("outputs", []):
        print(
            "  output: "
            + "  ".join(
                str(output[key])
                for key in ("name", "asset_type", "id", "asset_name")
                if output.get(key)
            )
        )
    if summary.get("error"):
        print(f"  error: {summary['error']}")
    if summary.get("note"):
        print(f"  note: {summary['note']}")


def _exit_code(action: Action) -> int:
    if action.is_cancelled:
        return EXIT_CANCELLED
    return EXIT_OK if action.is_success or action.is_pending else EXIT_FAILED


def _cmd_wait(ouro: Any, args: argparse.Namespace) -> int:
    try:
        action = ouro.routes.poll_action(
            args.action_id,
            poll_interval=args.interval,
            timeout=args.timeout,
            raise_on_error=False,
        )
    except TimeoutError:
        _print_action(ouro.routes.retrieve_action(args.action_id), args.json)
        print(
            f"Stopped waiting after {args.timeout:g}s. The action is still running, "
            "not finished; run the same command to keep waiting.",
            file=sys.stderr,
        )
        return EXIT_WAIT_TIMEOUT
    _print_action(action, args.json)
    return _exit_code(action)


def _cmd_get(ouro: Any, args: argparse.Namespace) -> int:
    action = ouro.routes.retrieve_action(args.action_id)
    _print_action(action, args.json)
    return _exit_code(action)


def _cmd_cancel(ouro: Any, args: argparse.Namespace) -> int:
    result = ouro.routes.cancel_action(args.action_id)
    summary = {
        "action_id": str(result.action_id),
        "status": result.status,
        "signal_delivered": result.signal_delivered,
        "cancel_requested_at": (
            result.cancel_requested_at.isoformat()
            if result.cancel_requested_at
            else None
        ),
    }
    if args.json:
        print(json.dumps(summary))
    else:
        print(f"{result.status}  {summary['action_id']}")
        if result.status == "cancelling":
            print(
                "  note: the service is stopping the run; "
                f"`ouro action wait {summary['action_id']}` follows it"
            )
            if not result.signal_delivered:
                print(
                    "  note: the service didn't confirm the signal; Ouro "
                    "finishes the run itself after a grace period"
                )
    return EXIT_OK


def _cmd_list(ouro: Any, args: argparse.Namespace) -> int:
    status = RUNNING_STATUSES if args.running else args.status
    page = ouro.routes.list_my_actions(
        status=status, since=args.since, limit=args.limit, offset=args.offset
    )
    summaries = [_summary(action) for action in page]
    if args.json:
        print(json.dumps({"actions": summaries, "has_more": page.has_more}))
        return EXIT_OK
    if not summaries:
        print("No actions.")
    for summary in summaries:
        print(_line(summary))
    if page.has_more:
        print(f"More available: --offset {args.offset + len(summaries)}")
    return EXIT_OK


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ouro",
        description="Ouro command line. Authenticates with OURO_API_KEY.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    action = commands.add_parser("action", help="Follow route actions (runs)")
    sub = action.add_subparsers(dest="action_command", required=True)

    wait = sub.add_parser(
        "wait",
        help="Block until an action finishes",
        description=(
            "Block until the action finishes, then print its outcome. Exit "
            "status: 0 success, 1 failed or timed out, 3 cancelled, 124 still "
            "running after --timeout."
        ),
    )
    wait.add_argument("action_id")
    wait.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Give up after this many seconds (default: wait until it finishes)",
    )
    wait.add_argument(
        "--interval",
        type=float,
        default=10.0,
        help="Seconds between status checks (default: 10)",
    )
    wait.set_defaults(func=_cmd_wait)

    get = sub.add_parser("get", help="Show an action's current status")
    get.add_argument("action_id")
    get.set_defaults(func=_cmd_get)

    cancel = sub.add_parser(
        "cancel",
        help="Cancel a running action",
        description=(
            "Ask the route's service to stop a run you started. Works on "
            "routes that support cancelling. A runtime-priced run is billed "
            "for the seconds up to the cancel; anything else is free."
        ),
    )
    cancel.add_argument("action_id")
    cancel.set_defaults(func=_cmd_cancel)

    list_ = sub.add_parser("list", help="List your actions across every route")
    list_.add_argument(
        "--running", action="store_true", help="Only queued and in-progress actions"
    )
    list_.add_argument(
        "--status",
        help='Comma-separated statuses: "queued,in-progress,success,error,timed-out,cancelled"',
    )
    list_.add_argument("--since", help="Only actions created at or after this ISO time")
    list_.add_argument("--limit", type=int, default=20)
    list_.add_argument("--offset", type=int, default=0)
    list_.set_defaults(func=_cmd_list)

    for command in (wait, get, cancel, list_):
        command.add_argument("--json", action="store_true", help="Print JSON")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = _parser().parse_args(argv)
    try:
        from ouro import Ouro

        return args.func(Ouro(client="ouro-cli"), args)
    except OuroError as exc:
        print(f"ouro: {exc}", file=sys.stderr)
        return EXIT_USAGE


if __name__ == "__main__":
    sys.exit(main())

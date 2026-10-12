"""ZEUS microstructure collector (two-file GitHub setup, public Binance USD-M feeds).

Only three extra feeds; no duplication of TraderSpy indicators, funding, or screening.

CVD: Binance aggTrades, using `m == true` => buyer was maker => taker SELL.
     Incremental ID-based backfill with bounded pagination and honest lag reporting.
Depth: REST orderbook snapshots; historical snapshots are NOT full depth update history.
Liquidations: publicly published forceOrder snapshots over a short live capture window;
              NOT an exhaustive liquidation tape and NOT a liquidation heatmap.

Unverified live transport/collection is never labelled complete.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

BASE = "https://fapi.binance.com"
STREAM = "wss://fstream.binance.com/ws/!forceOrder@arr"
ROOT = Path(__file__).resolve().parent
HISTORY = ROOT / "data" / "history"
LOG = logging.getLogger("zeus")
LOOKBACK_MS = 48 * 3600 * 1000
MAX_PAGES = int(os.environ.get("ZEUS_MAX_PAGES", "35"))
REST_TIMEOUT = 18


def utc_iso(ms: int | None = None) -> str:
    stamp = (ms if ms is not None else int(time.time() * 1000)) / 1000
    return datetime.fromtimestamp(stamp, timezone.utc).isoformat().replace("+00:00", "Z")


def load_json(path: Path, fallback: Any) -> Any:
    if not path.is_file():
        return fallback
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        handle.write("\n")
    tmp.replace(path)


def read_symbols() -> list[str]:
    """Contracts are curated by ZEUS/TraderSpy, not discovered by this collector.

    Configure ZEUS_SYMBOLS in the GitHub workflow, comma-separated. This avoids
    a third config file and keeps screening/tokenomics with existing plugins.
    """
    raw = os.environ.get(
        "ZEUS_SYMBOLS", "BTCUSDT,ICPUSDT,FETUSDT,RAYSOLUSDT"
    )
    symbols = [symbol.strip().upper() for symbol in raw.split(",") if symbol.strip()]
    if not 1 <= len(symbols) <= 15:
        raise ValueError("Configure 1-15 vetted Binance USD-M perpetual symbols")
    for symbol in symbols:
        if not re.fullmatch(r"[A-Z0-9]{3,28}", symbol):
            raise ValueError(f"Invalid Binance contract symbol {symbol!r}")
    return list(dict.fromkeys(symbols))


def api(path: str, params: dict[str, Any]) -> Any:
    url = BASE + path + "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers={"User-Agent": "zeus-free-market-data/1.0", "Accept": "application/json"})
    last_error: Exception | None = None
    for n in range(3):
        try:
            with urllib.request.urlopen(request, timeout=REST_TIMEOUT) as resp:
                return json.load(resp)
        except (urllib.error.URLError, TimeoutError, ValueError) as e:
            last_error = e
            if n < 2:
                time.sleep(2 * (n + 1))
    raise RuntimeError(f"Binance REST {path} unavailable: {last_error}")


def aggregate_signed(bar: dict[str, Any], trade: dict[str, Any]) -> None:
    """Never infer signed volume from OHLCV. Uses Binance buyer-is-maker flag."""
    price = Decimal(str(trade["p"]))
    base = Decimal(str(trade["q"]))
    if base < 0 or price <= 0:
        raise ValueError("Invalid exchange trade price/quantity")
    quote = base * price
    side = "sell" if trade["m"] else "buy"
    for suffix, amount in (("base", base), ("quote", quote)):
        key = f"taker_{side}_{suffix}"
        bar[key] = str(Decimal(bar.get(key, "0")) + amount)
    bar["aggregate_trade_events"] = bar.get("aggregate_trade_events", 0) + 1


def build_cvd(minutes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cvd_base = Decimal("0")
    cvd_quote = Decimal("0")
    output = []
    for bar in sorted(minutes, key=lambda b: b["minute_ms"]):
        signed_base = Decimal(bar.get("taker_buy_base", "0")) - Decimal(bar.get("taker_sell_base", "0"))
        signed_quote = Decimal(bar.get("taker_buy_quote", "0")) - Decimal(bar.get("taker_sell_quote", "0"))
        cvd_base += signed_base
        cvd_quote += signed_quote
        output.append({**bar, "delta_base": str(signed_base), "delta_quote": str(signed_quote), "cvd_base": str(cvd_base), "cvd_quote": str(cvd_quote)})
    return output


def get_aggregate_trades(symbol: str, last_id: int | None, at_ms: int, max_pages: int = MAX_PAGES) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Collect trades from inclusive cursor or initial ~15 min. Never skip an unprocessed ID."""
    params: dict[str, Any] = {"symbol": symbol, "limit": 1000}
    if last_id is None:
        params["startTime"] = at_ms - 15 * 60_000
    else:
        params["fromId"] = last_id + 1
    trades: list[dict[str, Any]] = []
    seen_last = last_id
    complete_to_now = False
    fetched_pages = 0
    broken_sequence = False
    for page in range(max_pages):
        chunk = api("/fapi/v1/aggTrades", params)
        fetched_pages += 1
        if not isinstance(chunk, list):
            raise ValueError("Unexpected Binance aggTrades response")
        if not chunk:
            complete_to_now = True
            break
        for trade in chunk:
            t_id = int(trade["a"])
            if seen_last is not None and t_id <= seen_last:
                continue
            if seen_last is not None and t_id > seen_last + 1:
                broken_sequence = True
            # In a fast-moving market, data can arrive during the fetch. This is okay.
            trades.append(trade)
            seen_last = t_id
        if len(chunk) < 1000:
            complete_to_now = True
            break
        if seen_last is None:
            break
        params = {"symbol": symbol, "limit": 1000, "fromId": seen_last + 1}
    last_trade_at = int(trades[-1]["T"]) if trades else None
    coverage = {
        "complete_to_fetch_head": bool(complete_to_now and not broken_sequence),
        "id_sequence_gap": broken_sequence,
        "page_cap_reached": not complete_to_now,
        "pages": fetched_pages,
        "trades": len(trades),
        "last_aggregate_id": seen_last,
        "last_trade_at": utc_iso(last_trade_at) if last_trade_at is not None else None,
        "lag_seconds_at_fetch": round(max(0, at_ms - last_trade_at) / 1000, 1) if last_trade_at else None,
    }
    return trades, coverage


def store_cvd(symbol: str, at_ms: int) -> dict[str, Any]:
    path = HISTORY / f"{symbol}.json"
    state = load_json(path, {"symbol": symbol, "source": "binance_usdm_aggTrades", "last_aggregate_id": None, "bars": []})
    trades, coverage = get_aggregate_trades(symbol, state.get("last_aggregate_id"), at_ms)
    by_minute = {int(x["minute_ms"]): {k: v for k, v in x.items() if k not in ("delta_base", "delta_quote", "cvd_base", "cvd_quote")} for x in state["bars"]}
    for trade in trades:
        minute = (int(trade["T"]) // 60_000) * 60_000
        bar = by_minute.setdefault(minute, {"minute_ms": minute, "aggregate_trade_events": 0})
        aggregate_signed(bar, trade)
    window_start = at_ms - LOOKBACK_MS
    final = build_cvd([v for k, v in by_minute.items() if k >= window_start])
    # A missing minute is not a zero delta; do not fabricate unknown volume.
    state.update({"bars": final, "last_aggregate_id": coverage["last_aggregate_id"], "updated_at": utc_iso(at_ms), "coverage": coverage, "cvd_reset_at": utc_iso(final[0]["minute_ms"]) if final else None})
    write_json(path, state)
    return {"coverage": coverage, "bars_1m_recent": final[-60:], "cvd_reset_at": state["cvd_reset_at"], "history_path": f"data/history/{symbol}.json"}


def store_orderbook(symbol: str, at_ms: int) -> dict[str, Any]:
    path = HISTORY / f"{symbol}_depth.json"
    depth = api("/fapi/v1/depth", {"symbol": symbol, "limit": 50})
    bids, asks = depth.get("bids"), depth.get("asks")
    if not isinstance(bids, list) or not isinstance(asks, list) or not bids or not asks:
        raise ValueError("Depth snapshot missing bids or asks")
    item = {"observed_at": utc_iso(at_ms), "observed_at_ms": at_ms, "exchange_event_ms": depth.get("E"), "exchange_transaction_ms": depth.get("T"), "last_update_id": depth.get("lastUpdateId"), "bids": bids, "asks": asks}
    history = load_json(path, {"symbol": symbol, "source": "binance_usdm_depth_snapshot", "snapshots": []})
    history["snapshots"] = [a for a in history.get("snapshots", []) if a.get("observed_at_ms", 0) >= at_ms - LOOKBACK_MS][-575:] + [item]
    history["sampling_note"] = "Snapshots only; order additions/cancellations between snapshots are NOT observed. Not a continuous orderbook heatmap."
    write_json(path, history)
    return {"latest": item, "samples_48h": len(history["snapshots"]), "history_path": f"data/history/{symbol}_depth.json", "sampling": "periodic_rest_snapshot_not_continuous_depth"}


def parse_force_order(payload: Any, tracked: set[str]) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and "data" in payload:
        payload = payload["data"]
    events = payload if isinstance(payload, list) else [payload]
    result = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        order = ev.get("o", {})
        symbol = order.get("s") or ev.get("s")
        if symbol not in tracked:
            continue
        # Binance emits at most one liquidation snapshot per symbol per 1s.
        result.append({"symbol": symbol, "event_time_ms": ev.get("E"), "trade_time_ms": order.get("T"), "side": order.get("S"), "price": order.get("ap") or order.get("p"), "quantity": order.get("z") or order.get("q"), "status": order.get("X"), "type": "observed_liquidation_snapshot_not_exhaustive"})
    return result


def capture_liquidation_window(symbols: list[str], seconds: int = 30) -> dict[str, Any]:
    import websocket  # github runner: pip install websocket-client==1.8.0
    path = ROOT / "data" / "liquidations.json"
    tracked = set(symbols)
    start = int(time.time() * 1000)
    events: list[dict[str, Any]] = []
    error = None
    end = start
    ws = None
    try:
        ws = websocket.create_connection(STREAM, timeout=7)
        stop = time.monotonic() + seconds
        ws.settimeout(1.5)
        while time.monotonic() < stop:
            try:
                msg = ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            try:
                events.extend(parse_force_order(json.loads(msg), tracked))
            except (ValueError, TypeError):
                LOG.warning("Ignoring undecodable liquidation payload")
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        LOG.error("Liquidation stream unavailable: %s", error)
    finally:
        end = int(time.time() * 1000)
        if ws is not None:
            ws.close()
    state = load_json(path, {"source": "binance_usdm_all_force_orders_snapshots", "events": [], "coverage_windows": []})
    state["events"] = sorted([ev for ev in state.get("events", []) if ev.get("event_time_ms") is not None and ev["event_time_ms"] >= end - LOOKBACK_MS] + events, key=lambda ev: ev["event_time_ms"] or 0)[-4000:]
    coverage = {"start_ms": start, "end_ms": end, "ok": error is None, "error": error, "events": len(events)}
    state["coverage_windows"] = [w for w in state.get("coverage_windows", []) if w.get("end_ms", 0) >= end - LOOKBACK_MS][-600:] + [coverage]
    state["limitations"] = "Only publicly published 1s liquidation snapshots during observed windows. Missed periods are unknown; this is NOT predicted liquidation clusters or a liquidation heatmap."
    write_json(path, state)
    return {"capture": coverage, "recent_events": state["events"][-25:], "history_path": "data/liquidations.json", "complete": False}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    symbols = read_symbols()
    at_ms = int(time.time() * 1000)
    output: dict[str, Any] = {"generated_at": utc_iso(at_ms), "source": "binance_usdm_public_market_data", "symbols": {}, "liquidations": {}, "health": {"errors": []}, "data_is_live_verified_in_this_run": False}
    any_ok = False
    for symbol in symbols:
        symbol_data: dict[str, Any] = {}
        try:
            symbol_data["cvd"] = store_cvd(symbol, int(time.time() * 1000))
            any_ok = True
        except Exception as exc:
            output["health"]["errors"].append(f"{symbol} CVD: {type(exc).__name__}: {exc}")
        try:
            symbol_data["depth"] = store_orderbook(symbol, int(time.time() * 1000))
            any_ok = True
        except Exception as exc:
            output["health"]["errors"].append(f"{symbol} depth: {type(exc).__name__}: {exc}")
        output["symbols"][symbol] = symbol_data
    try:
        output["liquidations"] = capture_liquidation_window(symbols, int(os.environ.get("ZEUS_LIQUIDATION_CAPTURE_SECONDS", "30")))
    except Exception as exc:
        output["health"]["errors"].append(f"Liquidation capture: {type(exc).__name__}: {exc}")
    output["data_is_live_verified_in_this_run"] = any_ok
    output["health"]["successful_symbol_feeds"] = sum(len(v) for v in output["symbols"].values())
    write_json(ROOT / "data" / "latest.json", output)
    LOG.info("Finished: %s feeds, %s errors", output["health"]["successful_symbol_feeds"], len(output["health"]["errors"]))
    if not any_ok:
        raise SystemExit("No market feed returned usable data; see data/latest.json health errors")


if __name__ == "__main__":
    main()

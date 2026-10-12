"""ZEUS extra microstructure collector: public Gate.io USDT futures.

Standalone replacement for the previous Binance-based collector.py. Uses existing
GitHub collect.yml, with no keys, new config, or added Python dependencies.

CVD: exchange market trades, signed `size` * `quanto_multiplier`, deduplicated
     by exchange trade id. True taker-side data, NOT candle-inferred volume.
Depth: periodic public REST snapshots only, not continuous heatmap history.
Liquidations: public WebSocket reports during explicitly recorded sampling windows;
              not full-time coverage and not a modeled liquidation heatmap.

The data refers to GATE futures, which can differ from prices/flows at Kraken.
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
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
HISTORY = ROOT / "data" / "history"
LOG = logging.getLogger("zeus-gate")
REST_BASES = ["https://api.gateio.ws/api/v4", "https://fx-api.gateio.ws/api/v4"]
WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
WINDOW_MS = 48 * 60 * 60 * 1000
API_TIMEOUT = 9
TRADE_PAGE_LIMIT = 1000
MAX_PAGES = max(2, int(os.environ.get("ZEUS_MAX_PAGES", "35")))


def utc(ms: int | None = None) -> str:
    return datetime.fromtimestamp((ms if ms is not None else int(time.time() * 1000)) / 1000, timezone.utc).isoformat().replace("+00:00", "Z")


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(exist_ok=True, parents=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def symbols() -> list[tuple[str, str]]:
    raw = os.environ.get("ZEUS_SYMBOLS", "BTCUSDT,ICPUSDT,FETUSDT,RAYSOLUSDT")
    out: list[tuple[str, str]] = []
    for v in raw.split(","):
        sym = v.strip().upper()
        if not sym:
            continue
        if not re.fullmatch(r"[A-Z0-9]{3,28}", sym):
            raise ValueError("Invalid contract symbol " + sym)
        base = sym[:-4] if sym.endswith("USDT") else sym
        if sym == "RAYSOLUSDT":
            base = "RAY"  # Binance's RAYSOL symbol refers to Raydium on other exchanges.
        gate = base + "_USDT"
        out.append((sym, gate))
    return list(dict(out).items())


def api(path: str, params: dict[str, Any] | None = None) -> Any:
    query = ("?" + urllib.parse.urlencode(params)) if params else ""
    errors: list[str] = []
    for base in REST_BASES:
        url = base + path + query
        req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "ZEUS-Microstructure/2.0"})
        try:
            with urllib.request.urlopen(req, timeout=API_TIMEOUT) as res:
                obj = json.load(res)
                if isinstance(obj, dict) and obj.get("label") and obj.get("message"):
                    raise ValueError(f"Gate API response: {obj['label']}: {obj['message']}")
                return obj
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, ValueError) as exc:
            errors.append(f"{base}: {type(exc).__name__} {str(exc)[:140]}")
    raise RuntimeError("Gate REST unavailable: " + " | ".join(errors))


def contract_info(contract: str) -> dict:
    info = api("/futures/usdt/contracts/" + urllib.parse.quote(contract, safe=""))
    if not isinstance(info, dict) or str(info.get("name", "")) != contract:
        raise ValueError("Invalid or unsupported Gate USDT contract " + contract)
    if info.get("status") and info["status"] != "trading":
        raise ValueError(contract + " has status " + str(info["status"]))
    mult = Decimal(str(info.get("quanto_multiplier", "0")))
    if mult <= 0:
        raise ValueError("Invalid contract multiplier for " + contract)
    return {"multiplier": mult, "name": contract, "status": info.get("status")}


def trade_timestamp_ms(t: dict) -> int:
    if t.get("create_time_ms") is not None:
        v = Decimal(str(t["create_time_ms"]))
        return int(v if v > 10**12 else v * 1000)
    return int(Decimal(str(t["create_time"])) * 1000)


def trade_to_delta(t: dict, multiplier: Decimal) -> tuple[int, Decimal, Decimal]:
    """Gate Futures `size`: positive = taker buy, negative = taker sell."""
    signed_contracts = Decimal(str(t["size"]))
    price = Decimal(str(t["price"]))
    if price <= 0:
        raise ValueError("invalid trade price")
    base_delta = signed_contracts * multiplier
    return trade_timestamp_ms(t), base_delta, base_delta * price


def get_trades(contract: str, start_ms: int, end_ms: int) -> tuple[list[dict], dict]:
    """Public Gate futures trades endpoint, offset-paginated within a time range."""
    result: list[dict] = []
    offset = 0
    pages = 0
    exhausted = False
    while pages < MAX_PAGES:
        obj = api("/futures/usdt/trades", {
            "contract": contract, "from": max(0, start_ms // 1000 - 1),
            "to": end_ms // 1000 + 1, "limit": TRADE_PAGE_LIMIT, "offset": offset,
        })
        if not isinstance(obj, list):
            raise ValueError("Unexpected trades shape")
        pages += 1
        result.extend(obj)
        if len(obj) < TRADE_PAGE_LIMIT:
            exhausted = True
            break
        offset += len(obj)
    # Pagination must be complete: otherwise CVD derived from this partial window
    # would be dangerously misleading.
    return result, {"pages": pages, "page_cap_reached": not exhausted, "records": len(result), "complete_pagination": exhausted}


def cvd_bars(trades: list[dict], multiplier: Decimal, cutoff_ms: int) -> list[dict]:
    buckets: dict[int, dict[str, Any]] = {}
    for t in trades:
        at, db, dq = trade_to_delta(t, multiplier)
        minute = at // 60000 * 60000
        if minute < cutoff_ms:
            continue
        b = buckets.setdefault(minute, {"minute_ms": minute, "delta_base": Decimal(0), "delta_quote": Decimal(0), "trades": 0})
        b["delta_base"] += db
        b["delta_quote"] += dq
        b["trades"] += 1
    return [
        {"minute_ms": b["minute_ms"], "delta_base": str(b["delta_base"]),
         "delta_quote": str(b["delta_quote"]), "trade_events": b["trades"]}
        for b in sorted(buckets.values(), key=lambda x: x["minute_ms"])
    ]


def collect_cvd(symbol: str, gate: str, info: dict, at: int) -> dict:
    path = HISTORY / (symbol + ".json")
    default = {"symbol": symbol, "exchange_symbol": gate, "source": "gate_futures_market_trades", "bars": [], "recent_trade_ids": [], "coverage": {}}
    state = read_json(path, default)
    cutoff = (at - WINDOW_MS) // 60000 * 60000
    prior = state.get("bars", [])
    # Fetch with 10s overlap; robust against inclusive timestamps, using exact trade IDs.
    last_observed = int(state.get("last_trade_ms", 0))
    from_ms = (max(last_observed - 10_000, at - 6 * 60_000) if last_observed else at - 6 * 60_000)
    raw, pagination = get_trades(gate, from_ms, at)
    last_ids = set(str(x) for x in state.get("recent_trade_ids", []))
    fresh: list[dict] = []
    unique_ids = set()
    for t in raw:
        tid = str(t.get("id", ""))
        if not tid:
            raise ValueError("Trade ID absent: cannot deduplicate")
        if tid in unique_ids or tid in last_ids:
            continue
        unique_ids.add(tid)
        fresh.append(t)
    # Refuse to publish a partial CVD reconstruction when API page caps are reached.
    if not pagination["complete_pagination"]:
        raise RuntimeError(f"Trade pagination capped for {gate}: {pagination['records']} records; no trustworthy CVD update")
    current = {int(b["minute_ms"]): dict(b) for b in prior if int(b["minute_ms"]) >= cutoff}
    for b in cvd_bars(fresh, info["multiplier"], cutoff):
        minute = b["minute_ms"]
        existing = current.setdefault(minute, {"minute_ms": minute, "delta_base": "0", "delta_quote": "0", "trade_events": 0})
        for key in ("delta_base", "delta_quote"):
            existing[key] = str(Decimal(str(existing[key])) + Decimal(b[key]))
        existing["trade_events"] += b["trade_events"]
    sorted_bars = sorted(current.values(), key=lambda b: b["minute_ms"])
    cumulative = Decimal(0)
    cumulative_quote = Decimal(0)
    for b in sorted_bars:
        cumulative += Decimal(str(b["delta_base"]))
        cumulative_quote += Decimal(str(b["delta_quote"]))
        b["cvd_base"] = str(cumulative)
        b["cvd_quote"] = str(cumulative_quote)
    newest_time = max([last_observed] + [trade_timestamp_ms(t) for t in raw])
    new_ids = list(dict.fromkeys(list(state.get("recent_trade_ids", [])) + [str(t["id"]) for t in raw]))[-25000:]
    coverage = {
        **pagination, "collected_at": utc(at), "fetch_from": utc(from_ms),
        "latest_trade": utc(newest_time) if newest_time else None,
        "seconds_behind": round((at - newest_time) / 1000, 1) if newest_time else None,
        "initial_backfill_minutes": 6,
        "gap_possible": bool(last_observed and last_observed < at - 6 * 60000),
        "cvd_origin": utc(sorted_bars[0]["minute_ms"]) if sorted_bars else None,
    }
    state.update({"bars": sorted_bars, "last_trade_ms": newest_time, "recent_trade_ids": new_ids, "coverage": coverage, "updated_at": utc(at), "contract_multiplier": str(info["multiplier"])})
    write_json(path, state)
    return {"coverage": coverage, "recent_bars_1m": sorted_bars[-30:], "history_path": f"data/history/{symbol}.json"}


def collect_depth(symbol: str, gate: str, info: dict, at: int) -> dict:
    book = api("/futures/usdt/order_book", {"contract": gate, "limit": 50, "with_id": "true"})
    if not isinstance(book, dict) or not book.get("asks") or not book.get("bids"):
        raise ValueError("Missing Gate order book bids or asks")
    # Gate book levels are {p: price, s: number_of_contracts}.
    def side(items: list) -> list[dict]:
        ans = []
        for v in items:
            price = Decimal(str(v["p"]))
            contracts = Decimal(str(v["s"]))
            if price <= 0 or contracts < 0:
                raise ValueError("invalid order book level")
            base = contracts * info["multiplier"]
            ans.append({"price": str(price), "contracts": str(contracts), "base_qty": str(base), "quote_notional": str(price * base)})
        return ans
    sample = {"observed_at": utc(at), "observed_at_ms": at, "book_id": book.get("id"), "exchange_update": book.get("update"), "bids": side(book["bids"]), "asks": side(book["asks"])}
    path = HISTORY / (symbol + "_depth.json")
    state = read_json(path, {"symbol": symbol, "exchange_symbol": gate, "source": "gate_futures_rest_depth_snapshots", "snapshots": []})
    state["snapshots"] = [x for x in state.get("snapshots", []) if x.get("observed_at_ms", 0) >= at - WINDOW_MS][-575:] + [sample]
    state["limitation"] = "Sampled snapshots only; intra-run orderbook updates not captured. Cannot claim a continuous depth heatmap."
    write_json(path, state)
    return {"latest": sample, "samples": len(state["snapshots"]), "history_path": f"data/history/{symbol}_depth.json", "continuous_depth": False}


def parse_liquidations(ws_message: Any, tracked: set[str]) -> list[dict]:
    if not isinstance(ws_message, dict) or ws_message.get("channel") != "futures.public_liquidates" or ws_message.get("event") != "update":
        return []
    events = ws_message.get("result", [])
    if not isinstance(events, list):
        return []
    out = []
    for x in events:
        if not isinstance(x, dict) or x.get("contract") not in tracked:
            continue
        v = x.get("time_ms") or x.get("time")
        try:
            ms = int(v)
            if ms < 10**11:
                ms *= 1000
        except (TypeError, ValueError):
            continue
        out.append({"contract": x["contract"], "event_time_ms": ms, "price": str(x.get("price")), "signed_contracts": str(x.get("size")), "kind": "observed_gate_public_liquidation"})
    return out


def collect_liquidations(gate_symbols: list[str], seconds: int) -> dict:
    import websocket
    start = int(time.time() * 1000)
    events: list[dict] = []
    err = None
    subscribed = False
    ws = None
    try:
        ws = websocket.create_connection(WS_URL, timeout=8, header=["X-Gate-Size-Decimal: 1"])
        ws.send(json.dumps({"time": int(time.time()), "channel": "futures.public_liquidates", "event": "subscribe", "payload": gate_symbols}))
        ws.settimeout(1.0)
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            try:
                msg = json.loads(ws.recv())
            except websocket.WebSocketTimeoutException:
                continue
            if msg.get("channel") == "futures.public_liquidates" and msg.get("event") == "subscribe":
                subscribed = msg.get("result", {}).get("status") == "success"
                if not subscribed:
                    raise RuntimeError("Gate liquidation WS subscription rejected: " + str(msg)[:200])
            elif msg.get("error"):
                raise RuntimeError("Gate liquidation WS returned " + str(msg["error"])[:200])
            events.extend(parse_liquidations(msg, set(gate_symbols)))
        if not subscribed:
            err = "No liquidation subscription confirmation"
    except Exception as exc:
        err = f"{type(exc).__name__}: {str(exc)[:220]}"
        LOG.error("Gate liquidation subscription unavailable: %s", err)
    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
    end = int(time.time() * 1000)
    path = ROOT / "data" / "liquidations.json"
    state = read_json(path, {"source": "gate_futures_public_liquidates", "events": [], "coverage_windows": []})
    existing = [x for x in state.get("events", []) if x.get("event_time_ms", 0) >= end - WINDOW_MS]
    # Deduplicate by observed event values because WS may re-emit a liquidation.
    de_dup = {json.dumps(x, sort_keys=True): x for x in existing + events}
    state["events"] = sorted(de_dup.values(), key=lambda e: e["event_time_ms"])[-4000:]
    window = {"start_ms": start, "end_ms": end, "seconds": round((end - start)/1000, 1), "events": len(events), "subscribed": subscribed, "ok": err is None, "error": err}
    state["coverage_windows"] = [x for x in state.get("coverage_windows", []) if x.get("end_ms", 0) >= end - WINDOW_MS][-600:] + [window]
    state["limitation"] = "Only observed public liquidation reports during capture windows; no claims about periods not observed or predictive clusters."
    write_json(path, state)
    return {"capture": window, "recent_events": state["events"][-20:], "history_path": "data/liquidations.json", "predictive_liquidation_heatmap": False}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    pairs = symbols()
    now = int(time.time() * 1000)
    output: dict[str, Any] = {"generated_at": utc(now), "source": "gate_usdt_futures_public", "symbols": {}, "liquidations": {}, "health": {"errors": []}, "data_is_live_verified_in_this_run": False}
    usable = 0
    supported = []
    for sym, gate in pairs:
        item: dict[str, Any] = {"exchange_symbol": gate}
        try:
            info = contract_info(gate)
            supported.append(gate)
            item["multiplier"] = str(info["multiplier"])
        except Exception as exc:
            output["health"]["errors"].append(f"{sym} metadata: {type(exc).__name__}: {exc}")
            output["symbols"][sym] = item
            continue
        for name, callback in (("cvd", collect_cvd), ("depth", collect_depth)):
            try:
                item[name] = callback(sym, gate, info, int(time.time()*1000))
                usable += 1
            except Exception as exc:
                output["health"]["errors"].append(f"{sym} {name}: {type(exc).__name__}: {exc}")
        output["symbols"][sym] = item
    if supported:
        try:
            output["liquidations"] = collect_liquidations(supported, int(os.environ.get("ZEUS_LIQUIDATION_CAPTURE_SECONDS", "30")))
            if not output["liquidations"]["capture"]["ok"]:
                output["health"]["errors"].append("Gate liquidation WS: " + str(output["liquidations"]["capture"]["error"]))
        except Exception as exc:
            output["health"]["errors"].append(f"Gate liquidation WS: {type(exc).__name__}: {exc}")
    output["health"]["successful_symbol_feeds"] = usable
    output["data_is_live_verified_in_this_run"] = bool(usable)
    write_json(ROOT / "data" / "latest.json", output)
    LOG.info("Finished: %s Gate feeds, %s errors", usable, len(output["health"]["errors"]))
    if usable == 0:
        raise SystemExit("No Gate trade/depth data; see data/latest.json for exact errors")


if __name__ == "__main__":
    main()

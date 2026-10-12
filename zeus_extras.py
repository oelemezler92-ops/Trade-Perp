#!/usr/bin/env python3
"""ZEUS extra public Gate futures liquidation history and transparently assumed risk scenarios.

Reads the existing Gate full-market collector's stored orderbook snapshots; does
not rescan trades or duplicate TraderSpy OI/funding. No API credentials/trading.

IMPORTANT: leverage scenarios are mathematical hypotheticals, NOT observed
liquidation clusters or a prediction of open positions. Orderbook snapshots
are NOT a continuous orderbook heatmap. Missing intervals are recorded.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
OUT = ROOT / 'data' / 'extras'
BASES = ('https://api.gateio.ws/api/v4', 'https://fx-api.gateio.ws/api/v4')
LIMIT = 1000
RETENTION = 48 * 3600
BUDGET = max(60, min(int(os.environ.get('ZEUS_EXTRAS_BUDGET_SECONDS', '180')), 480))


def iso(sec: int | float | None = None) -> str:
    return datetime.fromtimestamp(time.time() if sec is None else sec, timezone.utc).isoformat().replace('+00:00', 'Z')


def load(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except FileNotFoundError:
        return default


def save(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + '\n', encoding='utf-8')
    tmp.replace(path)


def gate(path: str, params: dict[str, Any] | None = None) -> Any:
    query = '?' + urllib.parse.urlencode(params) if params else ''
    errors = []
    for base in BASES:
        try:
            req = urllib.request.Request(base + path + query, headers={'Accept': 'application/json', 'User-Agent': 'ZEUS-public-extras/1.0'})
            with urllib.request.urlopen(req, timeout=12) as r:
                payload = json.load(r)
            if isinstance(payload, dict) and payload.get('label'):
                raise ValueError(str(payload.get('message', payload['label'])))
            return payload
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            errors.append(f'{type(exc).__name__}: {str(exc)[:120]}')
    raise RuntimeError('Public Gate endpoint failed: ' + ' | '.join(errors))


def event_id(event: dict) -> str:
    fields = (str(event.get(k, '')) for k in ('time', 'contract', 'size', 'order_size', 'order_price', 'fill_price'))
    return hashlib.sha256('|'.join(fields).encode()).hexdigest()[:24]


def normalized_liq(event: dict) -> dict:
    # Size direction and fill status are not inferred from missing exchange data.
    contract = str(event['contract'])
    at = int(event['time'])
    if not contract.endswith('_USDT') or at < 1_500_000_000:
        raise ValueError('Unexpected contract or event timestamp')
    price = Decimal(str(event.get('fill_price') or event.get('order_price') or 0))
    if price <= 0:
        raise ValueError('No valid liquidation price')
    return {'event_id': event_id(event), 'time_s': at, 'contract': contract,
            'fill_price': str(price), 'order_price': str(event.get('order_price')),
            'order_size_contracts': str(event.get('order_size')),
            'position_size_contracts': str(event.get('size')),
            'type': 'public_gate_liquidation_order_history',
            'notional_usdt': None}  # no multiplier; do not fabricate notional


def fetch_liquidation_interval(start: int, end: int) -> list[dict]:
    """Return all public orders in one interval, recursively split if capped.

    Gate limits a requested time window to <=3600s. Equal-to-limit responses
    are assumed possibly truncated; one-second bins cannot be split further.
    """
    if end <= start or end - start > 3600:
        raise ValueError('Liquidation query window must be in (0, 3600] seconds')
    raw = gate('/futures/usdt/liq_orders', {'from': start, 'to': end, 'limit': LIMIT})
    if not isinstance(raw, list):
        raise ValueError('Unexpected Gate liquidation response shape')
    if len(raw) >= LIMIT:
        if end - start <= 1:
            raise RuntimeError('Liquidation response capped inside a 1-second window; completeness unknown')
        middle = (start + end) // 2
        return fetch_liquidation_interval(start, middle) + fetch_liquidation_interval(middle, end)
    return [normalized_liq(o) for o in raw]


def collect_liquidations(now: int, start_clock: float) -> dict:
    path = OUT / 'observed_liquidations.json'
    state = load(path, {'source': 'gate_public_futures_liq_orders', 'last_complete_to_s': None, 'events': [], 'coverage': []})
    prior_cursor = state.get('last_complete_to_s')
    # Initial lookback: last 60 minutes. Future runs advance stored cursor.
    cursor = max(int(prior_cursor) if prior_cursor else now - 3600, now - RETENTION)
    fetched = 0
    while cursor < now and time.monotonic() - start_clock < BUDGET - 15:
        stop = min(now, cursor + 600)  # 10-minute segments, safely <1h
        try:
            rows = fetch_liquidation_interval(cursor, stop)
        except Exception as exc:
            state['last_error'] = f'{type(exc).__name__}: {exc}'
            break
        existing = {x['event_id']: x for x in state['events']}
        existing.update({x['event_id']: x for x in rows})
        state['events'] = sorted((x for x in existing.values() if x['time_s'] >= now - RETENTION), key=lambda x: x['time_s'])
        if len(state['events']) > 40_000:
            state['last_error'] = 'Too many events for retention cap: history truncated'
            state['events'] = state['events'][-40_000:]
        state['coverage'] = [x for x in state['coverage'] if x['end_s'] >= now - RETENTION]
        state['coverage'].append({'from_s': cursor, 'end_s': stop, 'records': len(rows), 'public_rest': True})
        cursor = stop
        state['last_complete_to_s'] = cursor
        fetched += len(rows)
        state['last_error'] = None
        save(path, state)  # incrementally persist after every fully fetched segment
    save(path, state)
    return {'endpoint': '/futures/usdt/liq_orders', 'tested_with_live_response_this_run': state['last_complete_to_s'] is not None and (not prior_cursor or state['last_complete_to_s'] > prior_cursor),
            'records_newly_read': fetched, 'total_events_48h': len(state['events']),
            'last_complete_to_s': state['last_complete_to_s'], 'lag_seconds': (now - state['last_complete_to_s']) if state['last_complete_to_s'] else None,
            'error': state.get('last_error'), 'file': 'data/extras/observed_liquidations.json'}


def decimal(value: Any) -> Decimal | None:
    try:
        x = Decimal(str(value))
        return x if x.is_finite() else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def scenario_prices(meta: dict) -> dict:
    mark = decimal(meta.get('mark_price'))
    mmr = decimal(meta.get('maintenance_rate'))
    if mark is None or mark <= 0 or mmr is None or mmr < 0 or mmr >= Decimal('0.1'):
        return {'status': 'unavailable', 'reason': 'Missing valid mark price or first-tier maintenance rate'}
    max_leverage = decimal(meta.get('leverage_max'))
    rows = []
    for level in (5, 10, 20, 50):
        if max_leverage is not None and level > max_leverage:
            continue
        lev = Decimal(level)
        long = mark * (Decimal('1') - Decimal('1') / lev + mmr)
        short = mark * (Decimal('1') + Decimal('1') / lev - mmr)
        if long <= 0:
            continue
        rows.append({'assumed_leverage': level, 'long_side_rough_liq_price': str(+long), 'short_side_rough_liq_price': str(+short)})
    return {'status': 'hypothetical_only', 'mark_price': str(mark), 'first_tier_maintenance_rate': str(mmr), 'scenarios': rows,
            'accuracy_note': 'Illustrative isolated-margin simplification; ignores entry prices, maintenance tiers, fees, margin additions, actual positions and leverage distributions. NOT a liquidation cluster heatmap.'}


def sample_heatmap(depth: dict) -> dict:
    snapshots = depth.get('snapshots', [])
    if not isinstance(snapshots, list):
        return {'status': 'unavailable'}
    compact = []
    gaps = []
    last_ts = None
    for snapshot in snapshots[-575:]:
        observed = int(snapshot.get('observed_at_ms') or 0)
        if observed <= 0:
            continue
        if last_ts is not None and observed - last_ts > 10 * 60_000:
            gaps.append({'from_ms': last_ts, 'to_ms': observed})
        last_ts = observed
        bids = snapshot.get('bids') or []
        asks = snapshot.get('asks') or []
        def top(side: list[dict]) -> list[list[str]]:
            parsed = []
            for x in side:
                p, v = decimal(x.get('price')), decimal(x.get('quote_notional'))
                if p is not None and v is not None and p > 0 and v > 0:
                    parsed.append((v, p))
            return [[str(p), str(v)] for v, p in sorted(parsed, reverse=True)[:5]]
        compact.append({'at_ms': observed, 'largest_bids_price_quote': top(bids), 'largest_asks_price_quote': top(asks)})
    return {'status': 'sampled_orderbook_only', 'snapshots': len(compact), 'sampled_liquidity_points': compact[-30:], 'gaps_gt_10min': gaps[-15:],
            'continuous_depth': False, 'note': 'REST samples show observed liquidity at sampling times. Add/cancel events between snapshots cannot be inferred.'}


def construct_model(now: int) -> dict:
    all_contracts = gate('/futures/usdt/contracts')
    if not isinstance(all_contracts, list) or not all_contracts:
        raise ValueError('Contracts endpoint empty or invalid')
    output = {'generated_at': iso(now), 'source': 'gate_public_futures_contracts + existing ZEUS sampled depth',
              'scope': 'all_known_gate_futures_with_cached_depth', 'models': {},
              'not_a_true_liquidation_heatmap': True,
              'limitations': 'No actual liquidation-price distribution, individual leverage or positions. Liquidity heatmap sampled, not tick-by-tick.'}
    for c in all_contracts:
        name = c.get('name') if isinstance(c, dict) else None
        if not isinstance(name, str) or not name.endswith('_USDT'):
            continue
        sym = 'RAYSOLUSDT' if name == 'RAY_USDT' else name.replace('_', '')
        depth = load(ROOT / 'data' / 'history' / f'{sym}_depth.json', {})
        output['models'][sym] = {'exchange_symbol': name, 'risk_scenarios': scenario_prices(c), 'sampled_depth_heatmap': sample_heatmap(depth)}
    output['model_count'] = len(output['models'])
    return output


def run() -> None:
    began = time.monotonic()
    now = int(time.time())
    status = {'generated_at': iso(now), 'success': False, 'errors': [], 'liquidations': None, 'models': None}
    try:
        status['liquidations'] = collect_liquidations(now, began)
        if status['liquidations']['error']:
            status['errors'].append(status['liquidations']['error'])
    except Exception as exc:
        status['errors'].append('Liquidations: ' + f'{type(exc).__name__}: {exc}')
    try:
        models = construct_model(now)
        save(OUT / 'market_scenarios.json', models)
        status['models'] = {'count': models['model_count'], 'file': 'data/extras/market_scenarios.json', 'continuous_depth': False, 'estimated_liquidation_clusters': False}
    except Exception as exc:
        status['errors'].append('Models: ' + f'{type(exc).__name__}: {exc}')
    status['success'] = not status['errors'] and status['liquidations'] is not None and status['models'] is not None
    save(OUT / 'status.json', status)
    print(json.dumps({'success': status['success'], 'errors': status['errors'], 'liquidations': status['liquidations'], 'models': status['models']}, ensure_ascii=False))
    if not status['success']:
        raise SystemExit(1)


if __name__ == '__main__':
    run()

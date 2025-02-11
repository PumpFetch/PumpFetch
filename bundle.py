from decimal import Decimal, getcontext
import requests
from collections import defaultdict
from flask import Flask, request, jsonify

# Increase decimal precision
getcontext().prec = 50
app = Flask(__name__)

def calculate_real_sol(lamports: int) -> Decimal:
    """
    Calculate the real SOL amount by converting lamports to SOL.
    
    :param lamports: The amount in lamports.
    :return: The equivalent amount in SOL as a Decimal.
    """
    return Decimal(lamports) / Decimal("1000000000")

def get_trades(mint, limit=100, offset=0, minimum_size=1):
    """
    Fetch trades for a given unique token identifier.

    :param mint: The unique token identifier.
    :param limit: Number of trades to fetch per request.
    :param offset: Starting offset for pagination.
    :param minimum_size: Minimum size of the trade.
    :return: A JSON response containing trade data or None in case of an error.
    """
    url = f"YOUR_API_YOU_WANT_TO_USE"
    params = {
        "limit": limit,
        "offset": offset,
        "minimumSize": minimum_size
    }
    headers = {
        "accept": "*/*",
        "User-Agent": "Mozilla/5.0"
    }
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print("Error fetching trades:", e)
        return None

def get_all_trades(mint, limit=100, minimum_size=1):
    """
    Fetch all trades for a given token by handling pagination.

    For each trade, calculate the real SOL amount and store it as "real_sol".

    :param mint: The unique token identifier.
    :param limit: The maximum number of trades per page.
    :param minimum_size: The minimum size to filter trades.
    :return: A list of all trades with the added "real_sol" field.
    """
    all_trades = []
    offset = 0
    while True:
        trades = get_trades(mint, limit=limit, offset=offset, minimum_size=minimum_size)
        if trades is None:
            break
        for trade in trades:
            lamports = trade.get("sol_amount", 0)
            # Calculate the real SOL amount and store it as "real_sol"
            trade["real_sol"] = calculate_real_sol(lamports)
        all_trades.extend(trades)
        if len(trades) < limit:
            break
        offset += limit
    return all_trades

def filter_small_transactions(trades, threshold=Decimal("0.05")):
    """
    Filter trades to only include those with a converted SOL amount of at least 0.05 SOL.

    :param trades: The list of trade dictionaries.
    :param threshold: The minimum SOL threshold.
    :return: A filtered list of trades.
    """
    filtered = []
    for trade in trades:
        if trade.get("real_sol") is not None and trade["real_sol"] >= threshold:
            filtered.append(trade)
    return filtered

def group_by_slot(trades):
    """
    Group trades by their 'slot'. Only groups with at least 2 trades (bundles) are returned.

    :param trades: The list of trade dictionaries.
    :return: A dictionary mapping slot values to lists of trades, for slots with more than one trade.
    """
    bundles = {}
    for trade in trades:
        slot = trade.get("slot")
        if slot is not None:
            bundles.setdefault(slot, []).append(trade)
    # Only return slots with at least 2 transactions
    return {slot: tlist for slot, tlist in bundles.items() if len(tlist) > 1}

@app.route('/bundles', methods=['GET'])
def bundles_analysis():
    """
    For a given token identifier (mint):
      - Retrieve all trades.
      - Filter trades to include only those with a real_sol amount >= 0.05 SOL.
      - Group the trades by slot (only groups with ≥2 trades are considered bundles).
      - For each bundle, calculate the total buys, total sells, and the net result 
        (net_result = total_sell - total_buy).
    
    :return: A JSON response with detailed bundle analysis and overall summary.
    """
    mint = request.args.get("mint")
    if not mint:
        return jsonify({"error": "Missing mint parameter"}), 400

    trades = get_all_trades(mint)
    if not trades:
        return jsonify({"error": "No trades found or API error"}), 404

    filtered_trades = filter_small_transactions(trades, threshold=Decimal("0.05"))
    bundles_grouped = group_by_slot(filtered_trades)

    overall_total_buy = Decimal("0")
    overall_total_sell = Decimal("0")
    bundle_results = []

    for slot, trades_list in bundles_grouped.items():
        total_buy = Decimal("0")
        total_sell = Decimal("0")
        for trade in trades_list:
            # Determine if the trade is a buy (true values) or a sell (any other value)
            if trade.get("is_buy") in [True, "true", "True"]:
                total_buy += trade.get("real_sol", Decimal("0"))
            else:
                total_sell += trade.get("real_sol", Decimal("0"))
        net = total_sell - total_buy
        overall_total_buy += total_buy
        overall_total_sell += total_sell
        bundle_results.append({
            "slot": slot,
            "trades": trades_list,
            "total_buy": float(total_buy),
            "total_sell": float(total_sell),
            "net_result": float(net),
            "result_type": "profit" if net > 0 else ("loss" if net < 0 else "break-even")
        })

    overall_net = overall_total_sell - overall_total_buy
    response = {
        "bundles": bundle_results,
        "overall": {
            "total_buy": float(overall_total_buy),
            "total_sell": float(overall_total_sell),
            "net_result": float(overall_net),
            "result_type": "profit" if overall_net > 0 else ("loss" if overall_net < 0 else "break-even")
        }
    }
    return jsonify(response)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

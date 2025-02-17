from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import pandas as pd
from datetime import datetime
import os

# Constants
GRAPHQL_URL = os.getenv('GRAPHQL_URL', 'https://stats.fluidprotocol.xyz/v1/graphql')
TOTAL_REWARDS = 4_010_000 # 4,010,000 FUEL
ETH_SHARE = 0.45
FUEL_SHARE = 0.55
PRECISION = 1e9

# Time period constants
START_DATE = datetime(2025, 1, 15).timestamp()
END_DATE = datetime(2025, 2, 15).timestamp()
TOTAL_PERIOD = END_DATE - START_DATE

# Set up GraphQL client
transport = RequestsHTTPTransport(url=GRAPHQL_URL)
client = Client(transport=transport, fetch_schema_from_transport=True)

# Query to get all relevant trove events
TROVE_EVENTS_QUERY = """
query {
    opens: BorrowOperations_OpenTroveEvent(
        where: {timestamp: {_lte: %d}}
        order_by: {timestamp: asc}
    ) {
        identity
        asset
        collateral
        timestamp
    }
    closes: BorrowOperations_CloseTroveEvent(
        where: {timestamp: {_lte: %d}}
        order_by: {timestamp: asc}
    ) {
        identity
        asset
        timestamp
    }
    adjusts: BorrowOperations_AdjustTroveEvent(
        where: {timestamp: {_gte: %d, _lte: %d}}
        order_by: {timestamp: asc}
    ) {
        identity
        asset
        collateral
        collateralChange
        isCollateralIncrease
        timestamp
    }
    liquidations: TroveManager_TroveFullLiquidationEvent(
        where: {timestamp: {_lte: %d}}
        order_by: {timestamp: asc}
    ) {
        identity
        asset
        timestamp
    }
    partial_liquidations: TroveManager_TrovePartialLiquidationEvent(
        where: {timestamp: {_lte: %d}}
        order_by: {timestamp: asc}
    ) {
        identity
        asset
        remaining_collateral
        timestamp
    }
    redemptions: TroveManager_RedemptionEvent(
        where: {timestamp: {_lte: %d}}
        order_by: {timestamp: asc}
    ) {
        identity
        asset
        collateral_amount
        timestamp
    }
}
""" % (END_DATE, END_DATE, START_DATE, END_DATE, END_DATE, END_DATE, END_DATE)

USDF_MINT_QUERY = """
query {
    USDF_Mint(order_by: {timestamp: asc}) {
        amount
        timestamp
    }
}
"""

def run_mint_query(client):
    from gql import gql
    query = gql(USDF_MINT_QUERY)
    result = client.execute(query)
    return result

def calculate_rewards():
    # Fetch all events
    result = client.execute(gql(TROVE_EVENTS_QUERY))
    
    # Initialize DataFrames for tracking trove states
    troves = {}  # Dictionary to track trove states: {(identity, asset): [(start_time, end_time, collateral)]}
    
    # Process open events
    for event in result['opens']:
        key = (event['identity'], event['asset'])
        if key not in troves:
            troves[key] = []
        troves[key].append({
            'start_time': int(event['timestamp']),
            'end_time': int(END_DATE),
            'collateral': float(event['collateral']) / PRECISION
        })
    
    # Process close events
    for event in result['closes']:
        key = (event['identity'], event['asset'])
        if key in troves and troves[key]:
            # Find the relevant open period and close it
            for period in troves[key]:
                if period['end_time'] > int(event['timestamp']):
                    period['end_time'] = int(event['timestamp'])
    
    # Process liquidation events
    for event in result['liquidations']:
        key = (event['identity'], event['asset'])
        if key in troves and troves[key]:
            # Find the relevant open period and close it
            for period in troves[key]:
                if period['end_time'] > int(event['timestamp']):
                    period['end_time'] = int(event['timestamp'])
    
    # Process partial liquidation events
    for event in result['partial_liquidations']:
        key = (event['identity'], event['asset'])
        timestamp = int(event['timestamp'])
        if key in troves and troves[key]:
            # Find the active period for this partial liquidation
            for period in troves[key]:
                if period['start_time'] <= timestamp and period['end_time'] > timestamp:
                    # Close the current period
                    old_end = period['end_time']
                    period['end_time'] = timestamp
                    
                    # Create a new period with remaining collateral
                    new_collateral = float(event['remaining_collateral']) / PRECISION
                    
                    # Add the new period with remaining collateral
                    troves[key].append({
                        'start_time': timestamp,
                        'end_time': old_end,
                        'collateral': new_collateral
                    })
                    break

    # Process redemption events
    for event in result['redemptions']:
        key = (event['identity'], event['asset'])
        timestamp = int(event['timestamp'])
        if key in troves and troves[key]:
            # Find the active period for this redemption
            for period in troves[key]:
                if period['start_time'] <= timestamp and period['end_time'] > timestamp:
                    # Close the current period
                    old_end = period['end_time']
                    period['end_time'] = timestamp
                    
                    # Calculate new collateral after redemption
                    redeemed_amount = float(event['collateral_amount']) / PRECISION
                    new_collateral = period['collateral'] - redeemed_amount
                    
                    # Only create new period if there's remaining collateral
                    if new_collateral > 0:
                        troves[key].append({
                            'start_time': timestamp,
                            'end_time': old_end,
                            'collateral': new_collateral
                        })
                    break

    # Process adjust events
    for event in result['adjusts']:
        key = (event['identity'], event['asset'])
        timestamp = int(event['timestamp'])
        if key in troves and troves[key]:
            # Find the active period for this adjustment
            for period in troves[key]:
                if period['start_time'] <= timestamp and period['end_time'] > timestamp:
                    # Close the current period
                    old_end = period['end_time']
                    period['end_time'] = timestamp
                    
                    # Create a new period with updated collateral
                    new_collateral = period['collateral']
                    change = float(event['collateralChange']) / PRECISION
                    if event['isCollateralIncrease']:
                        new_collateral += change
                    else:
                        new_collateral -= change
                    
                    # Add the new period
                    troves[key].append({
                        'start_time': timestamp,
                        'end_time': old_end,
                        'collateral': new_collateral
                    })
                    break
    
    # Filter out troves that were closed before our start date
    for key in list(troves.keys()):
        troves[key] = [period for period in troves[key] 
                      if period['end_time'] > int(START_DATE) and 
                         period['start_time'] < int(END_DATE)]
        if not troves[key]:  # Remove empty entries
            del troves[key]
    
    # Calculate time-weighted collateral for each asset
    eth_weights = {}
    fuel_weights = {}
    
    for (identity, asset), periods in troves.items():
        total_weighted_collateral = 0
        
        for period in periods:
            # Ensure period is within our time window
            start = max(period['start_time'], int(START_DATE))
            end = min(period['end_time'], int(END_DATE))
            
            if start < end:  # Valid period
                duration = end - start
                weighted_collateral = period['collateral'] * (duration / TOTAL_PERIOD)
                total_weighted_collateral += weighted_collateral
        
        if total_weighted_collateral > 0:
            if asset == 'ETH':
                eth_weights[identity] = total_weighted_collateral
            elif asset == 'FUEL':
                fuel_weights[identity] = total_weighted_collateral
    
    # Calculate rewards
    eth_total_weight = sum(eth_weights.values())
    fuel_total_weight = sum(fuel_weights.values())
    
    eth_rewards = {
        identity: (weight / eth_total_weight) * (TOTAL_REWARDS * ETH_SHARE)
        for identity, weight in eth_weights.items()
    }
    
    fuel_rewards = {
        identity: (weight / fuel_total_weight) * (TOTAL_REWARDS * FUEL_SHARE)
        for identity, weight in fuel_weights.items()
    }
    
    # Combine rewards
    all_rewards = {}
    for identity in set(list(eth_rewards.keys()) + list(fuel_rewards.keys())):
        all_rewards[identity] = eth_rewards.get(identity, 0) + fuel_rewards.get(identity, 0)
    
    # Create DataFrame and save to CSV
    rewards_df = pd.DataFrame([
        {'wallet': wallet, 'amount': amount}
        for wallet, amount in all_rewards.items()
    ])
    
    rewards_df.to_csv('trove_rewards.csv', index=False)
    print(f"Rewards calculated and saved to trove_rewards.csv")
    print(f"Total rewards distributed: {rewards_df['amount'].sum():,.2f}")

if __name__ == "__main__":
    calculate_rewards()

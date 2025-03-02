from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import pandas as pd
from datetime import datetime
import os

# Constants
GRAPHQL_URL = os.getenv('GRAPHQL_URL', 'https://stats.fluidprotocol.xyz/v1/graphql')
TOTAL_REWARDS = 1_800_000 # 1,800,000 FUEL
ETH_SHARE = 0.45
FUEL_SHARE = 0.55
PRECISION = 1e9


# Debug output for specific user
DEBUG_WALLET = ""
DEBUG_ASSET = "FUEL"

# Time period constants
START_DATE = datetime(2025, 1, 15).timestamp()
END_DATE = datetime(2025, 3, 1).timestamp()
TOTAL_PERIOD = END_DATE - START_DATE

# Set up GraphQL client
transport = RequestsHTTPTransport(url=GRAPHQL_URL)
client = Client(transport=transport, fetch_schema_from_transport=True)

# Query to get all relevant trove events
TROVE_EVENTS_QUERY = """
query {
    opens: BorrowOperations_OpenTroveEvent(
        where: {timestamp: {_lte: %d}, asset: {_in: ["FUEL", "ETH"]}}
        order_by: {timestamp: asc}
    ) {
        identity
        asset
        collateral
        timestamp
    }
    closes: BorrowOperations_CloseTroveEvent(
        where: {timestamp: {_lte: %d}, asset: {_in: ["FUEL", "ETH"]}}
        order_by: {timestamp: asc}
    ) {
        identity
        asset
        timestamp
    }
    adjusts: BorrowOperations_AdjustTroveEvent(
        where: {timestamp: {_lte: %d}, asset: {_in: ["FUEL", "ETH"]}}
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
        where: {timestamp: {_lte: %d}, asset: {_in: ["FUEL", "ETH"]}}
        order_by: {timestamp: asc}
    ) {
        identity
        asset
        timestamp
    }
    partial_liquidations: TroveManager_TrovePartialLiquidationEvent(
        where: {timestamp: {_lte: %d}, asset: {_in: ["FUEL", "ETH"]}}
        order_by: {timestamp: asc}
    ) {
        identity
        asset
        remaining_collateral
        timestamp
    }
    redemptions: TroveManager_RedemptionEvent(
        where: {timestamp: {_lte: %d}, asset: {_in: ["FUEL", "ETH"]}}
        order_by: {timestamp: asc}
    ) {
        identity
        asset
        collateral_amount
        timestamp
    }
}
""" % (END_DATE, END_DATE, END_DATE, END_DATE, END_DATE, END_DATE)


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
            # Only close periods that started before this close event
            for period in troves[key]:
                if period['start_time'] < int(event['timestamp']) and period['end_time'] > int(event['timestamp']):
                    period['end_time'] = int(event['timestamp'])
    
    # Process liquidation events
    for event in result['liquidations']:
        key = (event['identity'], event['asset'])
        if key in troves and troves[key]:
            # Only close periods that started before this liquidation
            for period in troves[key]:
                if period['start_time'] < int(event['timestamp']) and period['end_time'] > int(event['timestamp']):
                    period['end_time'] = int(event['timestamp'])
    
    # Process partial liquidation events
    for event in result['partial_liquidations']:
        key = (event['identity'], event['asset'])
        timestamp = int(event['timestamp'])
        if key in troves and troves[key]:
            # Find the active period that started before this event
            for period in troves[key]:
                if period['start_time'] < timestamp and period['end_time'] > timestamp:
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
            for period in troves[key]:
                if period['start_time'] < timestamp and period['end_time'] > timestamp:
                    old_end = period['end_time']
                    period['end_time'] = timestamp
                    redeemed_amount = float(event['collateral_amount']) / PRECISION
                    new_collateral = period['collateral'] - redeemed_amount
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
                    if period['start_time'] < timestamp and period['end_time'] > timestamp:
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
    
    # debug periods print out for user
    print(f"\nFinal processed trove periods for {DEBUG_WALLET}:")
    if (DEBUG_WALLET, DEBUG_ASSET) in troves:
        print(f"\n{DEBUG_ASSET} Trove periods:")
        for period in troves[(DEBUG_WALLET, DEBUG_ASSET)]:
            print(f"Start: {datetime.fromtimestamp(period['start_time'])}")
            print(f"End: {datetime.fromtimestamp(period['end_time'])}")
            print(f"Collateral: {period['collateral']}")
            print("---")
    else:
        print(f"No {DEBUG_ASSET} trove periods found after processing")
    
    # Filter out troves that lie fully before/after our reward window AND clamp them to the window
    for key in list(troves.keys()):
        clamped_periods = []
        for period in troves[key]:
            # If the end is after the start date, and the start is before the end date,
            # then there is at least some overlap with [START_DATE, END_DATE].
            if period['end_time'] > int(START_DATE) and period['start_time'] < int(END_DATE):
                # Clamp the start to START_DATE if it's earlier
                if period['start_time'] < int(START_DATE):
                    period['start_time'] = int(START_DATE)

                # Clamp the end to END_DATE if it's later
                if period['end_time'] > int(END_DATE):
                    period['end_time'] = int(END_DATE)

                # After clamping, make sure it's still a valid period
                if period['end_time'] > period['start_time']:
                    clamped_periods.append(period)
        # Replace with our "clamped" list. If empty, we'll remove that key altogether.
        troves[key] = clamped_periods
        if not troves[key]:
            del troves[key]
    
    # Calculate time-weighted collateral for each asset
    eth_weights = {}
    fuel_weights = {}
    
    for (identity, asset), periods in troves.items():
        total_weighted_collateral = 0
        
        for period in periods:
            if identity == DEBUG_WALLET and asset == DEBUG_ASSET:
                print(f"Processing period: {period}")
            
            # Ensure period is within our time window
            start = max(period['start_time'], int(START_DATE))
            end = min(period['end_time'], int(END_DATE))
            
            if start < end:  # Valid period
                if identity == DEBUG_WALLET and asset == DEBUG_ASSET:
                    print(f"Valid period: {period}")
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
        {'wallet': wallet, 'amount': (amount * 1e9 // 1) / 1e9}  # Floor to 9 decimal places
        for wallet, amount in all_rewards.items()
    ])
    

    
    # Debug raw events for this wallet
    print(f"\nDEBUG RAW EVENTS for {DEBUG_WALLET}:")
    print("\nOpen events:")
    for event in result['opens']:
        if event['identity'] == DEBUG_WALLET and event['asset'] == DEBUG_ASSET:
            print(f"Asset: {event['asset']}")
            print(f"Collateral: {float(event['collateral']) / PRECISION}")
            print(f"Timestamp: {datetime.fromtimestamp(int(event['timestamp']))}")
    
    print("\nClose events:")
    for event in result['closes']:
        if event['identity'] == DEBUG_WALLET and event['asset'] == DEBUG_ASSET:
            print(f"Asset: {event['asset']}")
            print(f"Timestamp: {datetime.fromtimestamp(int(event['timestamp']))}")
    
    print("\nAdjust events:")
    for event in result['adjusts']:
        if event['identity'] == DEBUG_WALLET and event['asset'] == DEBUG_ASSET:
            print(f"Asset: {event['asset']}")
            print(f"Collateral Change: {float(event['collateralChange']) / PRECISION}")
            print(f"Is Increase: {event['isCollateralIncrease']}")
            print(f"Timestamp: {datetime.fromtimestamp(int(event['timestamp']))}")
    
    print("\nLiquidation events:")
    for event in result['liquidations']:
        if event['identity'] == DEBUG_WALLET and event['asset'] == DEBUG_ASSET:
            print(f"Asset: {event['asset']}")
            print(f"Timestamp: {datetime.fromtimestamp(int(event['timestamp']))}")
    
    print("\nPartial Liquidation events:")
    for event in result['partial_liquidations']:
        if event['identity'] == DEBUG_WALLET and event['asset'] == DEBUG_ASSET:
            print(f"Asset: {event['asset']}")
            print(f"Remaining Collateral: {float(event['remaining_collateral']) / PRECISION}")
            print(f"Timestamp: {datetime.fromtimestamp(int(event['timestamp']))}")
    
    print("\nRedemption events:")
    for event in result['redemptions']:
        if event['identity'] == DEBUG_WALLET and event['asset'] == DEBUG_ASSET:
            print(f"Asset: {event['asset']}")
            print(f"Collateral Amount: {float(event['collateral_amount']) / PRECISION}")
            print(f"Timestamp: {datetime.fromtimestamp(int(event['timestamp']))}")
    
    # After processing all events, show final trove periods
    print(f"\nFinal processed trove periods for {DEBUG_WALLET}:")
    if (DEBUG_WALLET, 'FUEL') in troves:
        print("\nFUEL Trove periods:")
        for period in troves[(DEBUG_WALLET, 'FUEL')]:
            print(f"Start: {datetime.fromtimestamp(period['start_time'])}")
            print(f"End: {datetime.fromtimestamp(period['end_time'])}")
            print(f"Collateral: {period['collateral']} FUEL")
            print(f"Duration: {period['end_time'] - period['start_time']} seconds")
            print("---")
    else:
        print("No FUEL trove periods found after processing")
        
    if (DEBUG_WALLET, 'ETH') in troves:
        print("\nETH Trove periods:")
        for period in troves[(DEBUG_WALLET, 'ETH')]:
            print(f"Start: {datetime.fromtimestamp(period['start_time'])}")
            print(f"End: {datetime.fromtimestamp(period['end_time'])}")
            print(f"Collateral: {period['collateral']} ETH")
            print(f"Duration: {period['end_time'] - period['start_time']} seconds")
            print("---")
    else:
        print("No ETH trove periods found after processing")
    
    # Show the filtering step
    print(f"\nChecking if periods fall within reward window:")
    print(f"Reward window start: {datetime.fromtimestamp(START_DATE)}")
    print(f"Reward window end: {datetime.fromtimestamp(END_DATE)}")
    
    # Sort rewards in descending order
    rewards_df = rewards_df.sort_values('amount', ascending=False)
    
    rewards_df.to_csv('trove_rewards.csv', index=False, float_format='%.9f')  # Format with 9 decimal places
    print(f"Rewards calculated and saved to trove_rewards.csv")
    print(f"Total rewards distributed: {rewards_df['amount'].sum():,.9f}")

if __name__ == "__main__":
    calculate_rewards()

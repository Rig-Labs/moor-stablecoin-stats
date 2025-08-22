import streamlit as st
import pandas as pd
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import plotly.express as px
from queries import (
    TOTAL_SUPPLY_QUERY,
    MINT_BURN_QUERIES,
    TROVE_EVENTS_QUERY,
    STABILITY_POOL_QUERY,
    REDEMPTION_QUERY,
    LIQUIDATION_QUERY,
)
import os

# Constants
PRECISION = 1e9
GRAPHQL_URL = os.getenv('GRAPHQL_URL', 'http://localhost:8080/v1/graphql')

# Set up the GraphQL client
transport = RequestsHTTPTransport(url=GRAPHQL_URL)
client = Client(transport=transport, fetch_schema_from_transport=False)

# Convert the query strings to gql objects
query = gql(TOTAL_SUPPLY_QUERY)
mint_query = gql(MINT_BURN_QUERIES["mint"])
burn_query = gql(MINT_BURN_QUERIES["burn"])
trove_events_query = gql(TROVE_EVENTS_QUERY)
stability_pool_query = gql(STABILITY_POOL_QUERY)
redemption_query = gql(REDEMPTION_QUERY)
liquidation_query = gql(LIQUIDATION_QUERY)

def process_df(df):
    """Helper function to process dataframes"""
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df['amount'] = df['amount'].astype(float) / PRECISION
    return df

def fetch_and_process_data():
    # Execute the query
    result = client.execute(query)
    
    # Convert to DataFrame
    df = pd.DataFrame(result['USDM_TotalSupplyEvent'])
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    
    # Convert amount from wei to USDM (assuming 18 decimals)
    df['amount'] = df['amount'].astype(float) / PRECISION
    
    # Filter out rows with large jumps
    threshold = 200_000 
    df['amount_diff'] = df['amount'].diff().abs()
    df = df[df['amount_diff'].fillna(0) < threshold]
    df = df.drop('amount_diff', axis=1)
    
    return df

def fetch_mint_burn_data():
    # Execute the queries
    mint_result = client.execute(mint_query)
    burn_result = client.execute(burn_query)
    
    # Convert to DataFrames
    mint_df = process_df(pd.DataFrame(mint_result['USDM_Mint']))
    burn_df = process_df(pd.DataFrame(burn_result['USDM_Burn']))
    
    # Group by day and sum the amounts
    mint_df = mint_df.groupby(mint_df['timestamp'].dt.date)['amount'].sum().reset_index()
    burn_df = burn_df.groupby(burn_df['timestamp'].dt.date)['amount'].sum().reset_index()
    
    return mint_df, burn_df

def fetch_trove_data():
    """Fetch and process trove-related events"""
    result = client.execute(trove_events_query)
    
    # Convert each event type to DataFrame
    opens_df = pd.DataFrame(result['open'])
    closes_df = pd.DataFrame(result['close'])
    liquidations_df = pd.DataFrame(result['liquidation_full'])
    
    # Add event type column to each DataFrame
    opens_df['event'] = 1  # +1 for opens
    closes_df['event'] = -1  # -1 for closes
    liquidations_df['event'] = -1  # -1 for liquidations
    
    # Combine all events
    all_events = pd.concat([opens_df, closes_df, liquidations_df])
    all_events['timestamp'] = pd.to_datetime(all_events['timestamp'], unit='s')
    
    # Group by asset and date, calculate running total of troves
    all_events = all_events.sort_values('timestamp')
    grouped = all_events.groupby(['asset', all_events['timestamp'].dt.date])['event'].sum().reset_index()
    
    # Create a complete date range
    date_range = pd.date_range(start=grouped['timestamp'].min(), 
                              end=grouped['timestamp'].max(), 
                              freq='D')
    
    # Create a MultiIndex with all asset-date combinations
    assets = grouped['asset'].unique()
    multi_index = pd.MultiIndex.from_product([assets, date_range], 
                                           names=['asset', 'timestamp'])
    
    # Reindex and forward fill
    grouped = (grouped.set_index(['asset', 'timestamp'])
                     .reindex(multi_index)
                     .fillna(0))  # Fill missing dates with 0 events
    
    # Reset index and calculate cumulative sum
    grouped = grouped.reset_index()
    grouped['active_troves'] = grouped.groupby('asset')['event'].cumsum()
    
    return grouped



def fetch_stability_pool_data():
    """Fetch and process Stability Pool deposit/withdrawal events"""
    result = client.execute(stability_pool_query)
    
    # Convert to DataFrames and process
    deposits_df = process_df(pd.DataFrame(result['deposits']))
    withdrawals_df = process_df(pd.DataFrame(result['withdrawals']))
    
    # Group by day and sum the amounts
    deposits_df = deposits_df.groupby(deposits_df['timestamp'].dt.date)['amount'].sum().reset_index()
    withdrawals_df = withdrawals_df.groupby(withdrawals_df['timestamp'].dt.date)['amount'].sum().reset_index()
    
    # Label the events
    deposits_df['type'] = 'Deposit'
    withdrawals_df['type'] = 'Withdrawal'
    withdrawals_df['amount'] = -withdrawals_df['amount']  # Make withdrawals negative
    
    # Combine deposit and withdrawal data
    combined_df = pd.concat([deposits_df, withdrawals_df])
    
    # Calculate running total of deposits in Stability Pool
    combined_df = combined_df.sort_values('timestamp')
    combined_df['total_deposited'] = combined_df['amount'].cumsum()
    
    return combined_df

def fetch_redemption_data():
    """Fetch and process redemption events"""
    result = client.execute(redemption_query)
    df = pd.DataFrame(result['TroveManager_RedemptionEvent'])
    
    # Check if dataframe is empty
    if df.empty:
        return pd.DataFrame(columns=['timestamp', 'asset', 'usdm_amount', 
                                   'collateral_amount', 'collateral_price'])
    
    # Process data only if df is not empty
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df['usdm_amount'] = df['usdm_amount'].astype(float) / PRECISION
    df['collateral_amount'] = df['collateral_amount'].astype(float) / PRECISION
    df['collateral_price'] = df['collateral_price'].astype(float) / PRECISION
    return df

def fetch_liquidation_data():
    """Fetch and process liquidation events"""
    result = client.execute(liquidation_query)
    
    # Process full liquidations
    full_df = pd.DataFrame(result['full'])
    if not full_df.empty:
        full_df['timestamp'] = pd.to_datetime(full_df['timestamp'], unit='s')
        full_df['debt'] = full_df['debt'].astype(float) / PRECISION
        full_df['collateral'] = full_df['collateral'].astype(float) / PRECISION
        full_df['type'] = 'Full'

    # Process partial liquidations
    partial_df = pd.DataFrame(result['partial'])
    if not partial_df.empty:
        partial_df['timestamp'] = pd.to_datetime(partial_df['timestamp'], unit='s')
        partial_df['debt'] = partial_df['remaining_debt'].astype(float) / PRECISION
        partial_df['collateral'] = partial_df['remaining_collateral'].astype(float) / PRECISION
        partial_df['type'] = 'Partial'
        partial_df = partial_df.drop(['remaining_debt', 'remaining_collateral'], axis=1)

    # Combine the dataframes
    liquidation_df = pd.concat([full_df, partial_df]) if not (full_df.empty and partial_df.empty) else pd.DataFrame()
    
    if not liquidation_df.empty:
        # Group by day and asset
        daily_liquidations = liquidation_df.groupby(
            [liquidation_df['timestamp'].dt.date, 'asset', 'type']
        ).agg({
            'debt': 'sum',
            'collateral': 'sum'
        }).reset_index()
        
        return daily_liquidations
    
    return pd.DataFrame(columns=['timestamp', 'asset', 'type', 'debt', 'collateral'])

def format_number(num):
    """Format numbers to human readable format with K and M suffixes"""
    if abs(num) >= 1_000_000:
        return f"{num/1_000_000:.3f}M"
    elif abs(num) >= 1_000:
        return f"{num/1_000:.1f}K"
    else:
        return f"{num:.3f}"

# Streamlit app
st.set_page_config(page_title="Fluid Analytics")
st.title('Fluid Analytics')

# Add metrics section
col1, col2, col3, col4, col5 = st.columns(5)

try:
    # Fetch all data
    df = fetch_and_process_data()
    mint_df, burn_df = fetch_mint_burn_data()
    trove_data = fetch_trove_data()
    stability_pool_data = fetch_stability_pool_data()
    redemption_data = fetch_redemption_data()
    liquidation_data = fetch_liquidation_data()
    
    # Calculate current and 2-week-ago values for each metric
    two_weeks_ago = pd.Timestamp.now() - pd.Timedelta(days=14)

    # 1. Total USDM Supply
    current_supply = df.iloc[-1]['amount'] if not df.empty else 0
    past_supply = df[df['timestamp'] < two_weeks_ago].iloc[-1]['amount'] if not df.empty else 0
    supply_delta = current_supply - past_supply

    # 2. Total Troves
    current_troves = trove_data.groupby('timestamp')['active_troves'].sum().iloc[-1] if not trove_data.empty else 0
    past_troves = trove_data[pd.to_datetime(trove_data['timestamp']) < two_weeks_ago].groupby('timestamp')['active_troves'].sum().iloc[-1] if not trove_data.empty else 0
    troves_delta = current_troves - past_troves

    # 3. Stability Pool Deposits
    current_sp = stability_pool_data.iloc[-1]['total_deposited'] if not stability_pool_data.empty else 0
    past_sp = stability_pool_data[pd.to_datetime(stability_pool_data['timestamp']) < two_weeks_ago].iloc[-1]['total_deposited'] if not stability_pool_data.empty else 0
    sp_delta = current_sp - past_sp



    # 5. Total Liquidations (last 2 weeks)
    current_liquidations = liquidation_data[pd.to_datetime(liquidation_data['timestamp']) >= two_weeks_ago]['debt'].sum() if not liquidation_data.empty else 0
    past_liquidations = liquidation_data[(pd.to_datetime(liquidation_data['timestamp']) < two_weeks_ago) & 
                                       (pd.to_datetime(liquidation_data['timestamp']) >= two_weeks_ago - pd.Timedelta(days=14))]['debt'].sum() if not liquidation_data.empty else 0
    liquidations_delta = current_liquidations - past_liquidations

    # Calculate total mints for last 2 weeks
    two_weeks_mints = mint_df[pd.to_datetime(mint_df['timestamp']) >= two_weeks_ago]['amount'].sum()
    two_week_distribution = two_weeks_mints / 200  # USDM distributed to stakers

    # Display metrics
    with col1:
        st.metric("Total USDM (2W Δ)", 
                 format_number(current_supply), 
                 format_number(supply_delta))
    with col2:
        st.metric("Troves (2W Δ)", 
                 f"{current_troves:,.0f}", 
                 f"{troves_delta:+,.0f}")
    with col3:
        st.metric("SP Deposits (2W Δ)", 
                 format_number(current_sp), 
                 format_number(sp_delta))
    with col4:
        st.metric("USDM to Stakers (2W)", 
                 f"{format_number(two_week_distribution)}")
    
    # Total Supply Chart
    st.subheader('USDM Total Supply Over Time')
    fig_supply = px.line(df, 
                        x='timestamp', 
                        y='amount',
                        title='USDM Total Supply',
                        labels={'timestamp': 'Date', 'amount': 'USDM Supply'})
    st.plotly_chart(fig_supply)
    
    # Convert mint amounts to positive and burn amounts to negative
    mint_df['type'] = 'Mint'
    burn_df['type'] = 'Burn'
    burn_df['amount'] = -burn_df['amount']  # Make burns negative

    # Combine mint and burn data
    combined_df = pd.concat([mint_df, burn_df])

    # Mint and Burn Combined Chart
    st.subheader('USDM Mints and Burns')
    fig_combined = px.bar(combined_df,
                         x='timestamp',
                         y='amount',
                         color='type',
                         title='Daily USDM Mints and Burns',
                         labels={'timestamp': 'Date', 'amount': 'USDM Amount'},
                         color_discrete_map={'Mint': 'green', 'Burn': 'red'})
    
    # Update layout to make it more readable
    fig_combined.update_layout(
        barmode='relative',  # Allows bars to stack from zero
        yaxis_title='USDM Amount (+ Mints, - Burns)',
        showlegend=True
    )
    st.plotly_chart(fig_combined)
    
    # Add Troves Count Chart
    st.subheader('Active Troves Count Over Time')
    fig_troves = px.bar(trove_data,
                       x='timestamp',
                       y='active_troves',
                       color='asset',
                       title='Number of Active Troves by Asset',
                       labels={'timestamp': 'Date', 
                              'active_troves': 'Number of Active Troves',
                              'asset': 'Asset Type'})
    
    # Update layout to stack the bars
    fig_troves.update_layout(
        barmode='stack',
        xaxis_title='Date',
        yaxis_title='Number of Active Troves'
    )
    st.plotly_chart(fig_troves)
    

    
    # Add Stability Pool Charts
    st.subheader('Stability Pool Activity')
    
    # Daily Deposits/Withdrawals
    fig_sp_daily = px.bar(stability_pool_data,
                         x='timestamp',
                         y='amount',
                         color='type',
                         title='Daily Stability Pool Deposits and Withdrawals',
                         labels={'timestamp': 'Date', 
                                'amount': 'USDM Amount',
                                'type': 'Action'},
                         color_discrete_map={'Deposit': 'green', 'Withdrawal': 'red'})
    fig_sp_daily.update_layout(
        barmode='relative',
        yaxis_title='USDM Amount (+ Deposits, - Withdrawals)',
        showlegend=True
    )
    st.plotly_chart(fig_sp_daily)
    
    # Total Deposited USDM Over Time
    fig_sp_total = px.line(stability_pool_data,
                          x='timestamp',
                          y='total_deposited',
                          title='Total USDM in Stability Pool Over Time',
                          labels={'timestamp': 'Date',
                                 'total_deposited': 'Total USDM Deposited'})
    st.plotly_chart(fig_sp_total)
    
    # Redemption Analytics Section
    if not redemption_data.empty:
        st.subheader('Redemption Activity')
        
        daily_redemptions = redemption_data.groupby(
            [redemption_data['timestamp'].dt.date, 'asset']
        ).agg({
            'usdm_amount': 'sum',
            'collateral_amount': 'sum'
        }).reset_index()
        
        daily_redemptions['redemption_rate'] = (
            daily_redemptions['collateral_amount'] / daily_redemptions['usdm_amount']
        )
        
        # Redemption Volume Chart
        fig_redemptions = px.bar(daily_redemptions,
                                x='timestamp',
                                y='usdm_amount',
                                color='asset',
                                title='Daily USDM Redemptions by Asset',
                                labels={'timestamp': 'Date',
                                       'usdm_amount': 'USDM Amount Redeemed',
                                       'asset': 'Asset Type'})
        st.plotly_chart(fig_redemptions)
        
        # Redemption Rate Chart
        fig_rates = px.line(daily_redemptions,
                            x='timestamp',
                            y='redemption_rate',
                            color='asset',
                            title='Daily Redemption Rates by Asset',
                            labels={'timestamp': 'Date',
                                   'redemption_rate': 'Collateral/USDM Rate',
                                   'asset': 'Asset Type'})
        st.plotly_chart(fig_rates)

    # Liquidation Analytics Section
    if not liquidation_data.empty:
        st.subheader('Liquidation Activity')
        
        # Liquidation Volume Chart
        fig_liquidations = px.bar(liquidation_data,
                                 x='timestamp',
                                 y='debt',
                                 color='asset',
                                 pattern_shape='type',
                                 title='Daily Liquidation Volume by Asset',
                                 labels={'timestamp': 'Date',
                                        'debt': 'USDM Debt Liquidated',
                                        'asset': 'Asset Type',
                                        'type': 'Liquidation Type'})
        st.plotly_chart(fig_liquidations)
        
        # Liquidation Collateral Chart
        fig_liquidation_collateral = px.bar(liquidation_data,
                                           x='timestamp',
                                           y='collateral',
                                           color='asset',
                                           pattern_shape='type',
                                           title='Daily Collateral Liquidated by Asset',
                                           labels={'timestamp': 'Date',
                                                  'collateral': 'Collateral Amount Liquidated',
                                                  'asset': 'Asset Type',
                                                  'type': 'Liquidation Type'})
        st.plotly_chart(fig_liquidation_collateral)

except Exception as e:
    st.error(f"Error fetching or displaying data: {str(e)}")


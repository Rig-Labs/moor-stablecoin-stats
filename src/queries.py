# GraphQL queries for Fluid Analytics

TOTAL_SUPPLY_QUERY = """
    query {
        USDF_TotalSupplyEvent(
            order_by: {timestamp: asc}
        ) {
            amount
            timestamp
        }
    }
"""

MINT_BURN_QUERIES = {
    "mint": """
        query {
            USDF_Mint(
                order_by: {timestamp: asc}
            ) {
                amount
                timestamp
            }
        }
    """,
    "burn": """
        query {
            USDF_Burn(
                order_by: {timestamp: asc}
            ) {
                amount
                timestamp
            }
        }
    """
}

TROVE_EVENTS_QUERY = """
    query {
        open: BorrowOperations_OpenTroveEvent(order_by: {timestamp: asc}) {
            identity
            asset
            timestamp
        }
        close: BorrowOperations_CloseTroveEvent(order_by: {timestamp: asc}) {
            identity
            asset
            timestamp
        }
        liquidation_full: TroveManager_TroveFullLiquidationEvent(order_by: {timestamp: asc}) {
            identity
            asset
            timestamp
        }
    }
"""

FPT_STAKING_QUERY = """
    query {
        stakes: FPTStaking_StakeEvent(order_by: {timestamp: asc}) {
            amount
            timestamp
        }
        unstakes: FPTStaking_UnstakeEvent(order_by: {timestamp: asc}) {
            amount
            timestamp
        }
    }
"""

STABILITY_POOL_QUERY = """
    query {
        deposits: StabilityPool_ProvideToStabilityPoolEvent(order_by: {timestamp: asc}) {
            amount
            timestamp
        }
        withdrawals: StabilityPool_WithdrawFromStabilityPoolEvent(order_by: {timestamp: asc}) {
            amount
            timestamp
        }
    }
"""

REDEMPTION_QUERY = """
    query {
        TroveManager_RedemptionEvent(order_by: {timestamp: asc}) {
            asset
            usdf_amount
            collateral_amount
            collateral_price
            timestamp
        }
    }
"""

LIQUIDATION_QUERY = """
    query {
        full: TroveManager_TroveFullLiquidationEvent(order_by: {timestamp: asc}) {
            asset
            debt
            collateral
            timestamp
        }
        partial: TroveManager_TrovePartialLiquidationEvent(order_by: {timestamp: asc}) {
            asset
            remaining_debt
            remaining_collateral
            timestamp
        }
    }
""" 
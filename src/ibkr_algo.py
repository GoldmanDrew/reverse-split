from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional

from ib_insync import IB, MarketOrder, Stock
from ib_insync.contract import Contract

from .trading import TradeCandidate


@dataclass
class BorrowQuote:
    ticker: str
    fee_rate: Optional[float]


class IBBorrowCostProvider:
    """Fetches borrow/fee rates from IBKR's shortable data feed."""

    def __init__(self, ib: IB):
        self.ib = ib

    @classmethod
    def from_env(cls) -> "IBBorrowCostProvider":
        ib = connect_from_env()
        return cls(ib)

    def borrow_rate(self, ticker: str) -> Optional[float]:
        contract = Stock(ticker, "SMART", "USD")
        self.ib.qualifyContracts(contract)
        ticker_data = self.ib.reqMktData(contract, genericTickList="236", snapshot=True)
        self.ib.sleep(1.5)

        fee = None
        if ticker_data and ticker_data.shortableShares:
            fee = ticker_data.shortableShares[0].feeRate
        return fee


def connect_from_env() -> IB:
    host = os.environ.get("IBKR_HOST", "127.0.0.1")
    port = int(os.environ.get("IBKR_PORT", "7497"))
    client_id = int(os.environ.get("IBKR_CLIENT_ID", "18"))

    ib = IB()
    ib.connect(host, port, clientId=client_id)
    return ib


class IBKRAlgoTrader:
    """Places equal-weight short orders for selected candidates."""

    def __init__(self, ib: IB):
        self.ib = ib

    @classmethod
    def from_env(cls) -> "IBKRAlgoTrader":
        ib = connect_from_env()
        return cls(ib)

    def _share_quantity(self, notional: float, price: float) -> int:
        if price <= 0:
            return 0
        return max(int(notional // price), 0)

    def execute_equal_weight_shorts(
        self, candidates: List[TradeCandidate], total_notional: float
    ) -> List[Contract]:
        if not candidates or total_notional <= 0:
            return []

        per_name = total_notional / len(candidates)
        contracts: List[Contract] = []

        for c in candidates:
            qty = self._share_quantity(per_name, c.price)
            if qty <= 0:
                continue

            contract = Stock(c.ticker, "SMART", "USD")
            self.ib.qualifyContracts(contract)
            order = MarketOrder("SELL", qty)
            order.algoStrategy = "Adaptive"
            order.algoParams = []
            self.ib.placeOrder(contract, order)
            contracts.append(contract)

        return contracts


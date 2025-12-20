from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, List, Optional, Protocol

from . import price


class BorrowCostProvider(Protocol):
    def borrow_rate(self, ticker: str) -> Optional[float]:
        ...


@dataclass
class TradeCandidate:
    ticker: str
    price: float
    ratio_new: Optional[int]
    ratio_old: Optional[int]
    filed_at: str
    effective_date: Optional[str]
    potential_profit: Optional[float]
    borrow_rate: Optional[float] = None


class ReverseSplitStrategy:
    """Applies trading rules on top of the reverse-split screener output."""

    def __init__(
        self,
        compliance_threshold: float = 1.15,
        consecutive_days: int = 2,
        max_borrow_rate: float = 0.25,
    ) -> None:
        self.compliance_threshold = compliance_threshold
        self.consecutive_days = max(1, consecutive_days)
        self.max_borrow_rate = max_borrow_rate

    def _is_pre_effective(self, effective_date: Optional[str]) -> bool:
        if not effective_date:
            return True
        try:
            eff_dt = datetime.fromisoformat(effective_date).date()
        except ValueError:
            return True
        return eff_dt >= date.today()

    def _above_compliance(self, ticker: str) -> bool:
        closes = price.fetch_recent_closes(ticker, days=self.consecutive_days + 2)
        if len(closes) < self.consecutive_days:
            return False
        tail = closes[-self.consecutive_days :]
        return all(px >= self.compliance_threshold for px in tail)

    def select_candidates(
        self,
        records: Iterable[dict],
        borrow_cost_provider: Optional[BorrowCostProvider] = None,
    ) -> List[TradeCandidate]:
        picks: List[TradeCandidate] = []

        for record in records:
            if record.get("rejection_reason"):
                continue

            ticker = record.get("ticker") or ""
            price_val = record.get("price")
            if not ticker or price_val is None:
                continue

            if not self._is_pre_effective(record.get("effective_date")):
                continue

            if self._above_compliance(ticker):
                continue

            borrow_rate = None
            if borrow_cost_provider:
                borrow_rate = borrow_cost_provider.borrow_rate(ticker)
                if borrow_rate is not None and borrow_rate > self.max_borrow_rate:
                    continue

            picks.append(
                TradeCandidate(
                    ticker=ticker,
                    price=float(price_val),
                    ratio_new=record.get("ratio_new"),
                    ratio_old=record.get("ratio_old"),
                    filed_at=record.get("filed_at", ""),
                    effective_date=record.get("effective_date"),
                    potential_profit=record.get("potential_profit"),
                    borrow_rate=borrow_rate,
                )
            )

        return picks


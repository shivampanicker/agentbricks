from typing import Any, Dict
from dataclasses import dataclass


class LLMClient:
    """Pluggable LLM client abstraction. Implement call() to integrate vendor-specific inference."""

    def call(self, prompt: str, **kwargs) -> str:
        raise NotImplementedError


@dataclass
class AgentContext:
    metadata: Dict[str, Any]


class BaseAgent:
    name: str = "base"

    def __init__(self, llm: LLMClient | None = None):
        self.llm = llm

    def run(self, payload: Dict[str, Any], context: AgentContext | None = None) -> Dict[str, Any]:
        raise NotImplementedError


class TriageAgent(BaseAgent):
    name = "triage"

    def run(self, payload: Dict[str, Any], context: AgentContext | None = None) -> Dict[str, Any]:
        claim_amount = float(payload.get("claim_amount", 0.0))
        severity = str(payload.get("severity_level", "Low"))
        status = str(payload.get("claim_status", "Open"))

        severity_bonus = {"Critical": 30, "High": 20, "Medium": 10}.get(severity, 5)
        status_bonus = 5 if status in {"Open", "In Progress"} else 0
        score = claim_amount / 1000.0 + severity_bonus + status_bonus

        band = "P4"
        if score >= 60:
            band = "P1"
        elif score >= 40:
            band = "P2"
        elif score >= 20:
            band = "P3"

        return {
            "priority_score": float(round(score, 2)),
            "priority_band": band,
        }


class FraudAgent(BaseAgent):
    name = "fraud"

    def run(self, payload: Dict[str, Any], context: AgentContext | None = None) -> Dict[str, Any]:
        claims_count = int(payload.get("lifetime_claims_count", 0))
        claim_amount = float(payload.get("claim_amount", 0.0))
        witness_count = int(payload.get("witness_count", 0))
        photos_taken = int(payload.get("photos_taken", 0))
        days_to_report = int(payload.get("days_to_report", 0))
        premium_cov_ratio = float(payload.get("premium_coverage_ratio", 0.0))
        state = str(payload.get("state", ""))

        score = 0
        score += 10 if claims_count >= 3 else 0
        score += 15 if claim_amount >= 50000 else 0
        score += 10 if (witness_count == 0 and photos_taken < 3) else 0
        score += 8 if days_to_report > 14 else 0
        score += 5 if premium_cov_ratio < 0.005 else 0
        score += 5 if state in {"FL", "LA", "TX"} else 0

        band = "Low"
        if score >= 35:
            band = "High"
        elif score >= 20:
            band = "Medium"

        return {
            "fraud_risk_score": int(score),
            "fraud_risk_band": band,
        }


class SettlementAgent(BaseAgent):
    name = "settlement"

    def run(self, payload: Dict[str, Any], context: AgentContext | None = None) -> Dict[str, Any]:
        severity = str(payload.get("severity_level", "Low"))
        amount = float(payload.get("claim_amount", 0.0))
        fraud_score = float(payload.get("fraud_score", payload.get("fraud_risk_score", 0.0)))

        sev_mult = {"Critical": 0.9, "High": 0.7, "Medium": 0.5}.get(severity, 0.3)
        base_reserve = amount * sev_mult
        adj = -0.1 if fraud_score >= 70 else (-0.05 if fraud_score >= 40 else 0.0)
        reserve = float(round(base_reserve * (1 + adj), 2))
        suggested = float(round(reserve * 0.85, 2))

        return {
            "reserve_amount": reserve,
            "suggested_settlement": suggested,
        }


class RoutingAgent(BaseAgent):
    name = "routing"

    def run(self, payload: Dict[str, Any], context: AgentContext | None = None) -> Dict[str, Any]:
        amount = float(payload.get("claim_amount", 0.0))
        severity = str(payload.get("severity_level", "Low"))

        if amount < 2000:
            channel = "Straight-Through Processing"
        elif severity in {"Low", "Medium"}:
            channel = "Adjuster"
        else:
            channel = "Senior Adjuster"

        if channel == "Straight-Through Processing":
            action = "Auto-approve if checks pass"
        elif severity == "Critical":
            action = "Escalate to senior adjuster"
        elif severity == "High":
            action = "Expedite investigation"
        else:
            action = "Standard processing"

        return {
            "channel": channel,
            "recommended_action": action,
        }

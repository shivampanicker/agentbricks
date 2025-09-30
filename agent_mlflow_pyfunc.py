import mlflow
import mlflow.pyfunc
from typing import Any, Dict
from agentbricks import TriageAgent, FraudAgent, SettlementAgent, RoutingAgent, BaseAgent


class AgentPyFuncModel(mlflow.pyfunc.PythonModel):
    def __init__(self, agent: BaseAgent):
        self.agent = agent

    def predict(self, context, model_input):
        import pandas as pd
        outputs = []
        for _, row in model_input.iterrows():
            payload = row.to_dict()
            result = self.agent.run(payload)
            outputs.append(result)
        return pd.DataFrame(outputs)


def build_agent_model(agent_name: str) -> BaseAgent:
    if agent_name == "triage":
        return TriageAgent()
    if agent_name == "fraud":
        return FraudAgent()
    if agent_name == "settlement":
        return SettlementAgent()
    if agent_name == "routing":
        return RoutingAgent()
    raise ValueError(f"Unknown agent: {agent_name}")

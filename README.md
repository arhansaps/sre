I would ask:

How does retrieval actually improve RCA?

What would make this genuinely exceptional

Given your background, I'd integrate OpenTelemetry deeply.

Right now:

RAG over historical incidents

is good.

But this is stronger:

Agent traverses OpenTelemetry traces,
identifies the first failing span,
maps dependency chains,
correlates logs and metrics,
then retrieves similar historical incidents.

Diagnosed root cause correctly in 8/10 multi-service failure scenarios using trace correlation and historical incident retrieval.


What guardrails exist?
Can the model deploy arbitrary commands?
What actions are allowed?
How are approvals handled?
How do you prevent hallucinated remediation?

If your answer is:

"The agent can only invoke predefined tools and all actions are audited."

then great.

If your answer is:

"Claude decides what kubectl commands to run."

then that's a red flag.

think about this



# Incident Response Autopilot

> An LLM-powered SRE agent that detects production incidents, traces root cause across microservices, and auto-generates remediation runbooks and postmortems — without waking up a human at 3am.

## What it does

1. **Detects** — Consumes alerts from Prometheus, CloudWatch, and PagerDuty via Kafka
2. **Diagnoses** — A LangGraph ReAct agent traces root cause across distributed service spans using RAG over historical incidents
3. **Remediates** — Executes safe corrective actions (pod restart, rollback, scale) with audit logging
4. **Documents** — Auto-generates blameless postmortems and posts a summary to Slack

## Architecture

```
Production signals (CloudWatch / Prometheus / PagerDuty)
        ↓
Kafka event bus → TimescaleDB (metrics) + Pinecone (log embeddings)
        ↓
LangGraph Supervisor Agent
    ├── RCA Agent         (traces root cause via ReAct + RAG)
    ├── Remediation Agent (executes safe corrective actions)
    └── Postmortem Agent  (generates blameless doc)
        ↓
Slack alert + S3 postmortem PDF + Audit log
```

## Stack

| Layer | Technology |
|---|---|
| Fake microservices | FastAPI + OpenTelemetry |
| Streaming | Apache Kafka |
| Metrics | Prometheus + Grafana |
| Time-series | TimescaleDB |
| Vector search | Pinecone |
| Agent framework | LangGraph + LangChain |
| LLM | Claude (Anthropic) |
| Agent tracing | LangSmith |
| Cloud | AWS ECS + S3 + CloudWatch |
| State | Redis |

## Quick start

### Prerequisites
- Docker + Docker Compose
- Python 3.11+
- 8GB RAM minimum

### 1. Start the full local stack

```bash
docker compose up -d
```

Services available:
- Order service: http://localhost:8001
- Payment service: http://localhost:8002
- Inventory service: http://localhost:8003
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)

### 2. Set up environment

```bash
cp .env.example .env
# Fill in ANTHROPIC_API_KEY at minimum
```

### 3. Install agent dependencies

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 4. Send test traffic

```bash
python scripts/chaos.py load
```

### 5. Inject a failure

```bash
python scripts/chaos.py spike-payments   # 80% payment failures
python scripts/chaos.py slow-inventory   # 3s latency on inventory
python scripts/chaos.py cascade          # realistic multi-service incident
```

### 6. Watch the agent respond (week 3+)

```bash
python -m agents.run
```

### 7. Recover

```bash
python scripts/chaos.py recover
```

## Demo scenarios

| Scenario | Command | What fires |
|---|---|---|
| Payment gateway down | `chaos.py spike-payments` | HighPaymentFailureRate alert |
| Inventory slow | `chaos.py slow-inventory` | HighOrderLatency alert |
| Cascade failure | `chaos.py cascade` | Multiple alerts, agent must correlate |

## Results

| Metric | Human SRE | Autopilot |
|---|---|---|
| Mean time to detect | ~5 min | ~30s |
| Mean time to diagnose | ~20 min | ~90s |
| Postmortem draft | ~60 min | ~45s |

*Measured across 10 scripted failure scenarios in week 7 eval suite.*

## Project structure

```
incident-autopilot/
├── services/               # Fake breakable microservices
│   ├── order-service/
│   ├── payment-service/    # Chaos injection via FAILURE_RATE env var
│   └── inventory-service/
├── agents/                 # LangGraph agent code (week 3+)
│   ├── supervisor.py
│   ├── rca_agent.py
│   ├── remediation_agent.py
│   └── postmortem_agent.py
├── pipeline/               # Kafka consumers + data normalisation (week 2+)
├── infra/                  # Prometheus, Grafana, OTel configs
├── scripts/
│   └── chaos.py            # Incident injection tool
└── tests/                  # Eval suite (week 7)
```

## What I learned building this

- Multi-agent orchestration with LangGraph (supervisor + specialist pattern)
- ReAct prompting for tool-use reasoning chains
- RAG over distributed trace data with Pinecone
- Kafka event streaming and schema design
- OpenTelemetry instrumentation across microservices
- Safe agentic actions with guardrails and audit logging
- AWS ECS Fargate deployment and CloudWatch integration

---

Built as a 8-week portfolio project to demonstrate real-world LLM agent engineering.

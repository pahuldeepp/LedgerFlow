🚀 LedgerFlow
Distributed Event-Driven Financial Ledger Platform

LedgerFlow is a cloud-native, event-driven financial ledger system built using Go, Python, Rails, React, Kafka, Kubernetes, and Terraform.

It demonstrates production-grade architecture patterns including:

CQRS (Command Query Responsibility Segregation)

Outbox Pattern

Event-Driven Microservices

Kubernetes (EKS) Deployment

Terraform Infrastructure as Code

OAuth2 + IRSA Authentication

Observability with Prometheus & Grafana

🏗 Architecture Overview

LedgerFlow is designed as a distributed microservices platform:

Frontend (React)
        │
        ▼
admin-api (Rails)
        │
        ▼
ledger-go (Write Service)
        │
        ▼
Postgres (Write Model) + Outbox
        │
        ▼
Kafka (MSK)
        │
        ▼
read-model-builder (Projection Service)
        │
        ▼
Postgres (Read Model Tables)
🧠 Core Concepts
1️⃣ CQRS (Command Query Responsibility Segregation)

LedgerFlow separates write operations from read operations:

Write Side

Managed by ledger-go

Append-only ledger

Normalized schema

Strict invariant enforcement

Outbox pattern for reliable event publishing

Read Side

Built asynchronously by read-model-builder

Denormalized projection tables

Optimized for dashboard queries

Eventually consistent

🔄 Event Flow

Transaction request → admin-api

gRPC call → ledger-go

Transaction written to ledger

Event written to outbox table

Outbox worker publishes to Kafka

read-model-builder consumes event

Projection tables updated

Dashboard reflects updated state

🧩 Services
🟢 ledger-go (Go)

Primary transaction service.

Responsibilities:

Validate and post transactions

Maintain append-only ledger

Publish domain events

Expose HTTP + gRPC APIs

Prometheus metrics

Circuit breaker + middleware

🟡 billing (Python)

Kafka consumer service.

Responsibilities:

Consume transaction events

Process billing logic

Maintain billing state

Emit metrics

🔵 read-model-builder (Go)

Projection service.

Responsibilities:

Consume Kafka events

Maintain denormalized read tables

Track consumer lag

Expose health and metrics

🟣 admin-api (Rails)

Gateway API.

Responsibilities:

Authentication

Role-based access

gRPC communication with ledger-go

Serve frontend data

🖥 Frontend (React + TypeScript)

Billing Admin Dashboard.

Features:

Health monitoring

Outbox status

DLQ monitoring

Account balances

Role-based access

TanStack Query

Redux state management

☸ Kubernetes Deployment

LedgerFlow is deployed to Amazon EKS using Helm.

Each service has:

Deployment

Service

ServiceAccount

Ingress (NGINX)

HPA (Horizontal Pod Autoscaler)

Authentication is handled using:

OAuth2 Proxy

AWS IAM Roles for Service Accounts (IRSA)

AWS Secrets Manager

🏗 Infrastructure (Terraform)

Infrastructure is provisioned using Terraform:

VPC

EKS Cluster

RDS (Postgres)

MSK (Kafka)

IAM roles

Security groups

Remote state backend recommended (S3 + DynamoDB locking).

📊 Observability

LedgerFlow includes:

Prometheus (metrics collection)

Grafana (dashboards)

Loki (logs)

Promtail (log shipping)

Health endpoints across services

🔐 Security

OAuth2 authentication

Role-based authorization

AWS IAM integration

Secrets stored in AWS Secrets Manager

IRSA for Kubernetes service access

📦 Repository Structure
Backend/
  services/
    ledger-go/
    billing/
    read-model-builder/
  admin-api/

frontend/
  billing-admin-dashboard-new/

helm/
  ledger-go/
  billing/
  frontend/
  admin-api/

terraform/
  msk/

eks/
  cluster/
  vpc/
  bootstrap/
⚙️ Running Locally (Development)
Backend
docker-compose up --build
Frontend
cd frontend/billing-admin-dashboard-new
npm install
npm run dev
🌍 Production Deployment

Provision infrastructure:

terraform apply

Deploy services:

helm upgrade --install ledger-go ./helm/ledger-go
helm upgrade --install billing ./helm/billing
helm upgrade --install read-model-builder ./helm/read-model-builder
helm upgrade --install frontend ./helm/frontend
📈 Design Principles

Financial correctness first

Append-only ledger model

Asynchronous event propagation

Eventually consistent read model

Infrastructure as Code

Cloud-native deployment

Observability by default

🧪 Future Improvements

Separate read replica database

Redis caching layer

Saga orchestrator

Multi-tenant support

Elasticsearch read projections

GitOps deployment (ArgoCD)

🎯 Why This Project Matters

LedgerFlow demonstrates:

Distributed systems design

Event-driven architecture

Financial data integrity

Kubernetes production deployment

Infrastructure automation

Full-stack integration

This project simulates a Stripe-like distributed financial ledger built with modern cloud-native tooling.

📜 License

MIT License

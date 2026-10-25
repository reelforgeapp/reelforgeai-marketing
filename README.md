# ReelForge Marketing Engine

Automated affiliate marketing system for recruiting content creators who review AI video tools.

> **ReelForge Partner Intelligence (RPI)** — influencer discovery, qualification, partnership CRM, controlled
> outreach, attribution and profit reporting — lives in `rpi/` and is served from this app at `/rpi/ui` and
> `/rpi/api/v1`. Start with [docs/rpi/README.md](docs/rpi/README.md). Schema: `python -m scripts.rpi_migrate`.
> Tests: `pip install -r requirements-dev.txt && pytest`.

## Features

- **Multi-Platform Discovery**: YouTube, Instagram, TikTok creator discovery
- **Contact emails**: from the channel description returned by the YouTube Data API at discovery; creators' own websites are checked when known. YouTube pages are never scraped (YouTube Terms of Service and API Developer Policies)
- **Email Verification**: Clearout/Hunter.io integration
- **Automated Outreach**: Multi-step email sequences with personalization. Every email carries the sender, a postal address and a one-click unsubscribe link; sending is refused until `COMPANY_POSTAL_ADDRESS` is set
- **Idempotency Protection**: Prevents duplicate sends
- **GDPR Compliance**: Consent logging, data retention, right to deletion
- **Real-time Webhooks**: Brevo event tracking with HMAC validation

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Render ($21/mo)                       │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │            Docker Container ($7/mo)               │  │
│  │                                                   │  │
│  │  FastAPI (webhooks + triggers)                    │  │
│  │  Celery Worker (background tasks)                 │  │
│  │  Celery Beat (scheduled jobs)                     │  │
│  └──────────────────────────────────────────────────┘  │
│                         │                               │
│           ┌─────────────┼─────────────┐                │
│           ▼             ▼             ▼                │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐       │
│  │ PostgreSQL │  │   Redis    │  │ (external) │       │
│  │  ($7/mo)   │  │  ($7/mo)   │  │   APIs     │       │
│  └────────────┘  └────────────┘  └────────────┘       │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Prerequisites

- Render account (https://render.com)
- YouTube Data API key
- Brevo account (free tier works)
- Optional: Clearout account for email verification

### 2. Deploy to Render

```bash
# Clone repository
git clone <your-repo-url>
cd reelforge-marketing

# Push to GitHub
git push origin main
```

In Render Dashboard:
1. Create **PostgreSQL** ($7/mo starter)
2. Create **Redis** ($7/mo starter)
3. Create **Web Service** from Docker ($7/mo starter)
4. Connect to your GitHub repo

### 3. Run Database Migration

```bash
# Get database URL from Render
psql $DATABASE_URL

# Run migrations
\i database/001_marketing_tables.sql
\i database/002_v2_schema.sql
```

### 4. Configure Environment Variables

In Render Web Service → Environment:

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | ✅ | Auto-set by Render |
| `REDIS_URL` | ✅ | Copy from Redis service |
| `YOUTUBE_API_KEY` | ✅ | Google Cloud Console |
| `BREVO_API_KEY` | ✅ | Brevo dashboard |
| `BREVO_WEBHOOK_SECRET` | ✅ | Random string (24+ chars); Brevo sends it with every webhook (see step 5). Webhooks are rejected without it, except when `ENVIRONMENT=development` |
| `ADMIN_API_KEY` | ✅ | Random string (24+ chars). Required in the `X-Admin-Key` header for `/trigger/*`, `/keywords*` and `/tasks/*`. While unset (or left as `change-me-in-production`) those endpoints refuse every request |
| `COMPANY_POSTAL_ADDRESS` | ✅ (web + worker) | Physical postal address for the email footer (street address, USPS PO box or private mailbox). **No outreach is sent while this is empty** |
| `PUBLIC_BASE_URL` | Optional | Base URL for unsubscribe links (default `https://reelforgeai-marketing.onrender.com`) |
| `CLEAROUT_API_KEY` | Recommended | For email verification |
| `SENTRY_DSN` | Optional | Error monitoring |

### 5. Configure Brevo Webhook

Brevo does not sign webhook payloads. It authenticates with a bearer token, credentials in the notify URL, or
custom headers ([Brevo: secured webhooks](https://developers.brevo.com/docs/secured-webhooks)). The app accepts
`BREVO_WEBHOOK_SECRET` through any of these:

- **Credentials in the URL** (simplest): set the notify URL to
  `https://brevo:<BREVO_WEBHOOK_SECRET>@your-app.onrender.com/webhooks/brevo` (any username; the password is the secret).
- **Bearer token**: create or update the webhook through the Brevo API with `"auth": {"type": "bearer", "token": "<BREVO_WEBHOOK_SECRET>"}`.
- **Custom header**: through the API, add the header `X-Brevo-Webhook-Token: <BREVO_WEBHOOK_SECRET>`.

Enable the delivered, opened, clicked, hard/soft bounce, unsubscribed and spam events.

**Order matters when upgrading:** set `BREVO_WEBHOOK_SECRET` in Render and update the Brevo webhook first, then
deploy. Otherwise bounce and unsubscribe events are rejected until both sides match.

### 6. Verify Deployment

```bash
# Health check
curl https://your-app.onrender.com/health

# Status (shows Redis connection)
curl https://your-app.onrender.com/status

# Trigger discovery manually (admin key required)
curl -X POST -H "X-Admin-Key: $ADMIN_API_KEY" https://your-app.onrender.com/trigger/youtube-discovery
```

## Scheduled Jobs (Celery Beat)

| Job | Schedule | Purpose |
|-----|----------|---------|
| YouTube Discovery | Daily 2 AM EST | Find new creators |
| Apify Discovery | Daily 3 AM EST | Instagram/TikTok |
| Email Extraction | Every 6 hours | Check creators' own websites for contact info (never YouTube pages) |
| Email Verification | Every 6 hours | Validate emails |
| Sequence Processing | Every 15 min | Send emails |
| Auto-Enrollment | Every 3 hours | Start sequences |
| YouTube data scrub | Daily 4:45 AM | Clear YouTube statistics older than 30 days (API policy) |

## API Endpoints

### Health & Status
- `GET /health` - Health check
- `GET /status` - System status + stats

### Unsubscribe (public, signed links)
- `GET /unsubscribe/{prospect_id}?t=...` - link in every outreach email
- `POST /unsubscribe/{prospect_id}?t=...` - one-click unsubscribe from mail clients

### Webhooks
- `POST /webhooks/brevo` - Email event webhooks (authenticated with `BREVO_WEBHOOK_SECRET`, see step 5)

Every endpoint below requires the header `X-Admin-Key: <ADMIN_API_KEY>`; requests without it get `401`.

### Manual Triggers
- `POST /trigger/youtube-discovery`
- `POST /trigger/email-extraction`
- `POST /trigger/email-verification`
- `POST /trigger/sequence-processing`
- `POST /trigger/auto-enroll`
- `POST /trigger/brevo-sync`, `POST /trigger/brevo-sync-full`
- `POST /trigger/deliverability-check`
- `POST /trigger/keyword-trends`

### Keywords
- `GET /keywords`, `POST /keywords`, `PATCH /keywords/{id}`, `DELETE /keywords/{id}`
- `GET /keywords/trends/{keyword}`, `POST /keywords/compare`

### Task Status
- `GET /tasks/{task_id}` - Check Celery task status

### GDPR Admin
- `DELETE /admin/gdpr/delete?email=xxx&admin_key=xxx`

## Cost Breakdown

| Service | Monthly Cost |
|---------|-------------|
| Render Web Service | $7 |
| Render PostgreSQL | $7 |
| Render Redis | $7 |
| Apify | $49 |
| Brevo | $0 (free) |
| Reditus | $69 |
| Clearout | $25 |
| **Total** | **$164/mo** |

## Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install Playwright
playwright install chromium

# Copy environment template
cp .env.example .env
# Edit .env with your values

# Start Redis (Docker)
docker run -d -p 6379:6379 redis

# Start PostgreSQL (Docker)
docker run -d -p 5432:5432 -e POSTGRES_DB=reelforge postgres

# Run migrations
psql postgresql://postgres@localhost:5432/reelforge < database/001_marketing_tables.sql
psql postgresql://postgres@localhost:5432/reelforge < database/002_v2_schema.sql

# Start Celery worker
celery -A celery_config worker --loglevel=info

# Start Celery beat (separate terminal)
celery -A celery_config beat --loglevel=info

# Start FastAPI (separate terminal)
uvicorn app.main:app --reload
```

## Monitoring

### Celery Flower (Optional)
```bash
celery -A celery_config flower --port=5555
# Access at http://localhost:5555
```

### Sentry
Configure `SENTRY_DSN` for error tracking.

### Logs
View in Render Dashboard → Logs

## Support

- Render: support@render.com
- Brevo: api@brevo.com
- Apify: support@apify.com

## License

Proprietary - Lydell Security

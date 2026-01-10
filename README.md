# ReelForge Marketing Engine

Automated affiliate marketing system for recruiting content creators who review AI video tools.

## Features

- **Multi-Platform Discovery**: YouTube, Instagram, TikTok creator discovery
- **Smart Email Extraction**: Hybrid HTTP + Playwright approach
- **Email Verification**: Clearout/Hunter.io integration
- **Automated Outreach**: Multi-step email sequences with personalization
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
| `BREVO_WEBHOOK_SECRET` | ✅ | Generate and set in Brevo |
| `CLEAROUT_API_KEY` | Recommended | For email verification |
| `SENTRY_DSN` | Optional | Error monitoring |

### 5. Configure Brevo Webhook

1. Go to Brevo → Settings → Webhooks
2. Add URL: `https://your-app.onrender.com/webhooks/brevo`
3. Enable all events
4. Set signing secret (copy to `BREVO_WEBHOOK_SECRET`)

### 6. Verify Deployment

```bash
# Health check
curl https://your-app.onrender.com/health

# Status (shows Redis connection)
curl https://your-app.onrender.com/status

# Trigger discovery manually
curl -X POST https://your-app.onrender.com/trigger/youtube-discovery
```

## Scheduled Jobs (Celery Beat)

| Job | Schedule | Purpose |
|-----|----------|---------|
| YouTube Discovery | Daily 2 AM EST | Find new creators |
| Apify Discovery | Daily 3 AM EST | Instagram/TikTok |
| Email Extraction | Every 6 hours | Get contact info |
| Email Verification | Every 6 hours | Validate emails |
| Sequence Processing | Every 15 min | Send emails |
| Auto-Enrollment | Every 3 hours | Start sequences |
| Data Purge | Daily 4 AM EST | GDPR cleanup |

## API Endpoints

### Health & Status
- `GET /health` - Health check
- `GET /status` - System status + stats

### Webhooks
- `POST /webhooks/brevo` - Email event webhooks

### Manual Triggers
- `POST /trigger/youtube-discovery`
- `POST /trigger/email-extraction`
- `POST /trigger/email-verification`
- `POST /trigger/sequence-processing`
- `POST /trigger/auto-enroll`

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

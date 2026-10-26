#!/usr/bin/env python3
"""
Create RPI's Stripe webhook endpoint and store its signing secret in Render.

Run it yourself (it uses your live Stripe key and writes a secret); it never
prints a key or secret:

    python3 scripts/rpi_connect_stripe.py            # create endpoint, store secret, redeploy RPI
    python3 scripts/rpi_connect_stripe.py --dry-run  # show what it would do
    python3 scripts/rpi_connect_stripe.py --verify   # check the endpoint and RPI's signature check end to end

What it does:
  1. reads the Render API key (RENDER_API_KEY, or the RENDER_API_KEY= line of the file
     named by RENDER_ENV_FILE, default ~/Projects/reelforge-ai/.env.analysis) and the
     product's STRIPE_SECRET_KEY from the reelforge-api service's env (Render API);
  2. creates a Stripe webhook endpoint for RPI with the events RPI processes
     (a second endpoint; the product's own endpoint is untouched). If one for
     this URL already exists it stops: Stripe only returns a secret at creation,
     so roll or reveal that endpoint's secret in the Stripe dashboard instead;
  3. puts the endpoint's signing secret into RPI_STRIPE_WEBHOOK_SECRET on the
     RPI web and worker services and redeploys the web service.

--verify checks the endpoint in Stripe and sends RPI an event signed with the
stored secret (a type RPI does not process, so it is stored as ignored and never
reaches the ledger), plus a wrongly signed one that must be rejected.
See docs/rpi/DEPLOYMENT_RUNBOOK.md section 7.
"""

import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

RPI_URL = "https://reelforgeai-marketing.onrender.com/rpi/api/v1/webhooks/stripe"
EVENTS = [
    "invoice.paid", "invoice.payment_succeeded", "charge.refunded", "charge.dispute.created",
    "charge.dispute.closed", "customer.subscription.created", "customer.subscription.updated",
    "customer.subscription.deleted",
]
PRODUCT_API = "reelforge-api"
RPI_SERVICES = {"web": "srv-d5hae9ali9vc73ac9cc0", "worker": "srv-d5hu9d2li9vc73an3dq0"}
ENV_FILE = os.environ.get("RENDER_ENV_FILE", os.path.expanduser("~/Projects/reelforge-ai/.env.analysis"))


def render(method, path, body=None):
    key = os.environ.get("RENDER_API_KEY") or next(
        l.split("=", 1)[1].strip().strip('"').strip("'") for l in open(ENV_FILE) if l.startswith("RENDER_API_KEY="))
    req = urllib.request.Request("https://api.render.com/v1" + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {key}", "Accept": "application/json",
                                          "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=90) as r:
        raw = r.read()
        return json.loads(raw) if raw else {}


def stripe(method, path, secret_key, form=None):
    data = urllib.parse.urlencode(form, doseq=True).encode() if form else None
    req = urllib.request.Request("https://api.stripe.com/v1" + path, method=method, data=data,
                                 headers={"Authorization": f"Bearer {secret_key}"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        detail = json.loads(e.read() or b"{}").get("error", {}).get("message", "")
        sys.exit(f"Stripe API error {e.code}: {detail}")


def post_signed(payload: bytes, secret: str):
    """POST a payload to RPI signed exactly the way Stripe signs (t=..., v1=HMAC-SHA256)."""
    t = int(time.time())
    sig = hmac.new(secret.encode(), f"{t}.".encode() + payload, hashlib.sha256).hexdigest()
    req = urllib.request.Request(RPI_URL, method="POST", data=payload,
                                 headers={"Content-Type": "application/json", "Stripe-Signature": f"t={t},v1={sig}"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, (e.read() or b"")[:200].decode(errors="replace")


def verify(stripe_key: str) -> int:
    """Endpoint enabled in Stripe, and RPI verifies an event signed with the stored secret. The test
    event uses a type RPI does not process, so it is stored as ignored and never touches the ledger."""
    ok = True
    endpoints = [w for w in stripe("GET", "/webhook_endpoints?limit=100", stripe_key)["data"] if w["url"] == RPI_URL]
    if not endpoints:
        print(f"FAIL: no Stripe endpoint for {RPI_URL}")
        return 1
    w = endpoints[0]
    missing = sorted(set(EVENTS) - set(w["enabled_events"]))
    print(f"Stripe endpoint {w['id']}: status {w['status']}, livemode {w['livemode']}, "
          f"{len(w['enabled_events'])} events" + (f", missing {missing}" if missing else ""))
    ok &= w["status"] == "enabled" and not missing

    secrets = {name: next((e["envVar"]["value"] for e in render("GET", f"/services/{sid}/env-vars?limit=100")
                           if e["envVar"]["key"] == "RPI_STRIPE_WEBHOOK_SECRET"), None) for name, sid in RPI_SERVICES.items()}
    if not secrets["web"]:
        print("FAIL: RPI_STRIPE_WEBHOOK_SECRET is not set on the RPI web service")
        return 1
    print("RPI_STRIPE_WEBHOOK_SECRET: set on web" + (", same on worker" if secrets["worker"] == secrets["web"] else
                                                      ", DIFFERENT or missing on worker"))
    event = {"id": f"evt_rpi_selftest_{int(time.time())}", "object": "event", "type": "rpi.connection_test",
             "created": int(time.time()), "livemode": False, "data": {"object": {}}}
    status, body = post_signed(json.dumps(event).encode(), secrets["web"])
    accepted = status == 200 and isinstance(body, dict) and body.get("status") == "ignored"
    print(f"Signed test event -> HTTP {status} {body}" + ("  (verified and processed)" if accepted else ""))
    status_bad, _ = post_signed(json.dumps(event).encode(), "whsec_wrong_secret_for_negative_test")
    print(f"Wrongly signed event -> HTTP {status_bad}" + ("  (rejected, as it should be)" if status_bad == 400 else ""))
    ok &= accepted and status_bad == 400
    secrets.clear()
    print("OK: Stripe -> RPI is configured; real events will be verified with this secret." if ok else "FAIL: see above")
    return 0 if ok else 1


def main() -> int:
    dry = "--dry-run" in sys.argv
    api_id = next(x["service"]["id"] for x in render("GET", "/services?limit=100") if x["service"]["name"] == PRODUCT_API)
    stripe_key = next((e["envVar"]["value"] for e in render("GET", f"/services/{api_id}/env-vars?limit=100")
                       if e["envVar"]["key"] == "STRIPE_SECRET_KEY"), None)
    if not stripe_key:
        sys.exit(f"STRIPE_SECRET_KEY not found on {PRODUCT_API}")
    mode = "LIVE" if stripe_key.startswith(("sk_live_", "rk_live_")) else "TEST"
    print(f"Stripe key from {PRODUCT_API}: {mode} mode")
    if "--verify" in sys.argv:
        return verify(stripe_key)

    existing = [w for w in stripe("GET", "/webhook_endpoints?limit=100", stripe_key)["data"] if w["url"] == RPI_URL]
    if existing:
        print(f"An endpoint for {RPI_URL} already exists ({existing[0]['id']}, status {existing[0]['status']}).")
        print("Stripe only returns a signing secret at creation. In the Stripe dashboard open that endpoint, "
              "reveal (or roll) its signing secret, and set RPI_STRIPE_WEBHOOK_SECRET on reelforgeai-marketing.")
        return 1
    if dry:
        print(f"DRY RUN: would create a {mode} endpoint {RPI_URL} for {len(EVENTS)} events, store its secret "
              "on RPI web and worker, and redeploy the RPI web service.")
        return 0

    endpoint = stripe("POST", "/webhook_endpoints", stripe_key, {
        "url": RPI_URL, "enabled_events[]": EVENTS,
        "description": "ReelForge Partner Intelligence: revenue ledger and partner attribution",
    })
    secret = endpoint.get("secret")
    if not secret:
        sys.exit(f"Endpoint {endpoint.get('id')} created but Stripe returned no secret; reveal it in the dashboard.")
    for service in RPI_SERVICES.values():
        render("PUT", f"/services/{service}/env-vars/RPI_STRIPE_WEBHOOK_SECRET", {"value": secret})
    del secret
    render("POST", f"/services/{RPI_SERVICES['web']}/deploys", {"clearCache": "do_not_clear"})
    print(f"Created {mode} endpoint {endpoint['id']} for {len(EVENTS)} events.")
    print("Signing secret stored in RPI_STRIPE_WEBHOOK_SECRET on RPI web and worker (not shown); RPI web is redeploying.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

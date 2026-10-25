-- Migration 004: compliant outreach copy
--
-- Replaces template text that made unverifiable earnings claims ("$500-$2,000/month",
-- "$1,000+ monthly", "hit $1,500 in their first month"), claimed the sender had watched
-- the creator's video, invented scarcity ("spots are filling up") or offered a plan that
-- does not exist ("3 months FREE ReelForge Pro ($150 value)").
--
-- Commission wording is "30% recurring", which is true under both published versions of
-- the affiliate terms. The CAN-SPAM footer (postal address + unsubscribe) is appended in
-- code (services/compliance.py), not stored here.
--
-- Upserts, so it also seeds a fresh database after schema.sql. Idempotent. Rollback: re-run database/schema.sql from v4.13.

BEGIN;

INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES ('youtube_initial', $s$Your {{competitor}} content and the ReelForge AI affiliate program$s$,
$h$<p>Hi {{first_name}},</p>
<p>I came across your {{competitor}} content while looking for creators who cover AI video tools, and I think your audience could be a good fit for ReelForge AI.</p>
<p>ReelForge AI turns a prompt or niche into finished short-form videos, with script, visuals, voiceover and captions, so creators can publish consistently without editing.</p>
<p>Our affiliate program pays a <strong>30% recurring commission</strong> on the subscriptions you refer. Program details and terms: <a href="https://www.reelforgeai.io/become-affiliate">https://www.reelforgeai.io/become-affiliate</a></p>
<p>You can try ReelForge AI for free at <a href="https://reelforgeai.io">https://reelforgeai.io</a>. If it looks like a fit, just reply and I'll answer any questions.</p>
<p>Best,<br>Larry Barksdale<br>ReelForge AI</p>$h$,
$t$Hi {{first_name}},

I came across your {{competitor}} content while looking for creators who cover AI video tools, and I think your audience could be a good fit for ReelForge AI.

ReelForge AI turns a prompt or niche into finished short-form videos, with script, visuals, voiceover and captions, so creators can publish consistently without editing.

Our affiliate program pays a 30% recurring commission on the subscriptions you refer. Program details and terms: https://www.reelforgeai.io/become-affiliate

You can try ReelForge AI for free at https://reelforgeai.io. If it looks like a fit, just reply and I'll answer any questions.

Best,
Larry Barksdale
ReelForge AI$t$,
TRUE)
ON CONFLICT (name) DO UPDATE SET subject_template = EXCLUDED.subject_template,
  html_template = EXCLUDED.html_template, text_template = EXCLUDED.text_template, is_active = TRUE;

INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES ('youtube_followup_1', $s$Following up: ReelForge AI affiliate program$s$,
$h$<p>Hi {{first_name}},</p>
<p>A quick follow-up on my note about the ReelForge AI affiliate program.</p>
<p>If you'd like to see whether it suits your audience, you can join here: <a href="{{affiliate_link}}">{{affiliate_link}}</a>. Commissions are 30% recurring on the subscriptions you refer.</p>
<p>Happy to answer any questions. Just reply to this email.</p>
<p>Best,<br>Larry Barksdale</p>$h$,
$t$Hi {{first_name}},

A quick follow-up on my note about the ReelForge AI affiliate program.

If you'd like to see whether it suits your audience, you can join here: {{affiliate_link}}. Commissions are 30% recurring on the subscriptions you refer.

Happy to answer any questions. Just reply to this email.

Best,
Larry Barksdale$t$,
TRUE)
ON CONFLICT (name) DO UPDATE SET subject_template = EXCLUDED.subject_template,
  html_template = EXCLUDED.html_template, text_template = EXCLUDED.text_template, is_active = TRUE;

INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES ('youtube_followup_2', $s$Closing the loop, {{first_name}}$s$,
$h$<p>Hi {{first_name}},</p>
<p>This is my last note on this, so I won't fill your inbox.</p>
<p>If a 30% recurring commission for recommending an AI short-form video tool is ever useful, the signup link is <a href="{{affiliate_link}}">{{affiliate_link}}</a>.</p>
<p>Either way, thanks for the content you make.</p>
<p>Best,<br>Larry Barksdale</p>$h$,
$t$Hi {{first_name}},

This is my last note on this, so I won't fill your inbox.

If a 30% recurring commission for recommending an AI short-form video tool is ever useful, the signup link is {{affiliate_link}}.

Either way, thanks for the content you make.

Best,
Larry Barksdale$t$,
TRUE)
ON CONFLICT (name) DO UPDATE SET subject_template = EXCLUDED.subject_template,
  html_template = EXCLUDED.html_template, text_template = EXCLUDED.text_template, is_active = TRUE;

INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES ('tiktok_initial', $s$Your AI content and the ReelForge AI affiliate program$s$,
$h$<p>Hi {{first_name}},</p>
<p>I came across your content about AI tools while looking for creators in this space. I'm Larry Barksdale from ReelForge AI, which turns a prompt or niche into finished short-form videos.</p>
<p>Our affiliate program pays a <strong>30% recurring commission</strong> on the subscriptions you refer. Program details and terms: <a href="https://www.reelforgeai.io/become-affiliate">https://www.reelforgeai.io/become-affiliate</a></p>
<p>You can try ReelForge AI for free at <a href="https://reelforgeai.io">https://reelforgeai.io</a>.</p>
<p>Best,<br>Larry Barksdale<br>ReelForge AI</p>$h$,
$t$Hi {{first_name}}, I came across your content about AI tools while looking for creators in this space. I'm Larry Barksdale from ReelForge AI, which turns a prompt or niche into finished short-form videos. Our affiliate program pays a 30% recurring commission on referred subscriptions: https://www.reelforgeai.io/become-affiliate. Try ReelForge AI free: https://reelforgeai.io. Best, Larry Barksdale$t$,
TRUE)
ON CONFLICT (name) DO UPDATE SET subject_template = EXCLUDED.subject_template,
  html_template = EXCLUDED.html_template, text_template = EXCLUDED.text_template, is_active = TRUE;

INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES ('instagram_initial', $s$Your AI content and the ReelForge AI affiliate program$s$,
$h$<p>Hi {{first_name}},</p>
<p>I came across your content about AI tools while looking for creators in this space. I'm Larry Barksdale from ReelForge AI, which turns a prompt or niche into finished short-form videos.</p>
<p>Our affiliate program pays a <strong>30% recurring commission</strong> on the subscriptions you refer. Program details and terms: <a href="https://www.reelforgeai.io/become-affiliate">https://www.reelforgeai.io/become-affiliate</a></p>
<p>You can try ReelForge AI for free at <a href="https://reelforgeai.io">https://reelforgeai.io</a>.</p>
<p>Best,<br>Larry Barksdale<br>ReelForge AI</p>$h$,
$t$Hi {{first_name}}, I came across your content about AI tools while looking for creators in this space. I'm Larry Barksdale from ReelForge AI, which turns a prompt or niche into finished short-form videos. Our affiliate program pays a 30% recurring commission on referred subscriptions: https://www.reelforgeai.io/become-affiliate. Try ReelForge AI free: https://reelforgeai.io. Best, Larry Barksdale$t$,
TRUE)
ON CONFLICT (name) DO UPDATE SET subject_template = EXCLUDED.subject_template,
  html_template = EXCLUDED.html_template, text_template = EXCLUDED.text_template, is_active = TRUE;

INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES ('tiktok_followup', $s$Following up: ReelForge AI affiliate program$s$,
$h$<p>Hi {{first_name}},</p>
<p>A quick follow-up on the ReelForge AI affiliate program. If it suits your audience, you can join here: <a href="{{affiliate_link}}">{{affiliate_link}}</a>.</p>
<p>Best,<br>Larry Barksdale</p>$h$,
$t$Hi {{first_name}}, a quick follow-up on the ReelForge AI affiliate program. If it suits your audience, you can join here: {{affiliate_link}}. Best, Larry Barksdale$t$,
TRUE)
ON CONFLICT (name) DO UPDATE SET subject_template = EXCLUDED.subject_template,
  html_template = EXCLUDED.html_template, text_template = EXCLUDED.text_template, is_active = TRUE;

INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES ('instagram_followup', $s$Following up: ReelForge AI affiliate program$s$,
$h$<p>Hi {{first_name}},</p>
<p>A quick follow-up on the ReelForge AI affiliate program. If it suits your audience, you can join here: <a href="{{affiliate_link}}">{{affiliate_link}}</a>.</p>
<p>Best,<br>Larry Barksdale</p>$h$,
$t$Hi {{first_name}}, a quick follow-up on the ReelForge AI affiliate program. If it suits your audience, you can join here: {{affiliate_link}}. Best, Larry Barksdale$t$,
TRUE)
ON CONFLICT (name) DO UPDATE SET subject_template = EXCLUDED.subject_template,
  html_template = EXCLUDED.html_template, text_template = EXCLUDED.text_template, is_active = TRUE;

-- Sequence step subjects (informational; the sender uses email_templates.subject_template).
UPDATE sequence_templates SET steps = (
  SELECT jsonb_agg(
    CASE (step->>'step')::int
      WHEN 1 THEN step || '{"subject_template": "Your {{competitor}} content and the ReelForge AI affiliate program"}'::jsonb
      WHEN 2 THEN step || '{"subject_template": "Following up: ReelForge AI affiliate program"}'::jsonb
      WHEN 3 THEN step || '{"subject_template": "Closing the loop, {{first_name}}"}'::jsonb
      ELSE step END
    ORDER BY (step->>'step')::int)
  FROM jsonb_array_elements(steps) AS step)
WHERE name = 'youtube_creator';

COMMIT;

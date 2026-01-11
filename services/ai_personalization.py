"""
ReelForge Marketing Engine - AI Personalization Service
Uses Claude to generate personalized outreach emails based on prospect data
"""

import asyncio
import httpx
import structlog
from typing import Optional, Dict, Any

from app.config import get_settings

logger = structlog.get_logger()


class AIPersonalizationService:
    """Generate personalized emails using Claude API."""
    
    ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
    
    def __init__(self):
        self.settings = get_settings()
        self.api_key = self.settings.anthropic_api_key
        self.model = self.settings.anthropic_model
    
    async def generate_personalized_email(
        self,
        prospect: Dict[str, Any],
        video_data: Optional[Dict[str, Any]] = None,
        template_type: str = "initial"
    ) -> Dict[str, str]:
        """
        Generate a personalized email for a prospect.
        
        Args:
            prospect: Dict with full_name, youtube_handle, primary_platform, etc.
            video_data: Optional dict with video_title, description, transcript_snippet
            template_type: 'initial', 'followup_1', or 'followup_2'
        
        Returns:
            Dict with 'subject' and 'body' keys
        """
        if not self.api_key:
            logger.warning("Anthropic API key not configured, using template fallback")
            return self._fallback_template(prospect, template_type)
        
        try:
            prompt = self._build_prompt(prospect, video_data, template_type)
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    self.ANTHROPIC_API_URL,
                    headers={
                        "x-api-key": self.api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json"
                    },
                    json={
                        "model": self.model,
                        "max_tokens": 1024,
                        "messages": [
                            {"role": "user", "content": prompt}
                        ]
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    content = data["content"][0]["text"]
                    return self._parse_email_response(content)
                else:
                    logger.error("Claude API error", status=response.status_code, body=response.text)
                    return self._fallback_template(prospect, template_type)
                    
        except Exception as e:
            logger.error("AI personalization failed", error=str(e))
            return self._fallback_template(prospect, template_type)
    
    def _build_prompt(
        self,
        prospect: Dict[str, Any],
        video_data: Optional[Dict[str, Any]],
        template_type: str
    ) -> str:
        """Build the prompt for Claude."""
        
        first_name = (prospect.get("full_name") or "").split()[0] or "there"
        platform = prospect.get("primary_platform", "youtube")
        handle = prospect.get("youtube_handle") or prospect.get("instagram_handle") or prospect.get("tiktok_handle") or ""
        subscribers = prospect.get("youtube_subscribers") or prospect.get("instagram_followers") or prospect.get("tiktok_followers") or 0
        competitor = (prospect.get("competitor_mentions") or ["AI video tools"])[0]
        
        video_context = ""
        if video_data:
            video_context = f"""
Their recent video:
- Title: {video_data.get('title', 'N/A')}
- Description snippet: {video_data.get('description', 'N/A')[:300]}
- Key topics: {video_data.get('topics', 'AI video tools')}
"""
        
        if template_type == "initial":
            email_goal = "First outreach - build rapport, introduce ReelForge AI affiliate program"
            tone = "warm, genuine, not salesy"
            key_points = """
- Compliment something specific about their content (be genuine, not generic)
- Introduce yourself as Larry Barksdale from ReelForge AI
- Briefly explain ReelForge (AI video platform, 90-second videos from prompts)
- Mention 30% lifetime recurring commissions
- Mention creators earning $1,000+/month
- End with soft CTA (reply to chat more)
"""
        elif template_type == "followup_1":
            email_goal = "Follow-up - provide social proof and affiliate link"
            tone = "helpful, providing value, light urgency"
            key_points = """
- Reference previous email briefly
- Share specific success story ($500-2000/month range)
- Include affiliate signup link
- Mention spots filling up
- Ask if they have questions
"""
        else:  # followup_2
            email_goal = "Final follow-up - special offer, respect their time"
            tone = "respectful, final push with bonus offer"
            key_points = """
- Acknowledge this is final reach-out
- Offer 3 months FREE ReelForge Pro ($150 value)
- Priority support and early access to features
- Remind of 30% lifetime commissions
- Include signup link
- Wish them well either way
"""
        
        prompt = f"""You are writing a personalized affiliate outreach email for ReelForge AI.

PROSPECT INFO:
- Name: {first_name}
- Platform: {platform}
- Handle: {handle}
- Followers/Subscribers: {subscribers:,}
- Found via: {competitor} content
{video_context}

EMAIL TYPE: {template_type}
GOAL: {email_goal}
TONE: {tone}

KEY POINTS TO INCLUDE:
{key_points}

RULES:
- Keep it concise (150-200 words max)
- Sound human, not AI-generated
- No excessive formatting or bullet points in the email body
- Personalize based on their specific content/niche if possible
- Sign off as "Larry Barksdale" (not "Larry Barksdale, ReelForge AI" - just the name)

OUTPUT FORMAT (exactly like this):
SUBJECT: [your subject line here]
BODY:
[your email body here]

Write the email now:"""
        
        return prompt
    
    def _parse_email_response(self, content: str) -> Dict[str, str]:
        """Parse Claude's response into subject and body."""
        try:
            lines = content.strip().split("\n")
            subject = ""
            body_lines = []
            in_body = False
            
            for line in lines:
                if line.startswith("SUBJECT:"):
                    subject = line.replace("SUBJECT:", "").strip()
                elif line.startswith("BODY:"):
                    in_body = True
                elif in_body:
                    body_lines.append(line)
            
            body = "\n".join(body_lines).strip()
            
            # Convert to HTML
            html_body = "<p>" + body.replace("\n\n", "</p><p>").replace("\n", "<br>") + "</p>"
            
            return {
                "subject": subject,
                "body": html_body,
                "text_body": body
            }
        except Exception as e:
            logger.error("Failed to parse AI response", error=str(e))
            return {"subject": "", "body": "", "text_body": ""}
    
    def _fallback_template(self, prospect: Dict[str, Any], template_type: str) -> Dict[str, str]:
        """Fallback to standard template if AI fails."""
        first_name = (prospect.get("full_name") or "").split()[0] or "there"
        competitor = (prospect.get("competitor_mentions") or ["AI video tools"])[0]
        
        if template_type == "initial":
            return {
                "subject": f"Impressed by your {competitor} review - Unlock 30% lifetime earnings?",
                "body": f"""<p>Hi {first_name},</p>
<p>I just watched your video on {competitor} and was genuinely impressed by your insightful breakdown of AI video tools. It's clear you know what creators really need.</p>
<p>I'm Larry Barksdale from ReelForge AI, where we empower creators like you to produce stunning videos in minutes—saving hours of editing time so you can focus on what you love.</p>
<p>We've handpicked a select group of top creators for our affiliate program, offering <strong>30% lifetime recurring commissions</strong> on every referral. Imagine earning passive income month after month from your authentic recommendations—many partners are already seeing $1,000+ monthly without extra work.</p>
<p>What if this could add a reliable revenue stream to your channel? Reply to chat more—I'd love to share how others are succeeding.</p>
<p>Best,<br>Larry Barksdale</p>""",
                "text_body": f"Hi {first_name}, I just watched your video on {competitor} and was genuinely impressed. I'm Larry Barksdale from ReelForge AI. We offer 30% lifetime recurring commissions. Reply to chat more! -Larry"
            }
        elif template_type == "followup_1":
            return {
                "subject": f"{first_name}, creators are earning $1k+/mo with this—your link inside",
                "body": f"""<p>Hi {first_name},</p>
<p>Quick follow-up on my note about partnering with ReelForge AI. I know your time is valuable, so I'll keep this brief.</p>
<p>Since launching, creators in our program have generated $500–$2,000/month simply by sharing honest reviews. One partner (a tech reviewer like you) hit $1,500 in their first month alone—purely from audience trust.</p>
<p>Spots are filling up fast—any thoughts or questions? Just hit reply.</p>
<p>Best,<br>Larry Barksdale</p>""",
                "text_body": f"Hi {first_name}, Quick follow-up on ReelForge. Creators earn $500-2000/month. Reply with questions! -Larry"
            }
        else:
            return {
                "subject": f"Last chance, {first_name}: Free Pro access + 30% commissions",
                "body": f"""<p>Hi {first_name},</p>
<p>This is my final reach-out—I respect your inbox!</p>
<p>To make it a no-brainer, if you join this week, you'll get:</p>
<ul>
<li>3 months FREE ReelForge Pro ($150 value)</li>
<li>Dedicated priority support</li>
<li>Exclusive early access to beta features</li>
</ul>
<p>Plus, that <strong>30% lifetime recurring commission</strong> on referrals.</p>
<p>Keep crushing it with your content either way!</p>
<p>Best,<br>Larry Barksdale</p>""",
                "text_body": f"Hi {first_name}, Last chance! Join this week for 3 months FREE Pro + 30% lifetime commissions. -Larry"
            }


class YouTubeVideoFetcher:
    """Fetch video data from YouTube for personalization."""
    
    def __init__(self):
        self.settings = get_settings()
        self.api_key = self.settings.youtube_api_key
    
    async def get_latest_video(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """Fetch the latest video from a YouTube channel."""
        if not self.api_key or not channel_id:
            return None
        
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                # Get latest video from channel
                search_url = "https://www.googleapis.com/youtube/v3/search"
                response = await client.get(search_url, params={
                    "key": self.api_key,
                    "channelId": channel_id,
                    "part": "snippet",
                    "order": "date",
                    "maxResults": 1,
                    "type": "video"
                })
                
                if response.status_code != 200:
                    return None
                
                data = response.json()
                items = data.get("items", [])
                
                if not items:
                    return None
                
                video = items[0]
                snippet = video.get("snippet", {})
                
                return {
                    "video_id": video.get("id", {}).get("videoId"),
                    "title": snippet.get("title", ""),
                    "description": snippet.get("description", ""),
                    "published_at": snippet.get("publishedAt", ""),
                    "topics": self._extract_topics(snippet.get("title", "") + " " + snippet.get("description", ""))
                }
                
        except Exception as e:
            logger.error("Failed to fetch YouTube video", channel_id=channel_id, error=str(e))
            return None
    
    def _extract_topics(self, text: str) -> str:
        """Extract likely topics from video text."""
        keywords = [
            "AI", "video editing", "content creation", "tutorial", "review",
            "Pictory", "InVideo", "Synthesia", "HeyGen", "Descript", "Runway",
            "automation", "passive income", "YouTube", "TikTok", "shorts"
        ]
        found = [kw for kw in keywords if kw.lower() in text.lower()]
        return ", ".join(found[:5]) if found else "AI video tools"

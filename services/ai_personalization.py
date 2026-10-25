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
        # Safely handle competitor_mentions - could be None, empty list, or list with values
        competitor_mentions = prospect.get("competitor_mentions") or []
        competitor = competitor_mentions[0] if competitor_mentions else "AI video tools"
        
        video_context = ""
        if video_data:
            video_context = f"""
Their recent video:
- Title: {video_data.get('title', 'N/A')}
- Description snippet: {video_data.get('description', 'N/A')[:300]}
- Key topics: {video_data.get('topics', 'AI video tools')}
"""
        
        if template_type == "initial":
            email_goal = "First outreach - introduce ReelForge AI and its affiliate program"
            tone = "warm, specific, not salesy"
            key_points = """
- Mention something specific from the video information above, stated factually
  ("Your video on X covers ..."), never as if the sender personally watched it
- Introduce yourself as Larry Barksdale from ReelForge AI
- Briefly explain ReelForge (turns a prompt or niche into finished short-form videos)
- Mention the 30% recurring commission on referred subscriptions
- End with a soft call to action (reply with questions)
"""
        elif template_type == "followup_1":
            email_goal = "Follow-up - share the affiliate signup link"
            tone = "helpful and brief"
            key_points = """
- Reference the previous email briefly
- Include the affiliate signup link
- Ask if they have questions
"""
        else:  # followup_2
            email_goal = "Final follow-up - respect their time"
            tone = "respectful and brief"
            key_points = """
- Say this is the last message
- Remind them of the 30% recurring commission
- Include the signup link
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
- CRITICAL: The commission rate is EXACTLY 30% - never mention any other percentage (not 40%, not 25%, etc.)
- Never claim or imply that the sender watched, follows or is a fan of the creator
- Never state or imply earnings, income, results, success stories or guarantees
- Never invent offers, discounts, free plans, deadlines or limited spots
- Do not add a footer or unsubscribe text; it is added automatically
- Include these links in initial emails:
  - Affiliate info: https://www.reelforgeai.io/become-affiliate
  - Free signup: https://reelforgeai.io

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
        competitor_mentions = prospect.get("competitor_mentions") or []
        competitor = competitor_mentions[0] if competitor_mentions else "AI video tools"
        
        # Same copy as database/migrations/004_compliant_email_templates.sql.
        if template_type == "initial":
            return {
                "subject": f"Your {competitor} content and the ReelForge AI affiliate program",
                "body": f"""<p>Hi {first_name},</p>
<p>I came across your {competitor} content while looking for creators who cover AI video tools, and I think your audience could be a good fit for ReelForge AI.</p>
<p>ReelForge AI turns a prompt or niche into finished short-form videos, with script, visuals, voiceover and captions, so creators can publish consistently without editing.</p>
<p>Our affiliate program pays a <strong>30% recurring commission</strong> on the subscriptions you refer. Program details and terms: <a href="https://www.reelforgeai.io/become-affiliate">https://www.reelforgeai.io/become-affiliate</a></p>
<p>You can try ReelForge AI for free at <a href="https://reelforgeai.io">https://reelforgeai.io</a>. If it looks like a fit, just reply and I'll answer any questions.</p>
<p>Best,<br>Larry Barksdale<br>ReelForge AI</p>""",
                "text_body": f"Hi {first_name}, I came across your {competitor} content while looking for creators who cover AI video tools. ReelForge AI turns a prompt or niche into finished short-form videos. Our affiliate program pays a 30% recurring commission on referred subscriptions: https://www.reelforgeai.io/become-affiliate. Try it free: https://reelforgeai.io. Best, Larry Barksdale"
            }
        elif template_type == "followup_1":
            return {
                "subject": "Following up: ReelForge AI affiliate program",
                "body": f"""<p>Hi {first_name},</p>
<p>A quick follow-up on my note about the ReelForge AI affiliate program.</p>
<p>If you'd like to see whether it suits your audience, you can join here: <a href="https://www.reelforgeai.io/become-affiliate">https://www.reelforgeai.io/become-affiliate</a>. Commissions are 30% recurring on the subscriptions you refer.</p>
<p>Happy to answer any questions. Just reply to this email.</p>
<p>Best,<br>Larry Barksdale</p>""",
                "text_body": f"Hi {first_name}, a quick follow-up on the ReelForge AI affiliate program. You can join here: https://www.reelforgeai.io/become-affiliate. Commissions are 30% recurring on referred subscriptions. Best, Larry Barksdale"
            }
        else:
            return {
                "subject": f"Closing the loop, {first_name}",
                "body": f"""<p>Hi {first_name},</p>
<p>This is my last note on this, so I won't fill your inbox.</p>
<p>If a 30% recurring commission for recommending an AI short-form video tool is ever useful, the signup link is <a href="https://www.reelforgeai.io/become-affiliate">https://www.reelforgeai.io/become-affiliate</a>.</p>
<p>Either way, thanks for the content you make.</p>
<p>Best,<br>Larry Barksdale</p>""",
                "text_body": f"Hi {first_name}, this is my last note on this. If a 30% recurring commission for recommending an AI short-form video tool is ever useful, the signup link is https://www.reelforgeai.io/become-affiliate. Best, Larry Barksdale"
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
            # Core topics
            "AI", "video editing", "content creation", "tutorial", "review",
            "automation", "passive income", "YouTube", "TikTok", "shorts",
            # Established competitors
            "Pictory", "InVideo", "Synthesia", "HeyGen", "Descript", "Runway",
            "Fliki", "Lumen5",
            # 2025-2026 trending tools
            "Sora", "Kling", "Pika", "Veo", "Luma", "CapCut", "OpusClip",
            "Opus Clip", "Dream Machine", "Topaz", "ElevenLabs", "Eleven Labs",
            # Content types
            "faceless", "avatar", "text to video", "AI voice", "clip generator"
        ]
        found = [kw for kw in keywords if kw.lower() in text.lower()]
        return ", ".join(found[:5]) if found else "AI video tools"

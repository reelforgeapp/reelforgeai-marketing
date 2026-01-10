# outreach/brevo_sender.py
"""
Brevo (Sendinblue) Email Sending Integration
Uses your existing Brevo account for outreach

Free tier: 300 emails/day
"""

import httpx
from typing import Optional
from datetime import datetime
import structlog
from config import settings

logger = structlog.get_logger()


class BrevoSender:
    """
    Send emails via Brevo API
    Integrates with your existing Brevo setup
    """
    
    BASE_URL = "https://api.brevo.com/v3"
    
    def __init__(self):
        self.api_key = settings.brevo_api_key
        self.sender_email = settings.brevo_sender_email
        self.sender_name = settings.brevo_sender_name
    
    async def send_email(
        self,
        to_email: str,
        to_name: str,
        subject: str,
        html_content: str,
        text_content: str,
        tags: Optional[list[str]] = None,
        reply_to: Optional[str] = None
    ) -> dict:
        """
        Send a single email via Brevo
        
        Args:
            to_email: Recipient email
            to_name: Recipient name
            subject: Email subject
            html_content: HTML body
            text_content: Plain text body
            tags: Tags for tracking
            reply_to: Reply-to address
        
        Returns:
            Dict with messageId and status
        """
        logger.info(
            "Sending email",
            to=to_email,
            subject=subject[:50]
        )
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                payload = {
                    'sender': {
                        'name': self.sender_name,
                        'email': self.sender_email
                    },
                    'to': [{'email': to_email, 'name': to_name}],
                    'subject': subject,
                    'htmlContent': html_content,
                    'textContent': text_content,
                    'tags': tags or ['affiliate-outreach']
                }
                
                if reply_to:
                    payload['replyTo'] = {'email': reply_to}
                
                response = await client.post(
                    f"{self.BASE_URL}/smtp/email",
                    headers={
                        'api-key': self.api_key,
                        'Content-Type': 'application/json'
                    },
                    json=payload
                )
                
                response.raise_for_status()
                data = response.json()
                
                logger.info(
                    "Email sent successfully",
                    message_id=data.get('messageId'),
                    to=to_email
                )
                
                return {
                    'success': True,
                    'message_id': data.get('messageId'),
                    'sent_at': datetime.utcnow().isoformat()
                }
                
            except httpx.HTTPStatusError as e:
                error_data = e.response.json() if e.response.content else {}
                logger.error(
                    "Brevo API error",
                    status_code=e.response.status_code,
                    error=error_data,
                    to=to_email
                )
                return {
                    'success': False,
                    'error': error_data.get('message', str(e)),
                    'status_code': e.response.status_code
                }
            except Exception as e:
                logger.error("Email send failed", to=to_email, error=str(e))
                return {
                    'success': False,
                    'error': str(e)
                }
    
    async def add_contact(
        self,
        email: str,
        attributes: dict,
        list_ids: Optional[list[int]] = None
    ) -> dict:
        """
        Add or update a contact in Brevo
        
        Args:
            email: Contact email
            attributes: Contact attributes (FIRSTNAME, LASTNAME, etc.)
            list_ids: List IDs to add contact to
        
        Returns:
            Dict with success status
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.post(
                    f"{self.BASE_URL}/contacts",
                    headers={
                        'api-key': self.api_key,
                        'Content-Type': 'application/json'
                    },
                    json={
                        'email': email,
                        'attributes': attributes,
                        'listIds': list_ids or [],
                        'updateEnabled': True
                    }
                )
                
                response.raise_for_status()
                
                return {'success': True, 'email': email}
                
            except httpx.HTTPStatusError as e:
                # 204 is success for updates
                if e.response.status_code == 204:
                    return {'success': True, 'email': email}
                    
                logger.error(
                    "Failed to add contact",
                    email=email,
                    status_code=e.response.status_code
                )
                return {'success': False, 'error': str(e)}
            except Exception as e:
                logger.error("Add contact failed", email=email, error=str(e))
                return {'success': False, 'error': str(e)}
    
    async def get_email_events(self, email: str) -> dict:
        """
        Get email events for a contact (opens, clicks, etc.)
        
        Args:
            email: Contact email
        
        Returns:
            Dict with events
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(
                    f"{self.BASE_URL}/smtp/statistics/events",
                    headers={'api-key': self.api_key},
                    params={
                        'email': email,
                        'limit': 50
                    }
                )
                
                response.raise_for_status()
                return response.json()
                
            except Exception as e:
                logger.error("Failed to get events", email=email, error=str(e))
                return {'events': []}
    
    async def check_daily_limit(self) -> dict:
        """Check today's send count against limit"""
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(
                    f"{self.BASE_URL}/account",
                    headers={'api-key': self.api_key}
                )
                
                response.raise_for_status()
                data = response.json()
                
                # Get plan limits
                plan = data.get('plan', [{}])[0] if data.get('plan') else {}
                
                return {
                    'daily_limit': 300,  # Free tier
                    'email': data.get('email'),
                    'plan_type': plan.get('type', 'free')
                }
                
            except Exception as e:
                logger.error("Failed to check limits", error=str(e))
                return {'daily_limit': 300}  # Assume free tier


# ===========================================
# EMAIL TEMPLATES
# ===========================================

class EmailTemplates:
    """
    Email templates for affiliate recruitment sequences
    Plain text focused for better deliverability
    """
    
    @staticmethod
    def youtube_creator_initial(
        first_name: str,
        competitor: str,
        affiliate_link: str
    ) -> dict:
        """First email to YouTube creator"""
        
        subject = f"Loved your {competitor} review - partnership opportunity"
        
        html = f"""
        <p>Hey {first_name},</p>
        
        <p>I just watched your {competitor} review and you clearly know your stuff 
        when it comes to AI video tools.</p>
        
        <p>I'm Jr, founder of <a href="https://reelforgeai.io">ReelForge.ai</a> - 
        we're a newer AI video platform that I think your audience would love.</p>
        
        <p><strong>Quick highlights:</strong></p>
        <ul>
            <li>90-second scripted videos from a single prompt</li>
            <li>Multi-scene storytelling (not just talking heads)</li>
            <li>Built-in viral hooks optimized for Shorts/Reels/TikTok</li>
        </ul>
        
        <p>I'd love to get you set up as an affiliate partner:</p>
        <ul>
            <li>40% recurring commission (not one-time)</li>
            <li>Free pro account for your content</li>
            <li>Custom landing page for your audience</li>
        </ul>
        
        <p><a href="{affiliate_link}">Sign up here</a> (takes 2 minutes)</p>
        
        <p>Happy to jump on a quick call if you want a demo first.</p>
        
        <p>Best,<br>
        Jr<br>
        Founder, ReelForge.ai</p>
        """
        
        text = f"""Hey {first_name},

I just watched your {competitor} review and you clearly know your stuff when it comes to AI video tools.

I'm Jr, founder of ReelForge.ai - we're a newer AI video platform that I think your audience would love.

Quick highlights:
- 90-second scripted videos from a single prompt
- Multi-scene storytelling (not just talking heads)
- Built-in viral hooks optimized for Shorts/Reels/TikTok

I'd love to get you set up as an affiliate partner:
- 40% recurring commission (not one-time)
- Free pro account for your content
- Custom landing page for your audience

Sign up here: {affiliate_link}

Best,
Jr
Founder, ReelForge.ai
"""
        
        return {
            'subject': subject,
            'html': html,
            'text': text
        }
    
    @staticmethod
    def youtube_creator_followup1(
        first_name: str,
        affiliate_link: str
    ) -> dict:
        """Second email - follow up"""
        
        subject = "Quick follow-up: 40% commission opportunity"
        
        html = f"""
        <p>Hey {first_name},</p>
        
        <p>Quick follow-up on my email about ReelForge.ai partnership.</p>
        
        <p>Since you cover AI video tools, thought you'd want to know we just shipped:</p>
        <ul>
            <li>Google Veo 3 integration for cinema-quality output</li>
            <li>Multi-API routing (HeyGen + AI avatars)</li>
            <li>One-click series generation for episodic content</li>
        </ul>
        
        <p>Our affiliates are seeing strong conversions because the product 
        genuinely delivers on the promise.</p>
        
        <p>40% recurring commission: <a href="{affiliate_link}">{affiliate_link}</a></p>
        
        <p>Let me know if you have any questions.</p>
        
        <p>Jr</p>
        """
        
        text = f"""Hey {first_name},

Quick follow-up on my email about ReelForge.ai partnership.

Since you cover AI video tools, thought you'd want to know we just shipped:
- Google Veo 3 integration for cinema-quality output
- Multi-API routing (HeyGen + AI avatars)
- One-click series generation for episodic content

Our affiliates are seeing strong conversions because the product genuinely delivers.

40% recurring commission: {affiliate_link}

Let me know if you have any questions.

Jr
"""
        
        return {
            'subject': subject,
            'html': html,
            'text': text
        }
    
    @staticmethod
    def youtube_creator_final(
        first_name: str,
        affiliate_link: str
    ) -> dict:
        """Final email - last touch"""
        
        subject = "Last one from me (with a bonus)"
        
        html = f"""
        <p>{first_name},</p>
        
        <p>Last email from me - I respect your inbox.</p>
        
        <p>If timing just isn't right, no worries at all. But wanted to 
        sweeten the deal:</p>
        
        <p><strong>Sign up this week</strong> and I'll bump you to 
        <strong>50% commission for your first 3 months</strong>, plus I'll 
        personally create a custom demo video featuring your channel's style.</p>
        
        <p><a href="{affiliate_link}">{affiliate_link}</a></p>
        
        <p>Either way, keep making great content.</p>
        
        <p>Jr</p>
        """
        
        text = f"""{first_name},

Last email from me - I respect your inbox.

If timing just isn't right, no worries at all. But wanted to sweeten the deal:

Sign up this week and I'll bump you to 50% commission for your first 3 months, plus I'll personally create a custom demo video featuring your channel's style.

{affiliate_link}

Either way, keep making great content.

Jr
"""
        
        return {
            'subject': subject,
            'html': html,
            'text': text
        }
    
    @staticmethod
    def get_template(
        template_name: str,
        first_name: str,
        competitor: str,
        affiliate_link: str
    ) -> dict:
        """Get template by name"""
        
        templates = {
            'youtube_initial': EmailTemplates.youtube_creator_initial,
            'youtube_followup1': EmailTemplates.youtube_creator_followup1,
            'youtube_final': EmailTemplates.youtube_creator_final,
        }
        
        template_func = templates.get(template_name)
        if not template_func:
            raise ValueError(f"Unknown template: {template_name}")
        
        if template_name == 'youtube_initial':
            return template_func(first_name, competitor, affiliate_link)
        else:
            return template_func(first_name, affiliate_link)

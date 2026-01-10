"""
ReelForge Marketing Engine - NLP Relevance Scoring
Enhanced prospect scoring using spaCy for semantic analysis
"""

import re
from typing import Optional
from dataclasses import dataclass
import structlog

logger = structlog.get_logger()

# Lazy load spaCy to avoid startup time
_nlp = None


def get_nlp():
    """Lazy load spaCy model."""
    global _nlp
    if _nlp is None:
        try:
            import spacy
            _nlp = spacy.load("en_core_web_sm")
            logger.info("spaCy model loaded successfully")
        except OSError:
            logger.warning(
                "spaCy model not found. Run: python -m spacy download en_core_web_sm"
            )
            _nlp = None
    return _nlp


@dataclass
class NLPScore:
    """Results of NLP analysis."""
    total_score: float  # 0.0 to 0.20
    affiliate_signals: float
    review_quality: float
    technical_depth: float
    entities_found: list
    keywords_found: list


class NLPRelevanceScorer:
    """
    Enhanced relevance scoring using NLP analysis.
    
    Adds up to 0.20 points to the base relevance score by analyzing:
    1. Affiliate/partnership signals in content
    2. Review quality indicators
    3. Technical depth markers
    4. Entity recognition (products, organizations)
    
    The NLP score supplements the basic keyword matching with
    semantic understanding of the creator's content.
    """
    
    # Affiliate/partnership signal words
    AFFILIATE_KEYWORDS = {
        'affiliate': 0.03,
        'commission': 0.03,
        'partner': 0.02,
        'partnership': 0.02,
        'sponsor': 0.02,
        'sponsored': 0.02,
        'referral': 0.02,
        'discount code': 0.02,
        'promo code': 0.02,
        'link in bio': 0.01,
        'link below': 0.01,
    }
    
    # Review quality indicators
    REVIEW_KEYWORDS = {
        'honest review': 0.02,
        'in-depth': 0.02,
        'comprehensive': 0.02,
        'pros and cons': 0.02,
        'comparison': 0.02,
        'versus': 0.01,
        'vs': 0.01,
        'alternative': 0.01,
        'best': 0.01,
        'top': 0.01,
    }
    
    # Technical depth indicators
    TECHNICAL_KEYWORDS = {
        'tutorial': 0.02,
        'how to': 0.02,
        'step by step': 0.02,
        'demo': 0.02,
        'walkthrough': 0.02,
        'guide': 0.01,
        'tips': 0.01,
        'tricks': 0.01,
        'features': 0.01,
        'pricing': 0.01,
    }
    
    # Competitor product names (for entity matching)
    COMPETITOR_PRODUCTS = {
        'pictory', 'synthesia', 'invideo', 'heygen', 'lumen5',
        'd-id', 'runway', 'kapwing', 'flexclip', 'opus clip',
        'descript', 'canva', 'animoto', 'veed', 'clipchamp'
    }
    
    def __init__(self):
        self.nlp = get_nlp()
    
    def analyze(
        self,
        title: str,
        description: str,
        channel_description: str = ""
    ) -> NLPScore:
        """
        Analyze text content and return NLP-based relevance score.
        
        Args:
            title: Video/content title
            description: Video/content description
            channel_description: Channel/profile bio
        
        Returns:
            NLPScore with breakdown of scoring components
        """
        # Combine all text
        full_text = f"{title} {description} {channel_description}".lower()
        
        # Calculate component scores
        affiliate_score, affiliate_keywords = self._score_affiliate_signals(full_text)
        review_score, review_keywords = self._score_review_quality(full_text)
        technical_score, technical_keywords = self._score_technical_depth(full_text)
        
        # NLP entity analysis (if spaCy available)
        entities = []
        if self.nlp:
            entities = self._extract_entities(full_text)
        
        # Cap total NLP score at 0.20
        total_score = min(
            affiliate_score + review_score + technical_score,
            0.20
        )
        
        return NLPScore(
            total_score=round(total_score, 3),
            affiliate_signals=round(affiliate_score, 3),
            review_quality=round(review_score, 3),
            technical_depth=round(technical_score, 3),
            entities_found=entities,
            keywords_found=affiliate_keywords + review_keywords + technical_keywords
        )
    
    def _score_affiliate_signals(self, text: str) -> tuple[float, list]:
        """Score based on affiliate/partnership signals."""
        score = 0.0
        found = []
        
        for keyword, points in self.AFFILIATE_KEYWORDS.items():
            if keyword in text:
                score += points
                found.append(keyword)
        
        return min(score, 0.08), found  # Cap at 0.08
    
    def _score_review_quality(self, text: str) -> tuple[float, list]:
        """Score based on review quality indicators."""
        score = 0.0
        found = []
        
        for keyword, points in self.REVIEW_KEYWORDS.items():
            if keyword in text:
                score += points
                found.append(keyword)
        
        return min(score, 0.06), found  # Cap at 0.06
    
    def _score_technical_depth(self, text: str) -> tuple[float, list]:
        """Score based on technical content indicators."""
        score = 0.0
        found = []
        
        for keyword, points in self.TECHNICAL_KEYWORDS.items():
            if keyword in text:
                score += points
                found.append(keyword)
        
        return min(score, 0.06), found  # Cap at 0.06
    
    def _extract_entities(self, text: str) -> list:
        """
        Extract named entities using spaCy.
        
        Looks for:
        - PRODUCT: Product names
        - ORG: Organizations
        - MONEY: Pricing mentions
        """
        if not self.nlp:
            return []
        
        # Limit text length to avoid slow processing
        text = text[:5000]
        
        doc = self.nlp(text)
        
        entities = []
        for ent in doc.ents:
            if ent.label_ in ('PRODUCT', 'ORG', 'MONEY'):
                entities.append({
                    'text': ent.text,
                    'label': ent.label_
                })
        
        # Also check for competitor product mentions
        for product in self.COMPETITOR_PRODUCTS:
            if product in text:
                entities.append({
                    'text': product,
                    'label': 'COMPETITOR'
                })
        
        return entities[:20]  # Limit to 20 entities


def calculate_enhanced_relevance_score(
    base_score: float,
    title: str,
    description: str,
    channel_description: str = ""
) -> tuple[float, NLPScore]:
    """
    Calculate enhanced relevance score combining base score with NLP analysis.
    
    Args:
        base_score: Base relevance score (0.0 to 0.80)
        title: Video/content title
        description: Video/content description
        channel_description: Channel/profile bio
    
    Returns:
        Tuple of (final_score, nlp_breakdown)
    
    Example:
        base_score = 0.55  # From subscriber count, competitor mentions, etc.
        final_score, nlp = calculate_enhanced_relevance_score(
            base_score,
            title="Pictory AI Review 2025 - Honest Tutorial",
            description="In this video I'll show you how to use Pictory..."
        )
        # final_score might be 0.70 (0.55 + 0.15 from NLP)
    """
    scorer = NLPRelevanceScorer()
    nlp_score = scorer.analyze(title, description, channel_description)
    
    # Combine scores (base max 0.80, NLP max 0.20)
    final_score = min(base_score + nlp_score.total_score, 1.0)
    
    logger.debug(
        "Enhanced relevance score calculated",
        base_score=base_score,
        nlp_score=nlp_score.total_score,
        final_score=final_score,
        keywords_found=nlp_score.keywords_found[:5]
    )
    
    return final_score, nlp_score


# Example usage in discovery:
"""
from services.nlp_scoring import calculate_enhanced_relevance_score

# In YouTubeDiscovery._calculate_relevance():

# Calculate base score (existing logic)
base_score = 0.0
base_score += subscriber_score  # up to 0.25
base_score += competitor_score  # up to 0.25
base_score += bio_link_score    # 0.10
base_score += activity_score    # up to 0.10
# Total base: up to 0.70

# Add NLP score
final_score, nlp_breakdown = calculate_enhanced_relevance_score(
    base_score,
    title=video_title,
    description=video_description,
    channel_description=channel_about
)

# Store both scores
prospect['relevance_score'] = final_score
prospect['nlp_relevance_score'] = nlp_breakdown.total_score
prospect['nlp_keywords'] = nlp_breakdown.keywords_found
"""

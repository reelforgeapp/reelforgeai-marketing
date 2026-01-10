"""ReelForge Marketing Engine - Services Package"""

from services.idempotency import IdempotencyService, get_idempotency_service
from services.email_verification import (
    ClearoutClient,
    HunterClient,
    VerificationResult,
    VerificationStatus,
    get_verification_client
)
from services.nlp_scoring import (
    NLPRelevanceScorer,
    NLPScore,
    calculate_enhanced_relevance_score
)

__all__ = [
    'IdempotencyService',
    'get_idempotency_service',
    'ClearoutClient',
    'HunterClient',
    'VerificationResult',
    'VerificationStatus',
    'get_verification_client',
    'NLPRelevanceScorer',
    'NLPScore',
    'calculate_enhanced_relevance_score',
]

"""URL validation utility for SSRF prevention.

Provides ``_validate_url_safe()`` which checks that a URL:
- Uses http or https scheme
- Has a non-empty hostname
- Does not point to private/loopback/link-local/multicast IPs

Shared between bot.py (direct download path) and tasks.py (background workers).
"""
import logging

logger = logging.getLogger(__name__)


def _validate_url_safe(url: str) -> bool:
    """Validate a URL to prevent SSRF attacks.

    - Only http/https schemes allowed
    - Blocks private/loopback/link-local/multicast IPs
    - Blocks empty hostnames

    Args:
        url: The URL to validate.

    Returns:
        True if the URL is safe to fetch, False otherwise.
    """
    if not url or not isinstance(url, str):
        return False
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("https", "http"):
            return False
        if not parsed.netloc:
            return False
        # Check for private/internal IPs
        hostname = parsed.netloc.split(":")[0].split("@")[-1]
        try:
            import ipaddress
            ip = ipaddress.ip_address(hostname)
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast:
                return False
        except ValueError:
            pass  # Hostname, not an IP
        return True
    except Exception:
        return False

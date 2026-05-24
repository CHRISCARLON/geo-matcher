"""Pre-defined bounding boxes for common English cities in EPSG:27700 (British National Grid).

Each bbox is ``[xmin, ymin, xmax, ymax]`` in metres.

Usage::

    from usrn_matcher import UsrnMatcher
    from usrn_matcher.bboxes import LEEDS, LONDON

    matcher = UsrnMatcher(...)
    table = matcher.match_dispatch("polygon", bbox=LEEDS)
"""

# Approximate administrative boundaries in EPSG:27700.
# Based on local authority / combined authority extents — verify against
# ONS/OS boundary data if precise clipping is required.
LONDON: list[int] = [503000, 156000, 562000, 201000]  # Greater London Authority
LEEDS: list[int] = [413000, 420000, 448000, 451000]  # Leeds
MANCHESTER: list[int] = [376000, 388000, 399000, 415000]  # Greater Manchester
MANCHESTER_CITY_CENTRE: list[int] = [
    380000,
    395000,
    388000,
    402000,
]  # Manchester city centre
BIRMINGHAM: list[int] = [396000, 272000, 420000, 296000]  # Birmingham
LIVERPOOL: list[int] = [333000, 382000, 350000, 399000]  # Liverpool
SHEFFIELD: list[int] = [426000, 379000, 451000, 410000]  # Sheffield
BRISTOL: list[int] = [357000, 168000, 379000, 187000]  # Bristol
NEWCASTLE: list[int] = [416000, 554000, 435000, 574000]  # Newcastle
NOTTINGHAM: list[int] = [450000, 331000, 468000, 352000]  # Nottingham
NORWICH: list[int] = [609000, 296000, 634000, 320000]  # Norwich

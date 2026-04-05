"""Pre-defined bounding boxes for common English cities in EPSG:27700 (British National Grid).

Each bbox is ``[xmin, ymin, xmax, ymax]`` in metres.

Usage::

    from usrn_soil_matcher import UsrnSoilMatcher
    from usrn_soil_matcher.bboxes import LEEDS, LONDON

    matcher = UsrnSoilMatcher.from_gpkgs(...)
    table = matcher.match(bbox=LEEDS)
"""

LONDON = [503000, 155000, 562000, 200000]
LEEDS = [412000, 426000, 444000, 445000]
MANCHESTER = [370000, 380000, 400000, 405000]
BIRMINGHAM = [395000, 270000, 420000, 295000]
LIVERPOOL = [330000, 380000, 355000, 400000]
SHEFFIELD = [425000, 380000, 445000, 400000]
BRISTOL = [355000, 165000, 380000, 185000]
NEWCASTLE = [415000, 555000, 435000, 575000]
NOTTINGHAM = [450000, 330000, 470000, 350000]

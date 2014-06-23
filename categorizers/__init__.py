#from random_categorizer import RandomCategorizer
#from garbage_detector import GarbagePointCategorizer
from stats_categorizer import StatsCategorizer
#from state_categorizer import StateCategorizer

all_categorizers = [
#    GarbagePointCategorizer,
#    StateCategorizer
    StatsCategorizer
 ]

__all__ = all_categorizers


def get_categorizer(cat_id):
    """
    This is ugly, but since we won't have thousands of categorizers,
    it will be efficient enought.
    """
    for cat in all_categorizers:
        if cat.ID == cat_id:
            return cat
    return None
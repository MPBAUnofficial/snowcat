from random_categorizer import RandomCategorizer
from garbage_detector import GarbagePointCategorizer
from state_categorizer import StateCategorizer

all_categorizers = [
    RandomCategorizer,  # todo: remove me! :D
    GarbagePointCategorizer,
    StateCategorizer
 ]

__all__ = all_categorizers
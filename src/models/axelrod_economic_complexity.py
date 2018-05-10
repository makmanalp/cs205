from .abstract_axelrod_model import AbstractAxelrod

import random


def similarity_num_traits(a, b):
    """Return number of equal traits."""
    num_same = 0
    for i in range(len(a)):
        if a[i] == b[i]:
            num_same += 1
    return num_same


def similarity_cosine(a, b):
    """Return cosine similarity of trait vectors"""
    from numpy import inner
    from numpy.linalg import norm

    return inner(a, b) / (norm(a) * norm(b))


def pick_most_similar_neighbor(my_traits, possible_neighbors, similarity_func):
    """Pick the most similar neighbor according to a similarity function."""

    if possible_neighbors is None:
        return None

    if len(possible_neighbors) > 0:
        max_similarity = 0
        max_similarity_idx = 0

        for i, neighbor in enumerate(possible_neighbors):
            similarity = similarity_func(my_traits, neighbor)
            if similarity > max_similarity:
                max_similarity = similarity
                max_similarity_idx = i

        return possible_neighbors[max_similarity_idx]

    else:
        return None


def pick_random_neighbor(my_traits, possible_neighbors):
    """Pick any nearby neighbor"""
    if possible_neighbors is None:
        return None

    if len(possible_neighbors) > 0:
        return random.choice(possible_neighbors)
    else:
        return None


class EconomicComplexity(AbstractAxelrod):

    def __init__(self):
        self.min_val = 0
        self.max_val = 9
        self.num_traits = 4
        self.num_traits_to_inherit = 4

    def initialize_traits(self, row):
        """Randomly initialize 4 traits from 0 to 9."""
        return [random.randint(self.min_val, self.max_val)
                for _ in range(self.num_traits)]

    def check_neighbor_encounters(self, my_traits, possible_neighbors):
        return pick_most_similar_neighbor(my_traits, possible_neighbors, similarity_cosine)

    def combine_traits(self, my_traits, other_traits):

        if other_traits is None:
            return my_traits

        # Will we inherit?
        inherit_probability = similarity_cosine(my_traits, other_traits)
        if random.random() < inherit_probability:

            # Inherit a few random traits
            for _ in range(self.num_traits_to_inherit):
                i = random.randrange(len(other_traits))
                my_traits[i] = other_traits[i]

        return my_traits

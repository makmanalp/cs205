class AbstractAxelrod(object):

    def initialize_traits(self, row):
        """Takes a node dataframe and creates a "traits" column, with each row
        containing a new (random?) vector of of traits. Each vector should be
        the same size. Should return an Array of Integers."""
        raise NotImplementedError()

    def check_neighbor_encounters(self, my_traits, possible_neighbors):
        """Given the set of possible neighbors, select a subset that you will
        have an encounter with this turn, if any. If not return None."""
        raise NotImplementedError()

    def combine_traits(self, my_traits, other_traits):
        """In an encounter, this is a function to combine two traits in
        whichever way, average all, pick one, etc. If other_traits is null,
        return my_traits (no change). """
        raise NotImplementedError()

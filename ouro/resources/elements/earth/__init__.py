from .datasets import Datasets

__all__ = ["Earth", "Datasets"]


class Earth:
    def __init__(self, ouro):
        self.datasets = Datasets(ouro)

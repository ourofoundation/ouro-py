from .earth import Earth
from .air import Air


__all__ = ["Elements"]


class Elements:
    def __init__(self, ouro):
        self.earth = Earth(ouro)
        self.air = Air(ouro)

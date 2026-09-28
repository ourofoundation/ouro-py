from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Generic, Iterator, List, Optional, TypeVar

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

if TYPE_CHECKING:
    from ouro import Ouro

__all__ = ["OuroModel", "Page"]

T = TypeVar("T")


class OuroModel(BaseModel):
    """Base for every object the SDK returns.

    Fields the backend adds after this SDK was released are kept
    (``extra="allow"``) and readable as attributes. Objects parsed by a
    resource carry the client that fetched them, which powers methods such as
    ``File.read_data()`` and ``Action.refresh()``.
    """

    model_config = ConfigDict(extra="allow", validate_by_name=True)

    _ouro: Optional["Ouro"] = PrivateAttr(default=None)

    def model_post_init(self, context: Any, /) -> None:
        if isinstance(context, dict):
            self._ouro = context.get("ouro")

    def _require_client(self) -> "Ouro":
        if self._ouro is None:
            raise RuntimeError(
                f"{type(self).__name__} is not connected to an Ouro client"
            )
        return self._ouro


class Page(OuroModel, Generic[T]):
    """One page of a paginated list.

    Iterating, ``len()``, and indexing operate on ``data``. Pass ``offset`` +
    ``len(page)`` (or ``next_cursor`` for cursor-paged endpoints) to the same
    method to fetch the next page while ``has_more`` is true.
    """

    data: List[T] = Field(default_factory=list)
    has_more: bool = Field(default=False, alias="hasMore")
    total: Optional[int] = None
    offset: Optional[int] = None
    limit: Optional[int] = None
    next_cursor: Optional[Dict[str, Any]] = Field(default=None, alias="nextCursor")

    def __iter__(self) -> Iterator[T]:  # type: ignore[override]
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, index: int) -> T:
        return self.data[index]

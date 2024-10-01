# This file is used to import all the resources in the ouro package

from .content import Content, Editor
from .conversations import Conversations
from .datasets import Datasets
from .files import Files
from .posts import Posts
from .users import Users

__all__ = ["Content", "Editor", "Conversations", "Datasets", "Posts", "Files", "Users"]


def EditorFactory(self, **kwargs) -> Editor:
    return Editor(**kwargs)


def ContentFactory(self, **kwargs) -> Content:
    return Content(**kwargs)


def __init__(self, ouro):
    # Earth
    self.datasets = Datasets(ouro)
    self.files = Files(ouro)

    # Air
    self.posts = Posts(ouro)
    self.conversations = Conversations(ouro)

    self.Editor = self.EditorFactory
    self.Content = self.ContentFactory

    self.users = Users(ouro)

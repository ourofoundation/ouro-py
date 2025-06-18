# This file is used to import all the resources in the ouro package

from .assets import Assets
from .comments import Comments
from .content import Content, Editor
from .conversations import Conversations
from .datasets import Datasets
from .files import Files
from .posts import Posts
from .routes import Routes
from .services import Services
from .users import Users

__all__ = [
    "Content",
    "Editor",
    "Conversations",
    "Datasets",
    "Posts",
    "Files",
    "Comments",
    "Users",
    "Assets",
    "Services",
    "Routes",
]


def EditorFactory(self, **kwargs) -> Editor:
    return Editor(**kwargs)


def ContentFactory(self, **kwargs) -> Content:
    return Content(**kwargs)


def __init__(self, ouro):
    self.users = Users(ouro)
    self.assets = Assets(ouro)

    self.datasets = Datasets(ouro)
    self.files = Files(ouro)
    self.posts = Posts(ouro)
    self.conversations = Conversations(ouro)
    self.comments = Comments(ouro)
    self.services = Services(ouro)
    self.routes = Routes(ouro)

    self.Editor = self.EditorFactory
    self.Content = self.ContentFactory

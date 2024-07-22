from .content import Content, Editor
from .conversations import Conversations
from .posts import Posts

__all__ = ["Air", "Conversations", "Posts", "Content", "Editor"]


class Air:
    def __init__(self, client):
        self.posts = Posts(client)
        self.conversations = Conversations(client)

        self.Editor = self.EditorFactory
        self.Content = self.ContentFactory

    def EditorFactory(self, **kwargs) -> Editor:
        return Editor(**kwargs)

    def ContentFactory(self, **kwargs) -> Content:
        return Content(**kwargs)

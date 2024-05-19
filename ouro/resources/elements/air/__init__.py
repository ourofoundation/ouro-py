from .content import Content, Editor
from .posts import Posts

__all__ = ["Air", "Posts", "Content", "Editor"]


class Air:
    def __init__(self, client):
        self.posts = Posts(client)

        self.Editor = self.EditorFactory
        self.Content = self.ContentFactory

    def EditorFactory(self, **kwargs) -> Editor:
        return Editor(**kwargs)

    def ContentFactory(self, **kwargs) -> Content:
        return Content(**kwargs)

"""Users, conversations, notifications, money, and the realtime socket."""

from __future__ import annotations

import time

import pytest

from ouro.models import (
    BitcoinBalance,
    BitcoinTransaction,
    Conversation,
    Notification,
    UsdBalance,
    UsdTransaction,
)


def test_users_lookup(ouro, me, other_me):
    assert ouro.users.get(me.username).user_id == me.user_id
    assert ouro.users.get(str(other_me.user_id)).username == other_me.username
    found = ouro.users.search(other_me.username)
    assert any(u.user_id == other_me.user_id for u in found)
    assert ouro.users.impact(me.username).user_id == me.user_id


@pytest.fixture(scope="module")
def conversation(ouro, other_me, run_id):
    convo = ouro.conversations.create(
        member_user_ids=[str(ouro.user.id), str(other_me.user_id)],
        name=f"sdk-it-{run_id}",
    )
    yield convo
    ouro.conversations.delete(str(convo.id))


def test_conversation_roundtrip(ouro, other, conversation):
    assert isinstance(conversation, Conversation)
    assert other.conversations.retrieve(str(conversation.id)).id == conversation.id
    listed = ouro.conversations.list(limit=50)
    assert any(c.id == conversation.id for c in listed)


def test_messages_and_cursor_pagination(ouro, other, conversation):
    for i in range(3):
        conversation.messages.create(text=f"message {i}")
    other.conversations.retrieve(str(conversation.id)).messages.create(text="reply from other")

    newest = ouro.conversations.retrieve(str(conversation.id)).messages.list(limit=2)
    assert [m.text for m in newest][-1] == "reply from other"
    assert newest.has_more is True

    older = conversation.messages.list(limit=10, before=newest.next_cursor["before"])
    assert [m.text for m in older] == ["message 0", "message 1"]


def test_notifications_after_share(ouro, other, track):
    before = other.notifications.unreads()
    post = track.add(ouro.posts.create(name=track.name("notify"), content_markdown="for you", visibility="private"))
    ouro.assets.share(str(post.id), other.user.id)

    deadline = time.time() + 10
    notifications = []
    while time.time() < deadline:
        notifications = other.notifications.list(limit=20)
        if any(str(n.asset_id) == str(post.id) for n in notifications):
            break
        time.sleep(0.5)
    match = next(n for n in notifications if str(n.asset_id) == str(post.id))
    assert isinstance(match, Notification)
    assert other.notifications.unreads() >= before
    assert other.notifications.read(str(match.id)).viewed is True

    page = other.notifications.list(limit=1, category="shares")
    assert isinstance(page.has_more, bool)


def test_money_reads(ouro):
    assert isinstance(ouro.money.get_balance("btc"), BitcoinBalance)
    assert isinstance(ouro.money.get_balance("usd"), UsdBalance)
    assert all(isinstance(t, BitcoinTransaction) for t in ouro.money.get_transactions("btc"))
    assert all(isinstance(t, UsdTransaction) for t in ouro.money.get_transactions("usd", limit=5))
    history = ouro.money.get_usage_history(limit=5)
    assert history.summary.record_count >= len(history)
    assert ouro.money.get_pending_earnings().total_pending_cents >= 0
    with pytest.raises(ValueError):
        ouro.money.get_balance("eur")


def test_websocket_session_and_activity(ouro, conversation):
    with ouro.websocket.session():
        assert ouro.websocket.is_connected
        ouro.websocket.join_conversation(str(conversation.id))
        ouro.websocket.emit_activity(conversation_id=str(conversation.id), status="thinking", active=True)
    assert not ouro.websocket.is_connected

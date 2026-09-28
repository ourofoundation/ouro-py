from __future__ import annotations

import pytest

from ouro import OuroError
from ouro.models import Entry, Quest, QuestItem


@pytest.fixture(scope="module")
def quest(ouro, track):
    return track.add(
        ouro.quests.create(
            name=track.name("quest"),
            description="Find a better magnet",
            visibility="public",
            status="draft",
            items=[
                "Collect candidate structures",
                {"description": "Upload the best CIF", "submission_assets": {"file": {"asset_type": "file"}}},
            ],
        )
    )


def test_create_with_items(ouro, quest):
    assert isinstance(quest, Quest)
    assert quest.quest.status == "draft"
    assert quest.quest.type == "closable"
    items = ouro.quests.list_items(str(quest.id))
    assert all(isinstance(i, QuestItem) for i in items)
    assert [i.description.text.strip() for i in items] == [
        "Collect candidate structures",
        "Upload the best CIF",
    ]
    assert items[1].submission_assets["file"]["asset_type"] == "file"


def test_retrieve_includes_progress(ouro, quest):
    fetched = ouro.quests.retrieve(str(quest.id))
    assert fetched.progress.total == 2
    assert fetched.progress.remaining == 2


def test_draft_rejects_entries(ouro, other, quest):
    item = ouro.quests.list_items(str(quest.id))[0]
    with pytest.raises(OuroError):
        other.quests.create_entry(str(quest.id), item_id=str(item.id), description="too early")


def test_item_crud(ouro, quest):
    [extra] = ouro.quests.create_items(str(quest.id), ["Temporary item"])
    updated = ouro.quests.update_item(str(quest.id), str(extra.id), description="Renamed item", reward_xp=5)
    assert updated.description.text.strip() == "Renamed item"
    assert updated.reward_xp == 5
    ouro.quests.delete_item(str(quest.id), str(extra.id))
    assert all(i.id != extra.id for i in ouro.quests.list_items(str(quest.id)))


def test_open_submit_review(ouro, other, track, quest):
    opened = ouro.quests.update(str(quest.id), status="open")
    assert opened.quest.status == "open"
    collect, upload = ouro.quests.list_items(str(quest.id))

    cif = track.add(
        ouro.files.create(
            name=track.name("submission"), visibility="public", file_content=b"data_x\n", file_name="x.cif"
        )
    )
    entry = other.quests.create_entry(
        str(quest.id),
        item_id=str(upload.id),
        assets={"file": str(cif.id)},
        description="Here is my structure",
    )
    assert isinstance(entry, Entry)
    assert entry.status == "submitted"

    with pytest.raises(OuroError):
        other.quests.create_entry(str(quest.id), item_id=str(upload.id), description="duplicate")

    submitted = ouro.quests.list_entries(str(quest.id), status="submitted")
    assert [e.id for e in submitted] == [entry.id]

    reviewed = ouro.quests.review_entry(str(quest.id), str(entry.id), status="accepted", review="Nice work")
    assert reviewed.status == "accepted"
    assert ouro.quests.retrieve(str(quest.id)).progress.resolved >= 1

    done = ouro.quests.complete_item(str(quest.id), str(collect.id), description="Did it myself")
    assert done.item.status == "done" and done.entry.status == "accepted"
    assert ouro.quests.retrieve(str(quest.id)).progress.remaining == 0


def test_entries_pagination(ouro, quest):
    page = ouro.quests.list_entries(str(quest.id), limit=1)
    assert len(page) <= 1
    assert isinstance(page.has_more, bool)


def test_continuous_quest_allows_repeat_entries(ouro, other, track):
    quest = track.add(
        ouro.quests.create(
            name=track.name("continuous"),
            visibility="public",
            type="continuous",
            items=["Share an idea"],
        )
    )
    [item] = ouro.quests.list_items(str(quest.id))
    for idea in ("one", "two"):
        other.quests.create_entry(str(quest.id), item_id=str(item.id), description=f"Idea {idea}")
    assert len(ouro.quests.list_entries(str(quest.id))) == 2


def test_assigned_items(ouro, me, track):
    quest = track.add(ouro.quests.create(name=track.name("assigned"), visibility="private", items=["Mine"]))
    [item] = ouro.quests.list_items(str(quest.id))
    ouro.quests.update_item(str(quest.id), str(item.id), assignee_id=str(me.user_id))
    assigned = ouro.quests.list_assigned_items(limit=100)
    assert any(a.id == item.id for a in assigned)


def test_list_quests(ouro, quest):
    assert any(q.id == quest.id for q in ouro.quests.list(scope="personal", sort="recent", limit=50))

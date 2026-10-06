"""Self-hosted environment coverage through the pinned SDK.

Arguments: the API base URL, a session whose executor is connected, and that
session's workspace directory.
"""

import base64
import os
import sys
import time

import openai

from sdk_helpers import client as sdk_client
from sdk_helpers import message
from sdk_sessions import idle
from sdk_sessions import rejected

client = sdk_client(sys.argv[1])
session_id, workspace = sys.argv[2], sys.argv[3]
sessions = client.beta.agents.sessions
environments = client.beta.agents.environments

session = sessions.retrieve(session_id)
environment = session.environment
assert environment.type == "self_hosted", environment
assert environment.workspace_directory == workspace
assert environment.remote_url.endswith("/registry")
info = environments.retrieve(environment.id)
assert (info.id, info.type, info.status) == (
    environment.id,
    "self_hosted",
    "connected",
), info
assert (info.files, info.plugins, info.skills) == ([], [], []), info

# Files API uploads feed environment files by ID.
uploaded = client.files.create(
    file=("notes.txt", b"from the files api"), purpose="user_data"
)
assert client.files.retrieve(uploaded.id) == uploaded
assert uploaded.id in [item.id for item in client.files.list(purpose="user_data")]
assert client.files.content(uploaded.id).read() == b"from the files api"

files = environments.files
first = os.path.join(workspace, "docs", "a.txt")
second = os.path.join(workspace, "docs", "b.txt")
inline = files.create(
    environment.id, type="inline", path=first, data=base64.b64encode(b"inline").decode()
)
assert (inline.path, inline.size_bytes) == (first, 6), inline
copied = files.create(environment.id, type="file_id", path=second, file_id=uploaded.id)
assert copied.size_bytes == len(b"from the files api"), copied
rejected(
    openai.BadRequestError,
    files.create,
    environment.id,
    type="inline",
    path=os.path.join(os.path.dirname(workspace), "outside.txt"),
    data="eA==",
)
# Iterating follows the opaque page token one file at a time.
listed = [item.path for item in files.list(environment.id, order="asc", limit=1)]
assert listed == [first, second], listed
client.files.delete(uploaded.id)
rejected(openai.NotFoundError, client.files.retrieve, uploaded.id)

# A command runs on the executor and is reported as an item.
sessions.events.create(session_id, events=[message("run the command")])
deadline = time.monotonic() + 20
while not [
    turn for turn in sessions.turns.list(session_id) if turn.status == "completed"
]:
    assert time.monotonic() < deadline, "the turn did not complete"
    time.sleep(0.05)
idle(sessions, session_id)
commands = [
    item for item in sessions.items.list(session_id) if item.type == "command_execution"
]
assert [(item.status, item.exit_code) for item in commands] == [("completed", 0)], (
    commands
)

# Without an executor, input waits behind an environment_connection action.
waiting = sessions.create(
    agent={"model": "mock-model"},
    environment={"type": "self_hosted", "workspace_directory": workspace},
    input="anyone there?",
)
assert waiting.status == "requires_action", waiting
assert [action.model_dump() for action in waiting.required_actions] == [
    {"type": "environment_connection", "environment_id": waiting.environment.id}
]
sessions.events.create(waiting.id, events=[{"type": "agent.session.input.cancel"}])
assert idle(sessions, waiting.id).required_actions == []
sessions.delete(waiting.id)
rejected(openai.NotFoundError, environments.retrieve, waiting.environment.id)
print("sdk environments ok")

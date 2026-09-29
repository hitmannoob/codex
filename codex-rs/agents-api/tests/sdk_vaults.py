"""Vault and credential coverage, called from the pinned SDK lifecycle test."""

import openai

from sdk_sessions import rejected


def check_vaults(client):
    vaults = client.beta.agents.vaults
    vault = vaults.create(name="  sdk-team  ", metadata={"app": "sdk"})
    assert vault.name == "sdk-team" and vault.metadata == {"app": "sdk"}
    assert vaults.retrieve(vault.id) == vault
    assert vault.id in [item.id for item in vaults.list()]
    assert list(vaults.list(status="archived")) == []

    credentials = vaults.credentials
    credential = credentials.create(
        vault.id,
        name="warehouse",
        auth={
            "type": "static_bearer",
            "token": "sdk-secret-token",
            "mcp_server_url": "https://mcp.example.com/mcp",
        },
    )
    assert credential.auth.type == "static_bearer"
    assert "sdk-secret-token" not in credential.model_dump_json()
    assert credentials.retrieve(credential.id, vault_id=vault.id) == credential
    assert [item.id for item in credentials.list(vault.id)] == [credential.id]
    rotated = credentials.update(
        credential.id,
        vault_id=vault.id,
        auth={"type": "static_bearer", "token": "sdk-rotated-token"},
    )
    assert rotated.id == credential.id and rotated.created_at == credential.created_at
    for auth in [
        {
            "type": "environment_variable",
            "secret_name": "KEY",
            "secret_value": "value",
            "networking": {"type": "unrestricted"},
        },
        {
            "type": "static_bearer",
            "token": "",
            "mcp_server_url": "https://mcp.example.com",
        },
    ]:
        rejected(
            openai.BadRequestError, credentials.create, vault.id, name="bad", auth=auth
        )

    deleted = credentials.delete(credential.id, vault_id=vault.id)
    assert deleted.model_dump() == {
        "id": credential.id,
        "object": "vault.credential.deleted",
        "deleted": True,
    }
    rejected(
        openai.NotFoundError, credentials.retrieve, credential.id, vault_id=vault.id
    )
    assert vaults.delete(vault.id).model_dump() == {
        "id": vault.id,
        "object": "vault.deleted",
        "deleted": True,
    }
    rejected(openai.NotFoundError, vaults.retrieve, vault.id)

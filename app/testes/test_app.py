import json
import pytest
from fastapi import FastAPI
from router.rabbit import Rabbit
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch

app = FastAPI()
app.include_router(Rabbit().router, prefix="")

client = TestClient(app)

@pytest.mark.asyncio
async def test_notificar_publica_mensagem():
    data = {
        "mensagemId": None,
        "conteudoMensagem": "teste",
        "tipoNotificacao": "EMAIL"
    }

    fake_channel = AsyncMock()
    fake_conn = AsyncMock()
    fake_conn.channel.return_value = fake_channel

    with patch("router.rabbit.aio_pika.connect_robust", return_value=fake_conn):
        resp = client.post("/api/notificar", json=data)

    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "ACCEPTED"
    assert "traceId" in body

    fake_channel.default_exchange.publish.assert_awaited()
    args, kwargs = fake_channel.default_exchange.publish.call_args

    sent_msg = args[0]
    payload = json.loads(sent_msg.body.decode())
    assert payload["conteudoMensagem"] == "teste"
    assert payload["tipoNotificacao"] == "EMAIL"

    assert kwargs["routing_key"]  

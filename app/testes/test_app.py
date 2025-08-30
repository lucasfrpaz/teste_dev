# tests/test_rabbit.py
import pytest
import json
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient
from router.rabbit import Rabbit, NOTIFICACOES
from schemas.rabbit_schemas import Notificar
from fastapi import FastAPI

# cria app só pra teste
app = FastAPI()
app.include_router(Rabbit().router, prefix="")

client = TestClient(app)

@pytest.mark.asyncio
async def test_notificar_publica_mensagem():
    data = {
        "mensagemId": "123",
        "conteudoMensagem": "teste",
        "tipoNotificacao": "EMAIL"
    }

    fake_channel = AsyncMock()
    fake_conn = AsyncMock()
    fake_conn.channel.return_value = fake_channel

    with patch("router.rabbit.aio_pika.connect_robust", return_value=fake_conn):
        resp = client.post("/api/notificar", json=data)

    # resposta da API
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "ACCEPTED"
    assert "traceId" in body

    # verifica se publicou no exchange
    fake_channel.default_exchange.publish.assert_awaited()
    args, kwargs = fake_channel.default_exchange.publish.call_args

    # verifica que payload bate
    sent_msg = args[0]
    payload = json.loads(sent_msg.body.decode())
    assert payload["mensagemId"] == "123"
    assert payload["conteudoMensagem"] == "teste"
    assert payload["tipoNotificacao"] == "EMAIL"

    # verifica routing key usada
    assert kwargs["routing_key"]  # bate com QUEUE_NAME do env

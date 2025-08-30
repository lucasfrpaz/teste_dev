import os
import aio_pika
import json, uuid
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from dotenv import load_dotenv, find_dotenv
from schemas.rabbit_schemas import Notificar
from fastapi.encoders import jsonable_encoder

load_dotenv(find_dotenv(".env"))


STATUS: dict[str, str] = {}

RABBIT_URL = os.environ.get("RABBIT_URL")
QUEUE_NAME = os.environ.get("QUEUE_NAME")
NOTIFICACOES = {} 


class Rabbit:
    def __init__(self, tags: list[str] = ["Rabbit"]) -> None:
        self.router = APIRouter(tags=tags)
       
        self.router.add_api_route("/api/notificar", self.notificar, methods=["POST"])
        self.router.add_api_route("/api/status", self.get_status, methods=["GET"])

    async def notificar(self, data: Notificar):
        trace_id = str(uuid.uuid4())

        NOTIFICACOES[trace_id] = {
            "traceId": trace_id,
            "mensagemId": str(data.mensagemId),  
            "conteudoMensagem": data.conteudoMensagem,
            "tipoNotificacao": data.tipoNotificacao,
            "status_atual": "RECEBIDO"
        }

        payload = {
            "traceId": trace_id,
            "mensagemId": str(data.mensagemId), 
            "conteudoMensagem": data.conteudoMensagem,
            "tipoNotificacao": data.tipoNotificacao
        }

        conn = await aio_pika.connect_robust(RABBIT_URL)
        async with conn:
            ch = await conn.channel()
            await ch.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(payload).encode(), 
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    headers={"x-retry": 0}
                ),
                routing_key=QUEUE_NAME
            )

        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content=jsonable_encoder({
                "mensagemId": data.mensagemId,  
                "traceId": trace_id,
                "status": "ACCEPTED",
            }),
        )

    async def get_status(self,traceId: str):
        data = NOTIFICACOES.get(traceId)
        if not data:
            return JSONResponse(status_code=404, content={"detail": "traceId não encontrado"})
        return data
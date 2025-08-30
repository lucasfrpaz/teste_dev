from router.rabbit import NOTIFICACOES
from dotenv import load_dotenv, find_dotenv
import os, json, asyncio, random, aio_pika, logging, sys

load_dotenv(find_dotenv(".env"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

RABBIT_URL    = os.environ.get("RABBIT_URL")
QUEUE_ENTRADA = os.environ.get("QUEUE_ENTRADA")
QUEUE_RETRY   = os.environ.get("QUEUE_RETRY")
QUEUE_VALID   = os.environ.get("QUEUE_VALID")
QUEUE_DLQ     = os.environ.get("QUEUE_DLQ")
PREFETCH      = int(os.environ.get("PREFETCH", "10"))

def update_status(trace_id: str, status: str):
    item = NOTIFICACOES.get(trace_id)
    if item:
        item["status_atual"] = status
        logging.info(f"[STATUS] {trace_id} -> {status}")

async def ensure_queues(ch: aio_pika.Channel):
    for q in [QUEUE_ENTRADA, QUEUE_RETRY, QUEUE_VALID, QUEUE_DLQ]:
        await ch.declare_queue(q, durable=True)
        logging.info(f"[QUEUE] declarada: {q}")

async def consumidor_entrada(ch: aio_pika.Channel):
    queue = await ch.declare_queue(QUEUE_ENTRADA, durable=True)
    async with queue.iterator() as qit:
        async for msg in qit:
            async with msg.process(requeue=False):
                payload = json.loads(msg.body.decode())
                trace_id = payload["traceId"]
                logging.info(f"[ENTRADA] recebida {trace_id} | payload={payload}")

                if payload.get("conteudoMensagem") == "teste DLQ":
                    update_status(trace_id, "FORCADO_DLQ")
                    logging.warning(f"[PUB] {trace_id} (teste DLQ) -> {QUEUE_DLQ}")
                    await ch.default_exchange.publish(
                        aio_pika.Message(
                            body=json.dumps(payload).encode(),
                            content_type="application/json",
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key=QUEUE_DLQ,
                    )
                    continue

                if random.random() < random.uniform(0.10, 0.15):
                    update_status(trace_id, "FALHA_PROCESSAMENTO_INICIAL")
                    logging.warning(f"[PUB] {trace_id} -> {QUEUE_RETRY}")
                    await ch.default_exchange.publish(
                        aio_pika.Message(
                            body=json.dumps(payload).encode(),
                            content_type="application/json",
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key=QUEUE_RETRY,
                    )
                    continue

                await asyncio.sleep(random.uniform(1, 1.5))
                update_status(trace_id, "PROCESSADO_INTERMEDIARIO")
                logging.info(f"[PUB] {trace_id} -> {QUEUE_VALID}")
                await ch.default_exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(payload).encode(),
                        content_type="application/json",
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key=QUEUE_VALID,
                )

async def consumidor_retry(ch: aio_pika.Channel):
    queue = await ch.declare_queue(QUEUE_RETRY, durable=True)
    async with queue.iterator() as qit:
        async for msg in qit:
            async with msg.process(requeue=False):
                payload = json.loads(msg.body.decode())
                trace_id = payload["traceId"]
                logging.info(f"[RETRY] recebida {trace_id}")

                await asyncio.sleep(3)

                if random.random() < 0.20:
                    update_status(trace_id, "FALHA_FINAL_REPROCESSAMENTO")
                    logging.error(f"[PUB] {trace_id} -> {QUEUE_DLQ}")
                    await ch.default_exchange.publish(
                        aio_pika.Message(
                            body=json.dumps(payload).encode(),
                            content_type="application/json",
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key=QUEUE_DLQ,
                    )
                    continue

                update_status(trace_id, "REPROCESSADO_COM_SUCESSO")
                logging.info(f"[PUB] {trace_id} -> {QUEUE_VALID}")
                await ch.default_exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(payload).encode(),
                        content_type="application/json",
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key=QUEUE_VALID,
                )

async def consumidor_validacao(ch: aio_pika.Channel):
    queue = await ch.declare_queue(QUEUE_VALID, durable=True)
    async with queue.iterator() as qit:
        async for msg in qit:
            async with msg.process(requeue=False):
                payload = json.loads(msg.body.decode())
                trace_id = payload["traceId"]
                logging.info(f"[VALIDACAO] recebida {trace_id} ({payload.get('tipoNotificacao')})")

                await asyncio.sleep(random.uniform(0.5, 1))

                if random.random() < 0.05:
                    update_status(trace_id, "FALHA_ENVIO_FINAL")
                    logging.error(f"[PUB] {trace_id} -> {QUEUE_DLQ}")
                    await ch.default_exchange.publish(
                        aio_pika.Message(
                            body=json.dumps(payload).encode(),
                            content_type="application/json",
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key=QUEUE_DLQ,
                    )
                    continue

                update_status(trace_id, "ENVIADO_SUCESSO")
                logging.info(f"[OK] {trace_id} finalizado com sucesso")

async def consumidor_dlq(ch: aio_pika.Channel):
    queue = await ch.declare_queue(QUEUE_DLQ, durable=True)
    async with queue.iterator() as qit:
        async for msg in qit:
            async with msg.process(requeue=False):
                payload = json.loads(msg.body.decode())
                trace_id = payload.get("traceId")
                update_status(trace_id, "NA_DLQ")
                logging.error(f"[DLQ] Mensagem foi para DLQ e NÃO será mais processada: {trace_id} | payload={payload}")

async def start_consumers():
    conn = await aio_pika.connect_robust(RABBIT_URL)
    ch = await conn.channel()
    await ch.set_qos(prefetch_count=PREFETCH)
    await ensure_queues(ch)

    tasks = [
        asyncio.create_task(consumidor_entrada(ch)),
        asyncio.create_task(consumidor_retry(ch)),
        asyncio.create_task(consumidor_validacao(ch)),
        asyncio.create_task(consumidor_dlq(ch)),
    ]
    logging.info("[START] consumidores inicializados")
    return conn, tasks

async def stop_consumers(conn, tasks):
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await conn.close()
    logging.info("[STOP] consumidores finalizados e conexão fechada")

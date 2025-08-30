import uuid
from typing import Optional
from pydantic import BaseModel, UUID4, Field,field_validator,root_validator

class Notificar(BaseModel):
    mensagemId: Optional[UUID4] = None
    conteudoMensagem: str = Field(min_length=1)
    tipoNotificacao: str  

    @field_validator("mensagemId", mode="before")
    def val_mensagemId(cls, value):

        if value == None:
            value = str(uuid.uuid4())
            return value
        return value    
    
    @field_validator("tipoNotificacao")
    def validar_tipoNotificacao(cls, value):
        tipos_validos = {"EMAIL", "SMS", "PUSH"}
        if value not in tipos_validos:
            raise ValueError(f"tipoNotificacao inválido. Use um de {tipos_validos}")
        return value
    
class NotificacaoStatus(BaseModel):
    traceId: UUID4
    mensagemId: UUID4
    conteudoMensagem: str
    tipoNotificacao: str
    status_atual: str
    historico: list[str]

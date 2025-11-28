# Trabalho de Redes de Computadores - Chat P2P

Implementação de um cliente de Chat P2P com arquitetura peer-to-peer e servidor Rendezvous.

## Requisitos

- Python 3.10+

## Instalação

1. Clone o repositório:
```bash
git clone https://github.com/SamuelRicosta/Trabalho-de-Redes-de-computadores.git
cd Trabalho-de-Redes-de-computadores
```

2. Crie e ative o ambiente virtual:
```powershell
# Windows PowerShell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

## Como Rodar

Execute o cliente P2P a partir da raiz do projeto:

```powershell
python -m src.p2p_client.main
```

Para parar, pressione `Ctrl+C`.

## Funcionalidades Implementadas

### ✅ Bloco 1 - Integração com Rendezvous
- REGISTER: registra peer no servidor
- DISCOVER: descobre peers ativos
- UNREGISTER: remove registro ao encerrar

### ✅ Bloco 2 - Conexões TCP entre Peers
- Servidor TCP (aceita conexões inbound)
- Cliente TCP (conecta aos peers descobertos)
- Handshake HELLO/HELLO_OK
- Encerramento limpo BYE/BYE_OK

### 🚧 Em desenvolvimento
- Bloco 3: Mensageria (SEND/ACK, PUB)
- Bloco 4: Keep-alive (PING/PONG, RTT)
- Bloco 5: CLI e comandos

## Autores

- Samuel
- Ricardo
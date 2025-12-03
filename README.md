# Trabalho de Redes de Computadores - Chat P2P

Implementação de um cliente de Chat P2P com arquitetura peer-to-peer e servidor Rendezvous.

## Requisitos

- Python 3.10+ (Linux é obrigatório; basta ter `python3` instalado)
	- O projeto foi validado em Ubuntu/Debian com `python3.13`
	- Garante acesso à rede para conectar ao servidor Rendezvous e peers TCP

## Instalação

1. Clone o repositório:
```bash
git clone https://github.com/SamuelRicosta/Trabalho-de-Redes-de-computadores.git
cd Trabalho-de-Redes-de-computadores
```

2. Crie e ative o ambiente virtual (Linux):
```bash
python3 -m venv .venv
source .venv/bin/activate
```

## Configuração

O projeto utiliza o arquivo `config.json` para configurações:

```json
{
  "rendezvous": {
    "host": "127.0.0.1",     // IP do servidor Rendezvous
    "port": 8080             // Porta do servidor
  },
  "client": {
    "namespace": "CIC",      // Namespace/sala do chat
    "ttl": 7200,             // Tempo de vida do registro (segundos)
    "log_level": "ERROR"     // Nível de log: DEBUG, INFO, ERROR
  },
  "network": {
    "discover_interval": 10, // Intervalo entre DISCOVER (segundos)
    "ping_interval": 30,     // Intervalo entre PING (segundos)
    "connection_timeout": 5.0 // Timeout de conexão TCP
  }
}
```

Para usar o servidor remoto, altere `"host": "45.171.101.167"` no `config.json`.

## Como Rodar

Execute o cliente P2P a partir da raiz do projeto:

```bash
python -m src.p2p_client.main [PORTA] [NOME]
```

**Exemplo:**
```powershell
# Terminal 1
python -m src.p2p_client.main 5001 samuel

# Terminal 2
python -m src.p2p_client.main 5002 ricardo
```

Para parar, pressione `Ctrl+C` ou digite `/quit`.

> Não há dependências externas além da biblioteca padrão do Python, portanto não é necessário instalar pacotes adicionais.

## Comandos Disponíveis

Uma vez conectado, você pode usar os seguintes comandos:

- `/msg <peer_id> <mensagem>` - Envia mensagem direta para um peer
- `/pub * <mensagem>` - Envia broadcast para todos os peers
- `/pub #<namespace> <mensagem>` - Envia mensagem para peers do namespace
- `/peers` - Lista todos os peers descobertos
- `/conn` - Exibe conexões ativas (inbound/outbound)
- `/rtt` - Mostra RTT (Round Trip Time) de cada peer
- `/reconnect` - Força reconexão com peers
- `/log <nível>` - Ajusta nível de log (DEBUG, INFO, ERROR)
- `/quit` - Encerra a aplicação

## Funcionalidades Implementadas

### ✅ Bloco 1 - Integração com Rendezvous
- REGISTER: registra peer no servidor
- DISCOVER: descobre peers ativos periodicamente
- UNREGISTER: remove registro ao encerrar

### ✅ Bloco 2 - Conexões TCP entre Peers
- Servidor TCP (aceita conexões inbound)
- Cliente TCP (conecta aos peers descobertos)
- Handshake HELLO/HELLO_OK
- Encerramento limpo BYE/BYE_OK

### ✅ Bloco 3 - Mensageria
- SEND: mensagens diretas com confirmação ACK
- PUB: broadcast global e por namespace
- Controle de TTL (Time To Live)
- Timeout de ACK configurável

### ✅ Bloco 4 - Keep-alive e Métricas
- PING/PONG automático a cada 30s
- Cálculo de RTT (Round Trip Time)
- Detecção de peers inativos
- Reconexão automática

### ✅ Bloco 5 - CLI e Observabilidade
- Interface de linha de comando completa
- Logs estruturados (ajustáveis)
- Comandos de inspeção e controle
- Exibição de estatísticas

## Autores

- Samuel Ribeiro da Costa - 21103146 
- Ricardo Pedrosa Ramos Filho - 242032587
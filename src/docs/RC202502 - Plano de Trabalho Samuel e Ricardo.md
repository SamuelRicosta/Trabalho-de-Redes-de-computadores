# Plano de Trabalho - Chat P2P (PyP2p)

Este documento divide as responsabilidades do trabalho entre **Samuel** e **Ricardo**, com foco em um desenvolvimento paralelo e organizado.

---

## Visão Geral das Entregas

O trabalho pode ser dividido em cinco grandes blocos:

1. Integração com o servidor Rendezvous (registro, descoberta, sincronização da tabela de peers)
2. Conexões TCP entre peers (handshake HELLO/HELLO_OK, BYE/BYE_OK e reconexão)
3. Mensageria entre peers (SEND/ACK, PUB, roteamento de mensagens)
4. Módulo de keep-alive e métricas (PING/PONG, RTT, estados dos peers)
5. CLI (interface de usuário) e observabilidade (logs, comandos de inspeção)

Abaixo, cada bloco é atribuído de forma principal a um dos dois, mas sempre com revisão cruzada.

Legenda:
- **Responsável principal** → implementa a maior parte do código
- **Revisor** → lê, testa e sugere melhorias

---

## Bloco 1 – Integração com o Servidor Rendezvous

### Escopo
- Implementar comunicação com o servidor Rendezvous (`REGISTER`, `UNREGISTER`, `DISCOVER`).
- Manter uma **tabela de peers** atualizada a partir das respostas do Rendezvous.
- Parametrizar endereço/porta do Rendezvous (ex.: `pyp2p.mfcaetano.cc:8080`).

### Tarefas
- Implementar módulo sugerido: `rendezvous_connection.py` ou equivalente.
- Implementar/ajustar módulo de estado/tabela de peers: `peer_table.py` / `state.py`.
- Funções para:
  - Registrar peer (enviar `REGISTER`).
  - Descobrir peers periodicamente (`DISCOVER`).
  - Remover registro (`UNREGISTER`) no encerramento.
- Integração com o cliente principal (`p2p_client.py`).

### Responsáveis
- **Responsável principal:** Samuel
- **Revisor:** Ricardo

---

## Bloco 2 – Conexões TCP entre Peers

### Escopo
- Estabelecer conexões TCP entre peers descobertos.
- Implementar handshake HELLO / HELLO_OK.
- Implementar encerramento controlado BYE / BYE_OK.
- Gerenciar conexões inbound/outbound.

### Tarefas
- Implementar módulo `peer_connection.py`.
- Implementar servidor TCP local (aceitar conexões de outros peers).
- Implementar cliente TCP (conectar em outros peers descobertos).
- Lógica de handshake:
  - Enviar `HELLO` ao conectar.
  - Responder `HELLO_OK`.
- Lógica de encerramento:
  - Enviar `BYE` ao sair.
  - Responder `BYE_OK`.
- Expor API para o `message_router` e para o `p2p_client` (ex.: `send_to_peer(peer_id, msg)`).

### Responsáveis
- **Responsável principal:** Ricardo
- **Revisor:** Samuel

---

## Bloco 3 – Mensageria (SEND/ACK, PUB)

### Escopo
- Implementar o envio e recebimento de mensagens entre peers, incluindo ACK.
- Suportar três escopos de envio:
  - Unicast (`/msg peer_id ...`)
  - Namespace-cast (`/pub #namespace ...`)
  - Broadcast global (`/pub * ...`)

### Tarefas
- Implementar módulo `message_router.py`.
- Definir formato interno das mensagens (JSON por linha, UTF-8).
- Implementar:
  - Envio `SEND` + controle de `ACK` (timeout 5s → log de aviso).
  - Envio `PUB` para `*` e `#namespace`.
  - Entrega das mensagens recebidas para o CLI (ex.: imprimir no terminal).
- Integração estreita com `peer_connection.py` (uso de sockets já conectados).

### Responsáveis
- **Responsável principal:** Ricardo
- **Revisor:** Samuel

---

## Bloco 4 – Keep-alive e Métricas (PING/PONG, RTT)

### Escopo
- Enviar periodicamente PING para peers conectados.
- Receber e responder PONG.
- Calcular RTT e armazenar/atualizar métricas.

### Tarefas
- Implementar módulo `keep_alive.py`.
- Agendador/loop para envio de PING a cada ~30s (configurável).
- Cálculo de RTT a partir de `timestamp` e armazenamento (ex.: em `peer_table` ou `state`).
- Integração com logs (ex.: `[KeepAlive] Sent N PINGs | Average RTT = X ms`).

### Responsáveis
- **Responsável principal:** Samuel
- **Revisor:** Ricardo

---

## Bloco 5 – CLI e Observabilidade (Logs)

### Escopo
- Implementar a interface de linha de comando (CLI) com os comandos previstos na especificação.
- Configurar logging (nível, formato, saída em console/arquivo).

### Tarefas
- Implementar módulo `cli.py`.
- Comandos mínimos:
  - `/peers [* | #namespace]`
  - `/msg <peer_id> <mensagem>`
  - `/pub * <mensagem>`
  - `/pub #<namespace> <mensagem>`
  - `/conn`
  - `/rtt`
  - `/reconnect`
  - `/log <Nível>`
  - `/quit`
- Integração do CLI com `p2p_client.py` (loop principal de leitura de comandos).
- Configurar logging global em `main.py` (níveis: DEBUG, INFO, WARNING...).

### Responsáveis
- **Responsável principal:** Samuel
- **Revisor:** Ricardo

---

## Cronograma Sugerido (Alto Nível)

1. **Semana 1**
   - Samuel: Bloco 1 (Rendezvous) – primeira versão funcional.
   - Ricardo: Bloco 2 (Conexões TCP) – servidor/cliente TCP e HELLO/HELLO_OK.

2. **Semana 2**
   - Ricardo: Bloco 3 (Mensageria) – SEND/ACK, PUB.
   - Samuel: Bloco 4 (Keep-alive) – PING/PONG, RTT.

3. **Semana 3**
   - Samuel: Bloco 5 (CLI e Logs) – comandos principais e observabilidade.
   - Ambos: testes integrados com o Rendezvous público e refino final.

---

## Checklist de Integração Final

Antes da entrega, verificar em dupla:

- [ ] Registrar e descobrir peers pelo Rendezvous funcionando.
- [ ] Conexões TCP estabelecendo HELLO/HELLO_OK corretamente.
- [ ] Envio de `/msg` entre dois peers em máquinas/terminais diferentes.
- [ ] Envio de `/pub *` e `/pub #namespace` sendo recebido pelos outros peers.
- [ ] PING/PONG ativo e RTT aparecendo nos logs e no comando `/rtt`.
- [ ] Reconexão automática funcionando conforme limites configurados.
- [ ] `/quit` encerrando conexões com BYE/BYE_OK e UNREGISTER no Rendezvous.

---

## Combinação de Trabalho

- Sempre que um terminar seu bloco principal, revisar o código do outro (pull request, comentários ou revisão em conjunto).
- Manter um padrão de nomenclatura e estilo de código comum.
- Usar logs abundantes durante o desenvolvimento para facilitar depuração.

Se precisarem, podemos depois detalhar este plano por arquivo e função (por exemplo, quais funções cada um implementa em cada módulo).
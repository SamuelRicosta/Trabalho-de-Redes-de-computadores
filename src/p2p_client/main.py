"""
# TRABALHO DE REDES DE COMPUTADORES - PROJETO P2P
#
# GRUPO: 12
#
# INTEGRANTES:
# 1.Samuel Ribeiro da Costa - 21103146 
# 2.Ricardo Pedrosa Ramos Filho - 242032587
#
#
# DESCRIÇÃO:
# Cliente P2P implementado em Python utilizando sockets TCP.
# O software permite comunicação descentralizada, troca de mensagens
# e descoberta de peers.
#
# REQUISITOS: Python 3.10+ (Linux)

----------------------------------------------------------------

 
@file main.py
@brief Ponto de entrada do Cliente Chat P2P (CLI).

Este script inicializa a aplicação, gerencia a interface de linha de comando
para interação com o usuário, e coordena as threads de rede e manutenção.

@usage python -m src.p2p_client.main [PORTA] [NOME]
"""

import sys
import threading
import time
import logging
import json
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timezone

from .rendezvous_connection import ClienteRendezvous
from .peer_connection import GerenciadorConexoesPeer

# Logger para este módulo
log = logging.getLogger(__name__)

# =============================================================================
# Carregamento de Configurações
# =============================================================================

def carregar_configuracao(caminho: str = "config.json") -> Dict[str, Any]:
    """
    @brief Carrega configurações do arquivo JSON.
    
    @param caminho Caminho para o arquivo de configuração.
    @return Dicionário com as configurações ou valores padrão se arquivo não existir.
    """
    try:
        with open(caminho, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        log.warning(f"Arquivo de configuração '{caminho}' não encontrado. Usando valores padrão.")
        return {
            "rendezvous": {"host": "127.0.0.1", "port": 8080},
            "client": {"namespace": "CIC", "ttl": 7200, "log_level": "ERROR"},
            "network": {"discover_interval": 10, "ping_interval": 30, "connection_timeout": 5.0}
        }
    except json.JSONDecodeError as e:
        log.error(f"Erro ao parsear JSON: {e}. Usando valores padrão.")
        return {
            "rendezvous": {"host": "127.0.0.1", "port": 8080},
            "client": {"namespace": "CIC", "ttl": 7200, "log_level": "ERROR"},
            "network": {"discover_interval": 10, "ping_interval": 30, "connection_timeout": 5.0}
        }

# Carrega configurações do arquivo
CONFIG = carregar_configuracao()

# Configuração de Log baseada no config.json
logging.basicConfig(
    level=getattr(logging, CONFIG["client"]["log_level"], logging.ERROR),
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Constantes extraídas do JSON
NAMESPACE = CONFIG["client"]["namespace"]
RENDEZVOUS_HOST = CONFIG["rendezvous"]["host"]
RENDEZVOUS_PORT = CONFIG["rendezvous"]["port"]
TTL = CONFIG["client"]["ttl"]
DISCOVER_INTERVAL = CONFIG["network"]["discover_interval"]

def exibir_mensagem(id_peer: str, msg: dict) -> None:
    """
    @brief Callback executado quando uma nova mensagem é recebida.
    
    Esta função é passada para o GerenciadorConexoesPeer e é invocada
    sempre que chega um pacote SEND ou PUB, imprimindo-o na tela do usuário.

    @param id_peer ID do peer que enviou a mensagem.
    @param msg Dicionário contendo os dados da mensagem (payload, tipo, etc).
    """
    tipo = msg.get("type")
    payload = msg.get("payload", "")
    src = msg.get("src", id_peer)
    
    if tipo == "SEND":
        print(f"\n[DM] {src} diz: {payload}")
        print("You> ", end="", flush=True) # Restaura o prompt
        
    elif tipo == "PUB":
        dst = msg.get("dst", "*")
        print(f"\n[PUB {dst}] {src} diz: {payload}")
        print("You> ", end="", flush=True) # Restaura o prompt

def tarefa_background(
    cliente_rdv: ClienteRendezvous, 
    gerenciador: GerenciadorConexoesPeer, 
    intervalo: int = 10
) -> None:
    """
    @brief Tarefa de manutenção executada em thread secundária (Daemon).
    
    Realiza periodicamente:
    1. REGISTER: Renova registro no Rendezvous antes de expirar o TTL.
    2. DISCOVER: Consulta o Rendezvous para encontrar novos peers.
    3. Auto-Connect: Tenta conectar a peers desconhecidos encontrados.
    4. Keep-Alive: Envia PING para todos os peers conectados.

    @param cliente_rdv Instância do cliente de comunicação com Rendezvous.
    @param gerenciador Instância do gerenciador de conexões P2P.
    @param intervalo Tempo em segundos entre os ciclos de manutenção.
    """
    contador_ciclos = 0
    ciclos_para_renovar = (cliente_rdv.ttl // 2) // intervalo  # Renova no meio do TTL
    
    while True:
        try:
            # 1. Renova REGISTER periodicamente (antes de expirar o TTL)
            if contador_ciclos % ciclos_para_renovar == 0:
                try:
                    cliente_rdv.registrar()
                    log.info("Registro renovado no Rendezvous")
                except Exception:
                    pass  # Ignora erros de renovação
            
            contador_ciclos += 1
            
            # 2. Busca peers no servidor
            peers = cliente_rdv.descobrir(namespace=NAMESPACE)
            meu_id = f"{cliente_rdv.nome}@{cliente_rdv.namespace}"
            
            # 3. Conecta a novos peers
            for p in peers:
                pid = f"{p['name']}@{p['namespace']}"
                if pid == meu_id: 
                    continue # Não conectar a si mesmo
                
                # Se não estiver conectado, tenta conectar
                if not gerenciador.esta_conectado(pid):
                    # O print abaixo está comentado para não poluir o chat, 
                    # mas pode ser útil para debug.
                    # print(f"[Auto] Conectando a {pid}...")
                    gerenciador.conectar_a_peer(pid, p['ip'], int(p['port']))
            
            # 3. Envia PING para manter conexões vivas (Keep-Alive)
            conexoes = gerenciador.listar_conexoes()
            for conn in conexoes:
                gerenciador.enviar_ping(conn.id_peer)
                
        except Exception:
            # Ignora erros na thread de background para não travar o programa
            pass
            
        time.sleep(intervalo)

def main() -> None:
    """
    @brief Função principal (Entry Point).
    
    Responsável por:
    - Processar argumentos da linha de comando.
    - Inicializar componentes de rede.
    - Iniciar o servidor TCP local.
    - Registrar o peer no Rendezvous.
    - Iniciar a thread de background.
    - Executar o loop de comandos do usuário (CLI).
    - Realizar a limpeza (Unregister/Bye) ao sair.
    """
    # 1. Configuração via Argumentos
    porta = 5001
    nome = "aluno"
    
    if len(sys.argv) > 1:
        try:
            porta = int(sys.argv[1])
        except ValueError:
            print("Erro: A porta deve ser um número inteiro.")
            return

    if len(sys.argv) > 2:
        nome = sys.argv[2]
        
    print(f"--- Iniciando Chat P2P ---")
    print(f"Eu sou: {nome}@{NAMESPACE} na porta {porta}")
    print(f"Servidor Rendezvous: {RENDEZVOUS_HOST}:{RENDEZVOUS_PORT}")
    print("Comandos disponíveis: /msg, /pub, /peers, /rtt, /quit")

    # 2. Inicializa Componentes
    cliente_rdv = ClienteRendezvous(
        host=RENDEZVOUS_HOST, 
        porta=RENDEZVOUS_PORT, 
        namespace=NAMESPACE, 
        nome=nome, 
        porta_escuta=porta,
        ttl=TTL
    )
    
    gerenciador = GerenciadorConexoesPeer(
        meu_id_peer=f"{nome}@{NAMESPACE}", 
        porta_escuta=porta, 
        callback_mensagem=exibir_mensagem
    )
    
    # 3. Inicia Servidor e Registro
    try:
        gerenciador.iniciar_servidor()
        cliente_rdv.registrar()
        print("✅ Registrado no Rendezvous com sucesso!")
    except Exception as e:
        print(f"❌ Erro fatal ao iniciar: {e}")
        print("Verifique se o servidor Rendezvous está rodando.")
        return

    # 4. Inicia Thread de Background (Discover + Ping)
    t_bg = threading.Thread(
        target=tarefa_background, 
        args=(cliente_rdv, gerenciador, DISCOVER_INTERVAL), 
        daemon=True
    )
    t_bg.start()

    # 5. Loop Principal (CLI)
    try:
        while True:
            # Input bloqueante espera o usuário digitar
            cmd = input("You> ").strip()
            if not cmd: continue
            
            # Separa comando e argumentos
            partes = cmd.split(" ", 2)
            comando = partes[0].lower()
            
            # --- Comando QUIT ---
            if comando == "/quit":
                break
                
            # --- Comando PEERS ---
            elif comando == "/peers":
                peers = cliente_rdv.tabela_peers.listar_todos()
                print(f"--- Peers Descobertos ({len(peers)}) ---")
                if not peers:
                    print(" Nenhum peer descoberto ainda.")
                else:
                    for p in peers:
                        status = "CONECTADO" if gerenciador.esta_conectado(p.id_peer) else "Disponível"
                        print(f" - {p.id_peer} [{p.ip}:{p.porta}] {status}")

            # --- Comando RTT (Latência) ---
            elif comando == "/rtt":
                conns = gerenciador.listar_conexoes()
                print(f"--- Latência (RTT Médio) ---")
                if not conns:
                    print(" Nenhuma conexão ativa.")
                for c in conns:
                    # Formata para mostrar 2 casas decimais (ex: 45.20 ms)
                    media = c.obter_rtt_medio()
                    print(f" {c.id_peer}: {media:.2f} ms")
            
            # --- Comando CONN (Conexões Ativas) ---
            elif comando == "/conn":
                conns = gerenciador.listar_conexoes()
                print(f"--- Conexões Ativas ({len(conns)}) ---")
                if not conns:
                    print(" Nenhuma conexão estabelecida.")
                else:
                    inbound = [c for c in conns if c.tipo == "inbound"]
                    outbound = [c for c in conns if c.tipo == "outbound"]
                    
                    if inbound:
                        print(f"\nINBOUND ({len(inbound)}):")
                        for c in inbound:
                            duracao = (datetime.now(timezone.utc) - c.conectado_em).total_seconds()
                            minutos = int(duracao // 60)
                            segundos = int(duracao % 60)
                            print(f"   {c.id_peer} <- {c.ip}:{c.porta} (há {minutos}m{segundos}s)")
                    
                    if outbound:
                        print(f"\nOUTBOUND ({len(outbound)}):")
                        for c in outbound:
                            duracao = (datetime.now(timezone.utc) - c.conectado_em).total_seconds()
                            minutos = int(duracao // 60)
                            segundos = int(duracao % 60)
                            print(f"   {c.id_peer} -> {c.ip}:{c.porta} (há {minutos}m{segundos}s)")
                    
            # --- Comando MSG (Direct Message) ---
            elif comando == "/msg":
                # Sintaxe: /msg <id_peer> <mensagem>
                if len(partes) < 3:
                    print("Uso incorreto. Tente: /msg <id_peer> <mensagem>")
                    continue
                
                destino = partes[1]
                texto = partes[2]
                
                if gerenciador.enviar_msg_direta(destino, texto):
                    print(f"Enviado para {destino}")
                else:
                    print(f"Falha no envio. Verifique se está conectado a {destino}.")
                    print("Dica: Use /peers para ver quem está online.")
            
            # --- Comando PUB (Broadcast) ---
            elif comando == "/pub":
                # Sintaxe: /pub <mensagem>
                if len(partes) < 2:
                    print("Uso incorreto. Tente: /pub <mensagem>")
                    continue
                
                texto = cmd.split(" ", 1)[1] # Pega tudo após o comando
                gerenciador.enviar_broadcast(texto, NAMESPACE)
                print(f"Broadcast enviado para o canal #{NAMESPACE}")
            
            # --- Comando Desconhecido ---
            else:
                print("Comando inválido. Tente: /msg, /pub, /peers, /conn, /rtt ou /quit")
                
    except KeyboardInterrupt:
        print("\nInterrupção detectada (Ctrl+C).")
        
    finally:
        # 6. Encerramento Gracioso
        print("\nEncerrando aplicação...")
        
        print("Desconectando de peers...")
        sys.stdout.flush()  # Força flush do buffer
        
        gerenciador.parar_servidor()
        
        # Aguarda mensagens BYE serem processadas e exibidas
        time.sleep(1.0)
        
        try:
            print("Removendo registro no Rendezvous...")
            cliente_rdv.desregistrar()
        except Exception as e:
            print(f"Erro ao desregistrar: {e}")
            
        print("Até logo!")

if __name__ == "__main__":
    main()
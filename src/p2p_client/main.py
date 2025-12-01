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
from typing import List

from .rendezvous_connection import ClienteRendezvous
from .peer_connection import GerenciadorConexoesPeer

# =============================================================================
# Configurações Globais
# =============================================================================

# Configuração de Log: 'ERROR' para limpar o terminal para o chat.
# Mude para 'INFO' ou 'DEBUG' se precisar diagnosticar problemas.
logging.basicConfig(level=logging.ERROR, format="%(asctime)s [%(levelname)s] %(message)s")

# Constantes de Conexão
NAMESPACE = "CIC"

# @note Altere para o IP Público do servidor se não estiver testando localmente.
# Para teste local (mesma máquina), use "127.0.0.1".
#RENDEZVOUS_HOST = "127.0.0.1" 
RENDEZVOUS_HOST = "45.171.101.167"
RENDEZVOUS_PORT = 8080

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
        print(f"\n📨 [DM] {src} diz: {payload}")
        print("You> ", end="", flush=True) # Restaura o prompt
        
    elif tipo == "PUB":
        dst = msg.get("dst", "*")
        print(f"\n📢 [PUB {dst}] {src} diz: {payload}")
        print("You> ", end="", flush=True) # Restaura o prompt

def tarefa_background(
    cliente_rdv: ClienteRendezvous, 
    gerenciador: GerenciadorConexoesPeer, 
    intervalo: int = 10
) -> None:
    """
    @brief Tarefa de manutenção executada em thread secundária (Daemon).
    
    Realiza periodicamente:
    1. DISCOVER: Consulta o Rendezvous para encontrar novos peers.
    2. Auto-Connect: Tenta conectar a peers desconhecidos encontrados.
    3. Keep-Alive: Envia PING para todos os peers conectados.

    @param cliente_rdv Instância do cliente de comunicação com Rendezvous.
    @param gerenciador Instância do gerenciador de conexões P2P.
    @param intervalo Tempo em segundos entre os ciclos de manutenção.
    """
    while True:
        try:
            # 1. Busca peers no servidor
            peers = cliente_rdv.descobrir(namespace=NAMESPACE)
            meu_id = f"{cliente_rdv.nome}@{cliente_rdv.namespace}"
            
            # 2. Conecta a novos peers
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
        porta_escuta=porta
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
        args=(cliente_rdv, gerenciador), 
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
                conns = gerenciador.listar_conexoes()
                print(f"--- Peers Conectados ({len(conns)}) ---")
                for c in conns:
                    print(f" - {c.id_peer} [{c.ip}:{c.porta}] (Desde {c.conectado_em.strftime('%H:%M:%S')})")

            # --- Comando RTT (Latência) ---
            elif comando == "/rtt":
                conns = gerenciador.listar_conexoes()
                print(f"--- Latência (RTT Médio) ---")
                if not conns:
                    print(" Nenhuma conexão ativa.")
                for c in conns:
                    # Formata para mostrar 2 casas decimais (ex: 45.20 ms)
                    media = c.obter_rtt_medio()
                    print(f" ⏱️  {c.id_peer}: {media:.2f} ms")
                    
            # --- Comando MSG (Direct Message) ---
            elif comando == "/msg":
                # Sintaxe: /msg <id_peer> <mensagem>
                if len(partes) < 3:
                    print("Uso incorreto. Tente: /msg <id_peer> <mensagem>")
                    continue
                
                destino = partes[1]
                texto = partes[2]
                
                if gerenciador.enviar_msg_direta(destino, texto):
                    print(f"➝ Enviado para {destino}")
                else:
                    print(f"⚠️ Falha no envio. Verifique se está conectado a {destino}.")
                    print("Dica: Use /peers para ver quem está online.")
            
            # --- Comando PUB (Broadcast) ---
            elif comando == "/pub":
                # Sintaxe: /pub <mensagem>
                if len(partes) < 2:
                    print("Uso incorreto. Tente: /pub <mensagem>")
                    continue
                
                texto = cmd.split(" ", 1)[1] # Pega tudo após o comando
                gerenciador.enviar_broadcast(texto, NAMESPACE)
                print(f"➝ Broadcast enviado para o canal #{NAMESPACE}")
            
            # --- Comando Desconhecido ---
            else:
                print("Comando inválido. Tente: /msg, /pub, /peers, /rtt ou /quit")
                
    except KeyboardInterrupt:
        print("\nInterrupção detectada (Ctrl+C).")
        
    finally:
        # 6. Encerramento Gracioso
        print("\nEncerrando aplicação...")
        try:
            print("Removendo registro no Rendezvous...")
            cliente_rdv.desregistrar()
        except Exception as e:
            print(f"Erro ao desregistrar: {e}")
            
        print("Parando servidor P2P...")
        gerenciador.parar_servidor()
        print("Até logo!")

if __name__ == "__main__":
    main()
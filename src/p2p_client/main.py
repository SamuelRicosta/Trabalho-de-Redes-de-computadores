"""Ponto de entrada simples para o cliente P2P.

Este main cuida de:
- Registrar o peer no servidor Rendezvous;
- Iniciar servidor TCP para aceitar conexões de outros peers;
- Fazer DISCOVER periódico para atualizar a tabela de peers;
- Conectar automaticamente aos peers descobertos;
- Exibir os peers conhecidos e conexões ativas;
- Fazer UNREGISTER e BYE ao encerrar.

Execução a partir da raiz do projeto:

    python -m src.p2p_client.main
"""

from __future__ import annotations

import signal
import sys
import time
import logging
from dataclasses import dataclass

from .rendezvous_connection import ClienteRendezvous
from .peer_connection import GerenciadorConexoesPeer

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


@dataclass
class ConfigCliente:
    host_rendezvous: str = "pyp2p.mfcaetano.cc"
    porta_rendezvous: int = 8080
    namespace: str = "CIC"
    nome: str = "samuel"
    porta_escuta: int = 5001
    ttl: int = 1800
    intervalo_discover: int = 15  # segundos


_executando = True


def _tratar_sigint(signum, frame):  # type: ignore[override]
    global _executando
    print("\n[main] Sinal de interrupção recebido, encerrando...")
    _executando = False


def callback_mensagem_peer(id_peer: str, mensagem: dict) -> None:
    """Callback chamado quando recebe mensagem de um peer."""
    tipo = mensagem.get("type")
    print(f"[peer] Mensagem de {id_peer}: {tipo}")


def main() -> None:
    # Configuração inicial (no futuro pode vir de args/arquivo)
    config = ConfigCliente()

    meu_id_peer = f"{config.nome}@{config.namespace}"

    # Cliente Rendezvous
    cliente_rdv = ClienteRendezvous(
        host=config.host_rendezvous,
        porta=config.porta_rendezvous,
        namespace=config.namespace,
        nome=config.nome,
        porta_escuta=config.porta_escuta,
        ttl=config.ttl,
    )

    # Gerenciador de conexões P2P
    gerenciador_peers = GerenciadorConexoesPeer(
        meu_id_peer=meu_id_peer,
        porta_escuta=config.porta_escuta,
        callback_mensagem=callback_mensagem_peer,
    )

    # Captura Ctrl+C para poder fazer UNREGISTER e BYE limpos
    signal.signal(signal.SIGINT, _tratar_sigint)

    print("[main] Iniciando cliente P2P...")
    print(f"[main] ID: {meu_id_peer}")
    print(f"[main] Porta TCP: {config.porta_escuta}")

    # Inicia servidor TCP
    try:
        gerenciador_peers.iniciar_servidor()
    except Exception as exc:
        print(f"[main] ERRO ao iniciar servidor TCP: {exc}")
        sys.exit(1)

    # Registra no Rendezvous
    print(
        f"[main] Registrando no Rendezvous "
        f"{config.host_rendezvous}:{config.porta_rendezvous}"
    )
    try:
        resp = cliente_rdv.registrar()
        print("[main] REGISTER OK:", resp)
    except Exception as exc:
        print("[main] ERRO ao registrar no Rendezvous:", exc)
        gerenciador_peers.parar_servidor()
        sys.exit(1)

    try:
        ultimo_discover = 0.0
        while _executando:
            agora = time.time()
            
            # DISCOVER periódico
            if agora - ultimo_discover >= config.intervalo_discover:
                print("\n[main] ========== DISCOVER ==========")
                try:
                    peers = cliente_rdv.descobrir(namespace=config.namespace)
                    print(f"[main] {len(peers)} peer(s) no Rendezvous:")
                    
                    for info in cliente_rdv.tabela_peers.listar_todos():
                        # Não conecta a si mesmo
                        if info.id_peer == meu_id_peer:
                            print(f"   - {info.id_peer} @ {info.ip}:{info.porta} (eu)")
                            continue
                        
                        conectado = gerenciador_peers.esta_conectado(info.id_peer)
                        status = "✓ conectado" if conectado else "✗ desconectado"
                        print(f"   - {info.id_peer} @ {info.ip}:{info.porta} [{status}]")
                        
                        # Tenta conectar se ainda não está conectado
                        if not conectado:
                            print(f"[main] Tentando conectar a {info.id_peer}...")
                            sucesso = gerenciador_peers.conectar_a_peer(
                                info.id_peer, info.ip, info.porta
                            )
                            if sucesso:
                                print(f"[main] ✓ Conectado a {info.id_peer}")
                            else:
                                print(f"[main] ✗ Falha ao conectar a {info.id_peer}")
                    
                    # Exibe conexões ativas
                    conexoes = gerenciador_peers.listar_conexoes()
                    print(f"\n[main] Conexões ativas: {len(conexoes)}")
                    for conn in conexoes:
                        print(f"   - {conn.id_peer} ({conn.tipo}) desde {conn.conectado_em.strftime('%H:%M:%S')}")
                    
                except Exception as exc:
                    print("[main] ERRO em DISCOVER:", exc)
                
                ultimo_discover = agora

            time.sleep(1)

    finally:
        print("\n[main] Encerrando...")
        
        # Para servidor e desconecta todos os peers
        gerenciador_peers.parar_servidor()
        
        # Desregistra do Rendezvous
        print("[main] Fazendo UNREGISTER...")
        try:
            resp = cliente_rdv.desregistrar()
            print("[main] UNREGISTER resp:", resp)
        except Exception as exc:
            print("[main] ERRO ao fazer UNREGISTER:", exc)

        print("[main] Encerrado.")


if __name__ == "__main__":
    main()

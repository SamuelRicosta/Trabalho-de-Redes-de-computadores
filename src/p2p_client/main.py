"""Ponto de entrada simples para o cliente P2P.

Por enquanto, este main só cuida de:
- Registrar o peer no servidor Rendezvous;
- Fazer DISCOVER periódico para atualizar a tabela de peers;
- Exibir os peers conhecidos;
- Fazer UNREGISTER ao encerrar.

Mais adiante, este módulo será integrado com a CLI e com os módulos de
conexão P2P (Blocos 2 e 3).

Execução a partir da raiz do projeto:

    python -m src.p2p_client.main
"""

from __future__ import annotations

import signal
import sys
import time
from dataclasses import dataclass

from .rendezvous_connection import ClienteRendezvous


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


def main() -> None:
    # Configuração inicial (no futuro pode vir de args/arquivo)
    config = ConfigCliente()

    cliente = ClienteRendezvous(
        host=config.host_rendezvous,
        porta=config.porta_rendezvous,
        namespace=config.namespace,
        nome=config.nome,
        porta_escuta=config.porta_escuta,
        ttl=config.ttl,
    )

    # Captura Ctrl+C para poder fazer UNREGISTER limpo
    signal.signal(signal.SIGINT, _tratar_sigint)

    print("[main] Iniciando cliente P2P...")
    print(
        f"[main] Registrando como {config.nome}@{config.namespace} "
        f"no Rendezvous {config.host_rendezvous}:{config.porta_rendezvous}"
    )

    try:
        resp = cliente.registrar()
        print("[main] REGISTER OK:", resp)
    except Exception as exc:
        print("[main] ERRO ao registrar no Rendezvous:", exc)
        sys.exit(1)

    try:
        ultimo_discover = 0.0
        while _executando:
            agora = time.time()
            if agora - ultimo_discover >= config.intervalo_discover:
                print("\n[main] Fazendo DISCOVER...")
                try:
                    peers = cliente.descobrir(namespace=config.namespace)
                    print(f"[main] {len(peers)} peer(s) encontrados:")
                    for info in cliente.tabela_peers.listar_todos():
                        print(
                            "   -",
                            info.id_peer,
                            "@",
                            f"{info.ip}:{info.porta}",
                            f"ttl={info.ttl}",
                            f"expira_em={info.expira_em}s",
                        )
                except Exception as exc:
                    print("[main] ERRO em DISCOVER:", exc)
                ultimo_discover = agora

            time.sleep(1)

    finally:
        print("\n[main] Fazendo UNREGISTER...")
        try:
            resp = cliente.desregistrar()
            print("[main] UNREGISTER resp:", resp)
        except Exception as exc:
            print("[main] ERRO ao fazer UNREGISTER:", exc)

        print("[main] Encerrado.")


if __name__ == "__main__":
    main()

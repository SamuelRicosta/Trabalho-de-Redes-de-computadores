"""Teste simples de integração com o servidor Rendezvous público.

Este script NÃO faz parte da entrega final necessariamente, mas serve
para validar o Bloco 1: REGISTER, DISCOVER, UNREGISTER.

Para executar a partir da raiz do projeto:

    python -m src.p2p_client.test_rendezvous_client

Certifique-se de que o servidor rendezvous público está acessível:
    host: pyp2p.mfcaetano.cc
    porta: 8080
"""

from __future__ import annotations

import time

from .rendezvous_connection import ClienteRendezvous


def main() -> None:
    # Ajuste "namespace" e "nome" conforme desejado
    cliente = ClienteRendezvous(
        host="pyp2p.mfcaetano.cc",
        porta=8080,
        namespace="CIC",
        nome="samuel-test",
        porta_escuta=5001,
        ttl=1800,
    )

    print("[1] REGISTER...")
    resp = cliente.registrar()
    print("REGISTER resp:", resp)

    time.sleep(1)

    print("[2] DISCOVER...")
    peers = cliente.descobrir(namespace="CIC")
    print("DISCOVER peers:")
    for p in peers:
        print("  ", p)

    print("[2b] TabelaPeers interna:")
    for info_peer in cliente.tabela_peers.listar_todos():
        print("  ", info_peer.id_peer, info_peer.ip, info_peer.porta)

    time.sleep(1)

    print("[3] UNREGISTER...")
    resp = cliente.desregistrar()
    print("UNREGISTER resp:", resp)


if __name__ == "__main__":
    main()

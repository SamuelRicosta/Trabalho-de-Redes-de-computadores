from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class InfoPeer:
    ip: str
    porta: int
    nome: str
    namespace: str
    ttl: int
    expira_em: int

    @property
    def id_peer(self) -> str:
        return f"{self.nome}@{self.namespace}"


class TabelaPeers:
    """Tabela simples de peers mantida pelo cliente.

    Esta estrutura é preenchida principalmente a partir das respostas DISCOVER
    do servidor Rendezvous e usada depois para abrir conexões TCP P2P.
    """

    def __init__(self) -> None:
        self._peers: Dict[str, InfoPeer] = {}

    def atualizar_do_discover(self, peers: List[dict]) -> None:
        """Atualiza/insere peers a partir da lista retornada em DISCOVER."""
        for p in peers:
            try:
                info = InfoPeer(
                    ip=p["ip"],
                    porta=int(p["port"]),
                    nome=p["name"],
                    namespace=p["namespace"],
                    ttl=int(p["ttl"]),
                    expira_em=int(p.get("expires_in", 0)),
                )
            except (KeyError, ValueError, TypeError):
                # Ignora entradas malformadas
                continue

            self._peers[info.id_peer] = info

    def listar_todos(self) -> List[InfoPeer]:
        return list(self._peers.values())

    def listar_namespace(self, namespace: str) -> List[InfoPeer]:
        return [p for p in self._peers.values() if p.namespace == namespace]

    def obter_por_id(self, id_peer: str) -> Optional[InfoPeer]:
        return self._peers.get(id_peer)

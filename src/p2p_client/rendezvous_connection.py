import json
import socket
from typing import List, Optional

from .peer_table import TabelaPeers


class ClienteRendezvous:
    """Cliente para o servidor Rendezvous.

    Responsabilidades principais:
    - REGISTER: registrar o peer (namespace, nome, porta de escuta, ttl).
    - DISCOVER: obter lista de peers e atualizar a TabelaPeers local.
    - UNREGISTER: remover o registro do peer no encerramento.

    O protocolo segue o formato implementado em `src/rendezvous/request_handler.py`.
    """

    def __init__(
        self,
        host: str,
        porta: int,
        namespace: str,
        nome: str,
        porta_escuta: int,
        ttl: int = 7200,
        timeout: float = 5.0,
    ) -> None:
        self.host = host
        self.porta = porta
        self.namespace = namespace
        self.nome = nome
        self.porta_escuta = porta_escuta
        self.ttl = ttl
        self.timeout = timeout

        self.tabela_peers = TabelaPeers()

    # ----------------- helpers internos -----------------

    def _enviar_requisicao(self, payload: dict) -> dict:
        """Abre uma conexão TCP, envia JSON + "\n", lê uma linha de resposta e fecha."""
        dados = json.dumps(payload)
        dados_bytes = (dados + "\n").encode("utf-8")

        with socket.create_connection((self.host, self.porta), timeout=self.timeout) as sock:
            sock.sendall(dados_bytes)

            buffer = b""
            linha = b""
            while True:
                pedaco = sock.recv(4096)
                if not pedaco:
                    break
                buffer += pedaco
                if b"\n" in buffer:
                    linha, _resto = buffer.split(b"\n", 1)
                    break
            if not linha:
                linha = buffer

        if not linha:
            raise RuntimeError("Sem resposta do servidor Rendezvous")

        raw = linha.decode("utf-8", errors="replace")
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Resposta inválida do Rendezvous: {raw!r}") from exc

    # ----------------- API pública -----------------

    def registrar(self) -> dict:
        """Envia REGISTER e retorna o JSON de resposta.

        Lança RuntimeError em caso de status != OK.
        """
        payload = {
            "type": "REGISTER",
            "namespace": self.namespace,
            "name": self.nome,
            "port": self.porta_escuta,
            "ttl": self.ttl,
        }
        resp = self._enviar_requisicao(payload)
        if resp.get("status") != "OK":
            raise RuntimeError(f"REGISTER falhou: {resp.get('message', 'unknown_error')}")
        return resp

    def descobrir(self, namespace: Optional[str] = None) -> List[dict]:
        """Envia DISCOVER, atualiza a TabelaPeers e retorna a lista de peers (dicts)."""
        payload = {
            "type": "DISCOVER",
        }
        if namespace is not None:
            payload["namespace"] = namespace

        resp = self._enviar_requisicao(payload)
        if resp.get("status") != "OK":
            raise RuntimeError(f"DISCOVER falhou: {resp.get('message', 'unknown_error')}")

        peers = resp.get("peers", [])
        self.tabela_peers.atualizar_do_discover(peers)
        return peers

    def desregistrar(
        self,
        namespace: Optional[str] = None,
        nome: Optional[str] = None,
        porta: Optional[int] = None,
    ) -> dict:
        """Envia UNREGISTER e retorna o JSON de resposta.

        Em caso de erro, não lança exceção automaticamente (para facilitar shutdown),
        mas quem chama pode inspecionar o campo "status".
        """
        payload = {
            "type": "UNREGISTER",
            "namespace": namespace or self.namespace,
        }
        if nome is not None:
            payload["name"] = nome
        else:
            payload["name"] = self.nome

        if porta is not None:
            payload["port"] = porta
        else:
            payload["port"] = self.porta_escuta

        return self._enviar_requisicao(payload)

    def fechar(self) -> None:
        """Atalho para tentar fazer UNREGISTER no encerramento."""
        try:
            self.desregistrar()
        except Exception:
            # Em um encerramento, falhas aqui podem ser apenas logadas no futuro
            pass

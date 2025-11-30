import json
import socket
from typing import List, Optional
from .peer_table import TabelaPeers

class ClienteRendezvous:
    """
    @brief Cliente responsável pela comunicação com o Servidor Rendezvous.
    
    Esta classe gerencia o registro do peer, a descoberta de outros peers
    e a manutenção da tabela local de peers conhecidos. O protocolo utilizado
    é baseado em mensagens JSON terminadas em nova linha sobre TCP.
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
        """
        @brief Inicializa o cliente Rendezvous.

        @param host Endereço IP ou hostname do servidor Rendezvous.
        @param porta Porta TCP do servidor Rendezvous.
        @param namespace O namespace (sala) onde o peer irá se registrar.
        @param nome O nome de usuário do peer.
        @param porta_escuta A porta TCP onde este peer aceitará conexões P2P.
        @param ttl Tempo de vida do registro em segundos (padrão: 7200).
        @param timeout Tempo máximo de espera para respostas do servidor (padrão: 5.0).
        """
        self.host = host
        self.porta = porta
        self.namespace = namespace
        self.nome = nome
        self.porta_escuta = porta_escuta
        self.ttl = ttl
        self.timeout = timeout
        self.tabela_peers = TabelaPeers()

    def _enviar_requisicao(self, payload: dict) -> dict:
        """
        @brief Envia uma carga JSON para o servidor e aguarda resposta.
        
        Este método é interno e gerencia a abertura do socket, envio,
        leitura da resposta e fechamento da conexão (short-lived connection).

        @param payload Dicionário contendo os dados da requisição.
        @return Dicionário com a resposta do servidor.
        @raises RuntimeError Se houver falha na conexão ou resposta inválida.
        """
        dados = json.dumps(payload)
        dados_bytes = (dados + "\n").encode("utf-8")

        with socket.create_connection((self.host, self.porta), timeout=self.timeout) as sock:
            sock.sendall(dados_bytes)
            # ... (código de leitura do buffer)
            buffer = b""
            linha = b""
            while True:
                pedaco = sock.recv(4096)
                if not pedaco: break
                buffer += pedaco
                if b"\n" in buffer:
                    linha, _ = buffer.split(b"\n", 1)
                    break
            if not linha: linha = buffer

        if not linha:
            raise RuntimeError("Sem resposta do servidor Rendezvous")

        raw = linha.decode("utf-8", errors="replace")
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Resposta inválida do Rendezvous: {raw!r}") from exc

    def registrar(self) -> dict:
        """
        @brief Registra o peer no servidor Rendezvous.
        
        Envia o comando REGISTER com os dados do peer (IP, Porta, Nome).
        
        @return Dicionário de resposta contendo 'status' e 'ttl'.
        @raises RuntimeError Se o status da resposta não for 'OK'.
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
        """
        @brief Solicita a lista de peers de um namespace.

        Envia o comando DISCOVER e atualiza a TabelaPeers interna com os resultados.

        @param namespace O namespace a ser consultado (opcional).
        @return Lista de dicionários, onde cada dicionário representa um peer.
        @raises RuntimeError Se o status da resposta não for 'OK'.
        """
        payload = {"type": "DISCOVER"}
        if namespace is not None:
            payload["namespace"] = namespace

        resp = self._enviar_requisicao(payload)
        if resp.get("status") != "OK":
            raise RuntimeError(f"DISCOVER falhou: {resp.get('message', 'unknown_error')}")

        peers = resp.get("peers", [])
        self.tabela_peers.atualizar_do_discover(peers)
        return peers

    def desregistrar(self, namespace: Optional[str] = None, nome: Optional[str] = None) -> dict:
        """
        @brief Remove o registro do peer no servidor.
        @param namespace O namespace do registro a remover.
        @param nome O nome do peer a remover.
        @return Dicionário com a resposta do servidor.
        """
        payload = {
            "type": "UNREGISTER",
            "namespace": namespace or self.namespace,
            "name": nome or self.nome,
            "port": self.porta_escuta
        }
        return self._enviar_requisicao(payload)
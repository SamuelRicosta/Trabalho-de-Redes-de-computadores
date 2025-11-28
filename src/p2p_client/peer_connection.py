"""Módulo de conexão TCP entre peers.

Este módulo implementa:
- Servidor TCP que aceita conexões de outros peers (inbound)
- Cliente TCP que conecta em peers descobertos (outbound)
- Protocolo de handshake HELLO / HELLO_OK
- Protocolo de encerramento BYE / BYE_OK
"""

import json
import socket
import threading
import logging
import uuid
from typing import Optional, Callable
from datetime import datetime, timezone
from dataclasses import dataclass

log = logging.getLogger("peer_connection")


@dataclass
class ConexaoPeer:
    """Representa uma conexão TCP ativa com outro peer."""
    id_peer: str  # name@namespace do peer remoto
    socket: socket.socket
    ip: str
    porta: int
    tipo: str  # "inbound" ou "outbound"
    conectado_em: datetime


class GerenciadorConexoesPeer:
    """Gerencia todas as conexões TCP com outros peers (inbound e outbound)."""

    def __init__(
        self,
        meu_id_peer: str,
        porta_escuta: int,
        callback_mensagem: Optional[Callable] = None,
    ):
        """
        Args:
            meu_id_peer: Identificação deste peer (name@namespace)
            porta_escuta: Porta TCP onde o servidor irá escutar
            callback_mensagem: Função chamada ao receber mensagens de peers
        """
        self.meu_id_peer = meu_id_peer
        self.porta_escuta = porta_escuta
        self.callback_mensagem = callback_mensagem

        # Dicionário de conexões ativas: id_peer -> ConexaoPeer
        self._conexoes: dict[str, ConexaoPeer] = {}
        self._lock = threading.RLock()

        # Servidor TCP
        self._servidor_socket: Optional[socket.socket] = None
        self._servidor_rodando = False
        self._thread_servidor: Optional[threading.Thread] = None

    # ==================== Servidor TCP (Inbound) ====================

    def iniciar_servidor(self) -> None:
        """Inicia o servidor TCP que aceita conexões de outros peers."""
        if self._servidor_rodando:
            log.warning("Servidor já está rodando")
            return

        self._servidor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._servidor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._servidor_socket.bind(("0.0.0.0", self.porta_escuta))
        self._servidor_socket.listen(10)
        self._servidor_rodando = True

        self._thread_servidor = threading.Thread(
            target=self._loop_servidor,
            name=f"ServidorPeer-{self.porta_escuta}",
            daemon=True,
        )
        self._thread_servidor.start()
        log.info(f"Servidor TCP iniciado na porta {self.porta_escuta}")

    def _loop_servidor(self) -> None:
        """Loop principal do servidor TCP."""
        while self._servidor_rodando:
            try:
                cliente_socket, endereco = self._servidor_socket.accept()
                log.info(f"Conexão inbound recebida de {endereco[0]}:{endereco[1]}")

                # Cria thread para lidar com esse peer
                thread = threading.Thread(
                    target=self._processar_conexao_inbound,
                    args=(cliente_socket, endereco),
                    daemon=True,
                )
                thread.start()

            except Exception as e:
                if self._servidor_rodando:
                    log.error(f"Erro no servidor TCP: {e}")

    def _processar_conexao_inbound(
        self, cliente_socket: socket.socket, endereco: tuple
    ) -> None:
        """Processa uma conexão inbound de outro peer."""
        ip, porta = endereco
        id_peer_remoto = None

        try:
            # Espera receber HELLO do peer remoto
            mensagem = self._receber_mensagem(cliente_socket)
            if not mensagem or mensagem.get("type") != "HELLO":
                log.warning(f"Esperava HELLO de {ip}:{porta}, recebeu: {mensagem}")
                cliente_socket.close()
                return

            id_peer_remoto = mensagem.get("peer_id")
            versao = mensagem.get("version", "1.0")
            features = mensagem.get("features", [])

            log.info(
                f"HELLO recebido de {id_peer_remoto} ({ip}:{porta}) "
                f"versao={versao} features={features}"
            )

            # Responde com HELLO_OK
            resposta = {
                "type": "HELLO_OK",
                "peer_id": self.meu_id_peer,
                "version": "1.0",
                "features": ["ack", "metrics"],
                "ttl": 1,
            }
            self._enviar_mensagem(cliente_socket, resposta)
            log.info(f"HELLO_OK enviado para {id_peer_remoto}")

            # Registra a conexão
            with self._lock:
                self._conexoes[id_peer_remoto] = ConexaoPeer(
                    id_peer=id_peer_remoto,
                    socket=cliente_socket,
                    ip=ip,
                    porta=porta,
                    tipo="inbound",
                    conectado_em=datetime.now(timezone.utc),
                )

            log.info(f"Conexão inbound estabelecida com {id_peer_remoto}")

            # Loop de recebimento de mensagens
            self._loop_recebimento(cliente_socket, id_peer_remoto)

        except Exception as e:
            log.error(f"Erro ao processar conexão inbound de {ip}:{porta}: {e}")
        finally:
            if id_peer_remoto:
                self._remover_conexao(id_peer_remoto)
            try:
                cliente_socket.close()
            except:
                pass

    # ==================== Cliente TCP (Outbound) ====================

    def conectar_a_peer(self, id_peer: str, ip: str, porta: int) -> bool:
        """
        Conecta a outro peer (outbound).

        Args:
            id_peer: Identificação do peer (name@namespace)
            ip: Endereço IP do peer
            porta: Porta TCP do peer

        Returns:
            True se conectou com sucesso, False caso contrário
        """
        # Verifica se já está conectado
        with self._lock:
            if id_peer in self._conexoes:
                log.debug(f"Já conectado a {id_peer}")
                return True

        try:
            log.info(f"Conectando a {id_peer} ({ip}:{porta})...")
            sock = socket.create_connection((ip, porta), timeout=5.0)

            # Envia HELLO
            mensagem_hello = {
                "type": "HELLO",
                "peer_id": self.meu_id_peer,
                "version": "1.0",
                "features": ["ack", "metrics"],
                "ttl": 1,
            }
            self._enviar_mensagem(sock, mensagem_hello)
            log.info(f"HELLO enviado para {id_peer}")

            # Espera HELLO_OK
            resposta = self._receber_mensagem(sock)
            if not resposta or resposta.get("type") != "HELLO_OK":
                log.warning(f"Esperava HELLO_OK de {id_peer}, recebeu: {resposta}")
                sock.close()
                return False

            log.info(f"HELLO_OK recebido de {id_peer}")

            # Registra a conexão
            with self._lock:
                self._conexoes[id_peer] = ConexaoPeer(
                    id_peer=id_peer,
                    socket=sock,
                    ip=ip,
                    porta=porta,
                    tipo="outbound",
                    conectado_em=datetime.now(timezone.utc),
                )

            log.info(f"Conexão outbound estabelecida com {id_peer}")

            # Inicia thread para receber mensagens desse peer
            thread = threading.Thread(
                target=self._loop_recebimento,
                args=(sock, id_peer),
                daemon=True,
            )
            thread.start()

            return True

        except Exception as e:
            log.error(f"Erro ao conectar a {id_peer} ({ip}:{porta}): {e}")
            return False

    # ==================== Envio e Recebimento de Mensagens ====================

    def _enviar_mensagem(self, sock: socket.socket, mensagem: dict) -> None:
        """Envia uma mensagem JSON para o peer."""
        dados = json.dumps(mensagem) + "\n"
        sock.sendall(dados.encode("utf-8"))

    def _receber_mensagem(self, sock: socket.socket) -> Optional[dict]:
        """Recebe uma mensagem JSON do peer."""
        sock.settimeout(30.0)  # timeout de 30s para receber mensagem
        buffer = b""

        while True:
            chunk = sock.recv(4096)
            if not chunk:
                return None

            buffer += chunk
            if b"\n" in buffer:
                linha, _resto = buffer.split(b"\n", 1)
                break

        if not linha:
            return None

        try:
            return json.loads(linha.decode("utf-8"))
        except json.JSONDecodeError:
            log.error(f"JSON inválido recebido: {linha[:100]}")
            return None

    def _loop_recebimento(self, sock: socket.socket, id_peer: str) -> None:
        """Loop que recebe mensagens de um peer específico."""
        try:
            while True:
                mensagem = self._receber_mensagem(sock)
                if not mensagem:
                    log.info(f"Conexão fechada com {id_peer}")
                    break

                tipo = mensagem.get("type")
                log.debug(f"Mensagem recebida de {id_peer}: {tipo}")

                # Processa BYE
                if tipo == "BYE":
                    self._processar_bye(sock, id_peer, mensagem)
                    break

                # Chama callback para outras mensagens
                if self.callback_mensagem:
                    self.callback_mensagem(id_peer, mensagem)

        except Exception as e:
            log.error(f"Erro no loop de recebimento de {id_peer}: {e}")
        finally:
            self._remover_conexao(id_peer)

    # ==================== Encerramento BYE / BYE_OK ====================

    def _processar_bye(
        self, sock: socket.socket, id_peer: str, mensagem: dict
    ) -> None:
        """Processa mensagem BYE recebida de um peer."""
        motivo = mensagem.get("reason", "sem motivo")
        log.info(f"BYE recebido de {id_peer}: {motivo}")

        # Responde com BYE_OK
        resposta = {
            "type": "BYE_OK",
            "msg_id": str(uuid.uuid4()),
            "src": self.meu_id_peer,
            "dst": id_peer,
            "ttl": 1,
        }
        try:
            self._enviar_mensagem(sock, resposta)
            log.info(f"BYE_OK enviado para {id_peer}")
        except:
            pass

    def desconectar_peer(self, id_peer: str, motivo: str = "Encerrando") -> None:
        """Desconecta de um peer específico enviando BYE."""
        with self._lock:
            conexao = self._conexoes.get(id_peer)
            if not conexao:
                return

        try:
            # Envia BYE
            mensagem_bye = {
                "type": "BYE",
                "msg_id": str(uuid.uuid4()),
                "src": self.meu_id_peer,
                "dst": id_peer,
                "reason": motivo,
                "ttl": 1,
            }
            self._enviar_mensagem(conexao.socket, mensagem_bye)
            log.info(f"BYE enviado para {id_peer}")

            # Espera BYE_OK (com timeout curto)
            conexao.socket.settimeout(2.0)
            resposta = self._receber_mensagem(conexao.socket)
            if resposta and resposta.get("type") == "BYE_OK":
                log.info(f"BYE_OK recebido de {id_peer}")

        except Exception as e:
            log.debug(f"Erro ao enviar BYE para {id_peer}: {e}")
        finally:
            self._remover_conexao(id_peer)

    def _remover_conexao(self, id_peer: str) -> None:
        """Remove uma conexão da lista de conexões ativas."""
        with self._lock:
            conexao = self._conexoes.pop(id_peer, None)
            if conexao:
                try:
                    conexao.socket.close()
                except:
                    pass
                log.info(f"Conexão removida: {id_peer}")

    # ==================== Gerenciamento ====================

    def listar_conexoes(self) -> list[ConexaoPeer]:
        """Retorna lista de todas as conexões ativas."""
        with self._lock:
            return list(self._conexoes.values())

    def esta_conectado(self, id_peer: str) -> bool:
        """Verifica se está conectado a um peer específico."""
        with self._lock:
            return id_peer in self._conexoes

    def parar_servidor(self) -> None:
        """Para o servidor TCP e fecha todas as conexões."""
        log.info("Parando servidor e fechando todas as conexões...")
        self._servidor_rodando = False

        # Desconecta todos os peers
        with self._lock:
            ids_peers = list(self._conexoes.keys())

        for id_peer in ids_peers:
            self.desconectar_peer(id_peer, "Servidor encerrando")

        # Fecha servidor
        if self._servidor_socket:
            try:
                self._servidor_socket.close()
            except:
                pass

        log.info("Servidor parado")

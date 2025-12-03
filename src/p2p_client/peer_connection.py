"""
@file peer_connection.py
@brief Módulo responsável pela comunicação P2P direta (TCP).

Este módulo gerencia tanto o servidor TCP (para receber conexões) quanto
os clientes TCP (para conectar a outros peers), implementando o protocolo
de troca de mensagens JSON definido na especificação.
"""

import json
import socket
import threading
import logging
import uuid
from typing import Optional, Callable, List, Dict
from datetime import datetime, timezone
from dataclasses import dataclass, field

# Configuração de Log
log = logging.getLogger("peer_connection")

@dataclass
class ConexaoPeer:
    """
    @brief Estrutura de dados que representa uma conexão ativa.
    
    @var id_peer Identificador único do peer remoto (nome@namespace).
    @var socket Objeto socket TCP conectado.
    @var ip Endereço IP do peer remoto.
    @var porta Porta TCP do peer remoto.
    @var tipo Tipo da conexão: 'inbound' (recebida) ou 'outbound' (iniciada).
    @var conectado_em Timestamp de quando a conexão foi estabelecida.
    @var rtts Lista das últimas amostras de latência (ms).
    """
    id_peer: str
    socket: socket.socket
    ip: str
    porta: int
    tipo: str
    conectado_em: datetime
    # Modificação: Lista para armazenar histórico de RTTs
    rtts: List[float] = field(default_factory=list)

    def obter_rtt_medio(self) -> float:
        """@brief Calcula a média dos RTTs armazenados."""
        if not self.rtts: return 0.0
        return sum(self.rtts) / len(self.rtts)


class GerenciadorConexoesPeer:
    """
    @brief Gerencia conexões TCP simultâneas e troca de mensagens P2P.
    
    Esta classe atua tanto como Servidor (aceitando conexões) quanto como
    Cliente (iniciando conexões). Ela mantém um registro de conexões ativas
    e roteia mensagens recebidas para a função de callback.
    """

    def __init__(
        self,
        meu_id_peer: str,
        porta_escuta: int,
        callback_mensagem: Optional[Callable] = None,
    ):
        """
        @brief Inicializa o gerenciador de conexões.

        @param meu_id_peer ID deste peer no formato 'nome@namespace'.
        @param porta_escuta Porta TCP local para o servidor.
        @param callback_mensagem Função callback(id_peer, msg_dict) chamada ao receber mensagens.
        """
        self.meu_id_peer = meu_id_peer
        self.porta_escuta = porta_escuta
        self.callback_mensagem = callback_mensagem
        
        # Dicionário seguro para threads: id_peer -> ConexaoPeer
        self._conexoes: Dict[str, ConexaoPeer] = {}
        self._lock = threading.RLock()
        
        # Controle de peers já conectados (histórico)
        self._peers_conhecidos: set = set()
        
        # Estado do Servidor
        self._servidor_socket: Optional[socket.socket] = None
        self._servidor_rodando = False
        self._thread_servidor: Optional[threading.Thread] = None

    # =========================================================================
    # Lógica do SERVIDOR (Inbound)
    # =========================================================================

    def iniciar_servidor(self) -> None:
        """
        @brief Inicia a thread do servidor TCP para aceitar conexões.
        
        Cria um socket servidor, faz bind na porta configurada e inicia
        uma thread daemon que executa o loop de aceitação.
        """
        if self._servidor_rodando:
            log.warning("Servidor já está rodando")
            return

        try:
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
        except Exception as e:
            log.error(f"Erro ao iniciar servidor TCP: {e}")
            self._servidor_rodando = False

    def _loop_servidor(self) -> None:
        """
        @brief Loop principal do servidor TCP (Thread interna).
        Aceita novas conexões e despacha threads de tratamento para cada cliente.
        """
        while self._servidor_rodando:
            try:
                if self._servidor_socket:
                    cliente_socket, endereco = self._servidor_socket.accept()
                    log.info(f"Conexão inbound recebida de {endereco[0]}:{endereco[1]}")

                    # Cria thread para lidar com o handshake e mensagens deste peer
                    thread = threading.Thread(
                        target=self._processar_conexao_inbound,
                        args=(cliente_socket, endereco),
                        daemon=True,
                    )
                    thread.start()
            except OSError:
                # Ocorre quando o socket é fechado forçadamente no shutdown
                break
            except Exception as e:
                log.error(f"Erro no accept do servidor: {e}")

    def _processar_conexao_inbound(
        self, cliente_socket: socket.socket, endereco: tuple
    ) -> None:
        """
        @brief Processa o handshake inicial de uma conexão recebida.
        
        Espera HELLO, envia HELLO_OK e inicia o loop de recebimento.

        @param cliente_socket Socket do cliente conectado.
        @param endereco Tupla (ip, porta) do cliente.
        """
        ip, porta = endereco
        id_peer_remoto = None

        try:
            # 1. Espera receber HELLO
            mensagem = self._receber_mensagem(cliente_socket)
            if not mensagem or mensagem.get("type") != "HELLO":
                log.warning(f"Esperava HELLO de {ip}:{porta}, recebeu: {mensagem}")
                cliente_socket.close()
                return

            id_peer_remoto = mensagem.get("peer_id")
            
            # 2. Responde com HELLO_OK
            resposta = {
                "type": "HELLO_OK",
                "peer_id": self.meu_id_peer,
                "version": "1.0",
                "features": ["ack", "metrics"],
                "ttl": 1,
            }
            self._enviar_mensagem(cliente_socket, resposta)
            
            # Exibe mensagem apenas se for primeira conexão com este peer
            if id_peer_remoto not in self._peers_conhecidos:
                print(f"\n[HELLO_OK] Respondido para {id_peer_remoto}")
                print("You> ", end="", flush=True)
                self._peers_conhecidos.add(id_peer_remoto)

            # 3. Registra a conexão
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

            # 4. Entra no loop de mensagens
            self._loop_recebimento(cliente_socket, id_peer_remoto)

        except Exception as e:
            log.error(f"Erro inbound de {ip}:{porta}: {e}")
            if id_peer_remoto:
                self._remover_conexao(id_peer_remoto)
            try: cliente_socket.close()
            except: pass

    # =========================================================================
    # Lógica do CLIENTE (Outbound)
    # =========================================================================

    def conectar_a_peer(self, id_peer: str, ip: str, porta: int) -> bool:
        """
        @brief Estabelece uma conexão ativa (outbound) com um peer remoto.
        
        Realiza o handshake inicial:
        1. Abre conexão TCP.
        2. Envia mensagem HELLO.
        3. Aguarda mensagem HELLO_OK.
        4. Inicia thread de recebimento de mensagens.

        @param id_peer ID do peer alvo.
        @param ip Endereço IP do peer.
        @param porta Porta TCP do peer.
        @return True se a conexão e o handshake foram bem sucedidos, False caso contrário.
        """
        # Verifica se já existe conexão
        with self._lock:
            if id_peer in self._conexoes:
                log.debug(f"Já conectado a {id_peer}")
                return True

        try:
            log.info(f"Conectando a {id_peer} ({ip}:{porta})...")
            sock = socket.create_connection((ip, porta), timeout=5.0)

            # 1. Envia HELLO
            mensagem_hello = {
                "type": "HELLO",
                "peer_id": self.meu_id_peer,
                "version": "1.0",
                "features": ["ack", "metrics"],
                "ttl": 1,
            }
            self._enviar_mensagem(sock, mensagem_hello)

            # 2. Espera HELLO_OK
            resposta = self._receber_mensagem(sock)
            if not resposta or resposta.get("type") != "HELLO_OK":
                log.warning(f"Esperava HELLO_OK de {id_peer}, recebeu: {resposta}")
                sock.close()
                return False
            
            # Exibe mensagem apenas se for primeira conexão com este peer
            if id_peer not in self._peers_conhecidos:
                print(f"\n[HELLO_OK] Recebido de {id_peer}")
                print("You> ", end="", flush=True)
                self._peers_conhecidos.add(id_peer)

            # 3. Registra a conexão
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

            # 4. Inicia thread de escuta
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

    # =========================================================================
    # Envio de Mensagens (API Pública)
    # =========================================================================

    def enviar_msg_direta(self, id_destino: str, texto: str) -> bool:
        """
        @brief Envia uma mensagem direta (SEND) para um peer conectado.

        @param id_destino ID do peer destinatário.
        @param texto Conteúdo da mensagem.
        @return True se enviado com sucesso, False caso contrário.
        """
        with self._lock:
            conn = self._conexoes.get(id_destino)
        
        if not conn:
            log.warning(f"Tentativa de envio para peer desconectado: {id_destino}")
            return False
            
        msg = {
            "type": "SEND",
            "msg_id": str(uuid.uuid4()),
            "src": self.meu_id_peer,
            "dst": id_destino,
            "payload": texto,
            "require_ack": True,
            "ttl": 1
        }
        
        try:
            self._enviar_mensagem(conn.socket, msg)
            return True
        except Exception as e:
            log.error(f"Erro ao enviar mensagem para {id_destino}: {e}")
            self._remover_conexao(id_destino)
            return False

    def enviar_broadcast(self, texto: str, namespace: str) -> None:
        """
        @brief Envia uma mensagem PUB para TODOS os peers conectados.
        
        Esta função itera sobre todas as conexões ativas e envia a mensagem.
        Falhas em um envio não interrompem o envio para os outros.

        @param texto Conteúdo da mensagem.
        @param namespace Namespace alvo (ex: 'CIC').
        """
        msg_template = {
            "type": "PUB",
            "msg_id": str(uuid.uuid4()),
            "src": self.meu_id_peer,
            "dst": f"#{namespace}",
            "payload": texto,
            "require_ack": False,
            "ttl": 1
        }
        
        peers_conectados = self.listar_conexoes()
        for conn in peers_conectados:
            try:
                # Envia para cada peer conectado
                self._enviar_mensagem(conn.socket, msg_template)
            except Exception:
                # Loga erro mas continua o broadcast para os outros
                pass

    def enviar_ping(self, id_peer: str) -> None:
        """
        @brief Envia mensagem PING para manter a conexão viva e medir latência.
        Inclui o timestamp atual para cálculo posterior do RTT.

        @param id_peer ID do peer alvo.
        """
        with self._lock:
            conn = self._conexoes.get(id_peer)
            
        if conn:
            msg = {
                "type": "PING",
                "msg_id": str(uuid.uuid4()),
                # Modificação: Inclui timestamp atual
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "ttl": 1
            }
            try:
                self._enviar_mensagem(conn.socket, msg)
            except:
                self._remover_conexao(id_peer)

    def desconectar_peer(self, id_peer: str, motivo: str = "Encerrando") -> None:
        """
        @brief Envia BYE e fecha a conexão com um peer específico.
        
        @param id_peer ID do peer a desconectar.
        @param motivo Texto explicativo para o encerramento.
        """
        with self._lock:
            conexao = self._conexoes.get(id_peer)
            if not conexao: return

        try:
            mensagem_bye = {
                "type": "BYE",
                "msg_id": str(uuid.uuid4()),
                "src": self.meu_id_peer,
                "dst": id_peer,
                "reason": motivo,
                "ttl": 1,
            }
            self._enviar_mensagem(conexao.socket, mensagem_bye)
            print(f"[BYE] Enviado para {id_peer}: {motivo}", flush=True)
            
            # Tenta esperar o BYE_OK brevemente
            conexao.socket.settimeout(2.0)
            resp = self._receber_mensagem(conexao.socket)
            if resp and resp.get("type") == "BYE_OK":
                print(f"[BYE_OK] Recebido de {id_peer}", flush=True)
                log.info(f"BYE_OK recebido de {id_peer}")
                
        except Exception:
            pass
        finally:
            self._remover_conexao(id_peer)

    # =========================================================================
    # Loop de Recebimento e Processamento
    # =========================================================================

    def _loop_recebimento(self, sock: socket.socket, id_peer: str) -> None:
        """
        @brief Loop contínuo que recebe e processa mensagens de um peer.
        
        Trata os tipos: SEND, PUB, PING, PONG, ACK, BYE.
        Se a conexão cair, encerra o loop e remove a conexão.

        @param sock Socket conectado.
        @param id_peer ID do peer associado.
        """
        try:
            while True:
                mensagem = self._receber_mensagem(sock)
                if not mensagem:
                    log.info(f"Conexão fechada remotamente por {id_peer}")
                    break

                tipo = mensagem.get("type")
                
                # 1. PING -> Responde PONG (com echo do timestamp)
                if tipo == "PING":
                    pong = {
                        "type": "PONG",
                        "msg_id": mensagem.get("msg_id"),
                        "timestamp": mensagem.get("timestamp"), # Echo do timestamp
                        "ttl": 1
                    }
                    self._enviar_mensagem(sock, pong)

                # 2. PONG -> Calcula RTT
                elif tipo == "PONG":
                    ts_str = mensagem.get("timestamp")
                    if ts_str:
                        try:
                            ts_envio = datetime.fromisoformat(ts_str)
                            agora = datetime.now(timezone.utc)
                            rtt_ms = (agora - ts_envio).total_seconds() * 1000.0
                            
                            with self._lock:
                                conn = self._conexoes.get(id_peer)
                                if conn:
                                    conn.rtts.append(rtt_ms)
                                    # Mantém histórico das últimas 10 medições
                                    if len(conn.rtts) > 10:
                                        conn.rtts.pop(0)
                        except ValueError:
                            pass 

                # 3. SEND -> Responde ACK e avisa usuário
                elif tipo == "SEND":
                    if mensagem.get("require_ack"):
                        ack = {
                            "type": "ACK",
                            "msg_id": mensagem.get("msg_id"),
                            "ttl": 1
                        }
                        self._enviar_mensagem(sock, ack)
                    
                    if self.callback_mensagem:
                        self.callback_mensagem(id_peer, mensagem)

                # 4. PUB -> Avisa usuário
                elif tipo == "PUB":
                    if self.callback_mensagem:
                        self.callback_mensagem(id_peer, mensagem)

                # 5. BYE -> Responde BYE_OK e fecha
                elif tipo == "BYE":
                    motivo = mensagem.get("reason", "Sem motivo")
                    print(f"\n[BYE] Recebido de {id_peer}: {motivo}")
                    print("You> ", end="", flush=True)
                    log.info(f"BYE recebido de {id_peer}")
                    self._processar_bye(sock, id_peer, mensagem)
                    break # Sai do loop para fechar conexão

                # 6. Outros (ACK, HELLO_OK inesperado)
                else:
                    log.debug(f"Mensagem {tipo} recebida de {id_peer}")

        except Exception as e:
            log.error(f"Erro no loop de recebimento de {id_peer}: {e}")
        finally:
            self._remover_conexao(id_peer)

    def _processar_bye(self, sock: socket.socket, id_peer: str, mensagem: dict) -> None:
        """
        @brief Envia BYE_OK em resposta a um BYE.
        """
        try:
            resp = {
                "type": "BYE_OK",
                "msg_id": str(uuid.uuid4()),
                "src": self.meu_id_peer,
                "dst": id_peer,
                "ttl": 1
            }
            self._enviar_mensagem(sock, resp)
        except:
            pass

    # =========================================================================
    # Utilitários de Socket e Gestão
    # =========================================================================

    def _enviar_mensagem(self, sock: socket.socket, mensagem: dict) -> None:
        """
        @brief Serializa e envia um dicionário como JSON terminado em newline.
        @raises Exception Em caso de erro de socket.
        """
        dados = json.dumps(mensagem) + "\n"
        sock.sendall(dados.encode("utf-8"))

    def _receber_mensagem(self, sock: socket.socket) -> Optional[dict]:
        """
        @brief Lê do socket até encontrar uma quebra de linha e decodifica JSON.
        
        @return Dicionário da mensagem ou None se a conexão falhar/fechar.
        """
        try:
            # Em implementações reais, deve-se usar um buffer persistente.
            # Aqui usamos uma abordagem simplificada lendo chunks.
            buffer = b""
            while True:
                # Lê 1 byte por vez ou blocos pequenos até achar \n
                # Para eficiência, blocos maiores são melhores, mas requer buffer de classe.
                # Simplificação robusta para o trabalho:
                chunk = sock.recv(4096)
                if not chunk:
                    return None
                
                buffer += chunk
                if b"\n" in buffer:
                    linha, _ = buffer.split(b"\n", 1)
                    return json.loads(linha.decode("utf-8"))
        except (socket.timeout, ConnectionResetError):
            return None
        except json.JSONDecodeError:
            log.error("JSON inválido recebido")
            return None
        except Exception as e:
            log.error(f"Erro na leitura do socket: {e}")
            return None

    def _remover_conexao(self, id_peer: str) -> None:
        """
        @brief Remove peer da lista de conexões e fecha o socket com segurança.
        """
        with self._lock:
            conexao = self._conexoes.pop(id_peer, None)
            
        if conexao:
            try:
                conexao.socket.close()
            except:
                pass
            log.info(f"Conexão removida: {id_peer}")

    def listar_conexoes(self) -> List[ConexaoPeer]:
        """@brief Retorna lista de todas as conexões ativas."""
        with self._lock:
            return list(self._conexoes.values())

    def esta_conectado(self, id_peer: str) -> bool:
        """@brief Verifica se existe conexão ativa com o peer informado."""
        with self._lock:
            return id_peer in self._conexoes

    def parar_servidor(self) -> None:
        """
        @brief Encerra o servidor e todas as conexões ativas.
        """
        log.info("Parando servidor e fechando todas as conexões...")
        self._servidor_rodando = False
        
        # Fecha socket servidor para desbloquear o accept()
        if self._servidor_socket:
            try: self._servidor_socket.close()
            except: pass

        # Desconecta peers
        with self._lock:
            ids = list(self._conexoes.keys())
        
        for id_peer in ids:
            self.desconectar_peer(id_peer, "Servidor encerrando")
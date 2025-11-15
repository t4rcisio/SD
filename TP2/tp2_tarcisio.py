#!/usr/bin/env python3
"""
peer.py - Implementação simples de um sistema P2P para transferência de arquivos
Trabalho Prático 2 - Sistemas Distribuídos - CEFET-MG

Cada peer atua simultaneamente como:
- Servidor (fornece blocos para outros peers)
- Cliente (solicita blocos que ainda não possui)

Execução:

# Seeder (quem tem o arquivo completo):
python3 peer.py --port 5000 --peers 127.0.0.1:5001 --file fileA.bin --seed

# Leecher (quem vai baixar):
python3 peer.py --port 5001 --peers 127.0.0.1:5000 --outfile recebido.bin
"""

import argparse
import socket
import threading
import json
import struct
import os
import hashlib
import time
import logging


# --------------------------------------------------------------
# CONFIGURAÇÃO DOS LOGS
# --------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# --------------------------------------------------------------
# FUNÇÕES AUXILIARES DE ENVIO E RECEBIMENTO
# --------------------------------------------------------------

def enviar_frame(conn, payload: bytes):
    """
    Envia uma mensagem com cabeçalho contendo o tamanho (4 bytes).
    Isso evita problemas na rede ("message framing").
    """
    tamanho = struct.pack('!I', len(payload))
    conn.sendall(tamanho + payload)


def receber_frame(conn) -> bytes:
    """
    Recebe uma mensagem no formato:
    [4 bytes: tamanho][dados]
    """
    header = conn.recv(4)
    if not header:
        raise ConnectionError("Conexão fechada ao ler cabeçalho")

    (tam,) = struct.unpack('!I', header)
    dados = b''

    while len(dados) < tam:
        parte = conn.recv(tam - len(dados))
        if not parte:
            raise ConnectionError("Conexão interrompida durante recebimento")
        dados += parte

    return dados


# --------------------------------------------------------------
# CLASSE PRINCIPAL DO PEER
# --------------------------------------------------------------

class Peer:
    def __init__(self, host, port, vizinhos, tamanho_bloco, arquivo_seed, arquivo_saida, is_seed):
        """
        Inicializa o peer com:
        - seu IP/porta
        - lista de vizinhos
        - tamanho do bloco
        - arquivo a compartilhar (se for Seeder)
        """
        self.host = host
        self.port = port
        self.vizinhos = vizinhos
        self.tamanho_bloco = tamanho_bloco
        self.arquivo_seed = arquivo_seed
        self.arquivo_saida = arquivo_saida or "arquivo_baixado.bin"
        self.is_seed = is_seed

        # Metadados do arquivo
        self.nome_arquivo = None
        self.tamanho_arquivo = None
        self.num_blocos = None
        self.sha256_original = None

        # Armazenamento dos blocos
        self.blocos = {}      # índice → bytes
        self.tenho = set()    # conjunto com índices que este peer já possui

        # Controle de parada
        self.encerrar = threading.Event()

        logging.info(f"[Peer {self.port}] Inicializado")

    # ----------------------------------------------------------
    # PREPARAÇÃO DO SEEDER (carrega o arquivo e fragmenta)
    # ----------------------------------------------------------

    def preparar_seeder(self):
        """
        Seeder carrega o arquivo, calcula hash e fragmenta em blocos.
        """
        if not self.arquivo_seed:
            raise ValueError("Seeder precisa fornecer --file")

        self.nome_arquivo = os.path.basename(self.arquivo_seed)
        self.tamanho_arquivo = os.path.getsize(self.arquivo_seed)

        # Calcula número total de blocos
        self.num_blocos = (self.tamanho_arquivo + self.tamanho_bloco - 1) // self.tamanho_bloco

        # Calcula SHA-256 do arquivo completo
        h = hashlib.sha256()
        with open(self.arquivo_seed, "rb") as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                h.update(chunk)
        self.sha256_original = h.hexdigest()

        logging.info(f"[Seeder] Arquivo: {self.nome_arquivo}")
        logging.info(f"[Seeder] Tamanho: {self.tamanho_arquivo} bytes")
        logging.info(f"[Seeder] Blocos: {self.num_blocos}")
        logging.info(f"[Seeder] SHA-256: {self.sha256_original}")

        # Carrega os blocos na memória
        with open(self.arquivo_seed, "rb") as f:
            for i in range(self.num_blocos):
                bloco = f.read(self.tamanho_bloco)
                self.blocos[i] = bloco
                self.tenho.add(i)

    # ----------------------------------------------------------
    # SERVIDOR TCP (cada peer aceita pedidos de blocos)
    # ----------------------------------------------------------

    def servidor(self):
        """
        Servidor que fica ouvindo pedidos de outros peers.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.host, self.port))
        sock.listen(10)

        logging.info(f"[Peer {self.port}] Servidor ouvindo...")

        while not self.encerrar.is_set():
            try:
                sock.settimeout(1)
                conn, addr = sock.accept()
            except socket.timeout:
                continue

            threading.Thread(target=self.tratar_conexao, args=(conn, addr), daemon=True).start()

    # ----------------------------------------------------------
    # TRATAMENTO DE UMA CONEXÃO ENTRANTE (servidor)
    # ----------------------------------------------------------

    def tratar_conexao(self, conn, addr):
        try:
            while True:
                dados = receber_frame(conn)
                msg = json.loads(dados.decode())
                tipo = msg["type"]

                # Pedido de metadados
                if tipo == "METADATA":
                    resposta = {
                        "type": "METADATA_REPLY",
                        "filename": self.nome_arquivo,
                        "filesize": self.tamanho_arquivo,
                        "blocksize": self.tamanho_bloco,
                        "num_blocks": self.num_blocos,
                        "sha256": self.sha256_original
                    }
                    enviar_frame(conn, json.dumps(resposta).encode())

                # Pedido de bloco
                elif tipo == "REQUEST":
                    idx = msg["block_index"]
                    if idx in self.tenho:
                        # Envia header
                        header = {
                            "type": "BLOCK",
                            "block_index": idx,
                            "block_len": len(self.blocos[idx]),
                        }
                        enviar_frame(conn, json.dumps(header).encode())

                        # Envia bloco bruto
                        enviar_frame(conn, self.blocos[idx])

                        logging.info(f"[Peer {self.port}] Enviou bloco {idx} para {addr}")

                else:
                    enviar_frame(conn, json.dumps({"type": "ERROR"}).encode())

        except:
            pass
        finally:
            conn.close()

    # ----------------------------------------------------------
    # CLIENTE: baixa blocos dos vizinhos
    # ----------------------------------------------------------

    def cliente(self):
        """
        Lógica que procura vizinhos, obtém metadados e baixa blocos.
        """
        if not self.is_seed:
            self.obter_metadata()

        # Spawna uma thread por vizinho para pedir blocos
        workers = []
        for v in self.vizinhos:
            t = threading.Thread(target=self.worker_baixar, args=(v,), daemon=True)
            t.start()
            workers.append(t)

        # Acompanha progresso
        while len(self.tenho) < self.num_blocos:
            logging.info(f"[Peer {self.port}] Progresso: {len(self.tenho)}/{self.num_blocos} blocos")
            time.sleep(1)

        # Depois de completo
        logging.info(f"[Peer {self.port}] Todos os blocos recebidos!")
        self.reconstruir_arquivo()

    # ----------------------------------------------------------
    # CLIENTE: pede metadados a um vizinho
    # ----------------------------------------------------------

    def obter_metadata(self):
        """
        Pede os metadados para algum vizinho.
        """
        for viz in self.vizinhos:
            host, port = viz.split(":")
            try:
                conn = socket.create_connection((host, int(port)), timeout=2)
                enviar_frame(conn, json.dumps({"type": "METADATA"}).encode())
                resp = json.loads(receber_frame(conn).decode())

                self.nome_arquivo = resp["filename"]
                self.tamanho_arquivo = resp["filesize"]
                self.tamanho_bloco = resp["blocksize"]
                self.num_blocos = resp["num_blocks"]
                self.sha256_original = resp["sha256"]

                logging.info(f"[Peer {self.port}] Metadados recebidos de {viz}")
                conn.close()
                return
            except:
                continue

        logging.error("Não foi possível obter os metadados de nenhum vizinho.")
        exit()

    # ----------------------------------------------------------
    # WORKER: cada thread tenta baixar blocos que faltam
    # ----------------------------------------------------------

    def worker_baixar(self, viz):
        host, port = viz.split(":")
        port = int(port)

        while len(self.tenho) < self.num_blocos:
            faltam = [i for i in range(self.num_blocos) if i not in self.tenho]
            if not faltam:
                break

            idx = faltam[0]  # estratégia simples: pega o primeiro que falta

            try:
                conn = socket.create_connection((host, port), timeout=3)

                enviar_frame(conn, json.dumps({
                    "type": "REQUEST",
                    "block_index": idx
                }).encode())

                header = json.loads(receber_frame(conn).decode())

                if header["type"] == "BLOCK":
                    dados = receber_frame(conn)
                    self.blocos[idx] = dados
                    self.tenho.add(idx)

                    logging.info(f"[Peer {self.port}] Recebeu bloco {idx} de {viz}")

                conn.close()
            except:
                time.sleep(0.2)

    # ----------------------------------------------------------
    # RECONSTRUÇÃO DO ARQUIVO FINAL
    # ----------------------------------------------------------

    def reconstruir_arquivo(self):
        with open(self.arquivo_saida, "wb") as f:
            for i in range(self.num_blocos):
                f.write(self.blocos[i])

        logging.info(f"[Peer {self.port}] Arquivo reconstruído em: {self.arquivo_saida}")

        # Checar integridade
        h = hashlib.sha256()
        with open(self.arquivo_saida, "rb") as f:
            h.update(f.read())

        if h.hexdigest() == self.sha256_original:
            logging.info("[OK] SHA-256 confere! Arquivo íntegro.")
        else:
            logging.warning("[ERRO] SHA-256 não confere!")

    # ----------------------------------------------------------
    # INICIAR PEER
    # ----------------------------------------------------------

    def iniciar(self):
        """
        Corrigido: Seeder NÃO executa cliente nem encerra a thread principal.
        Ele deve continuar rodando como servidor para atender peers.
        """
        if self.is_seed:
            self.preparar_seeder()

            # Apenas inicia servidor — NUNCA roda cliente
            threading.Thread(target=self.servidor, daemon=True).start()

            logging.info(f"[Peer {self.port}] Seeder ativo. Aguardando peers...")

            # Mantém processo vivo para o servidor continuar rodando
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logging.info(f"[Peer {self.port}] Encerrando Seeder...")
                self.encerrar.set()

        else:
            # Leecher: inicia servidor e cliente normalmente
            threading.Thread(target=self.servidor, daemon=True).start()
            self.cliente()


# --------------------------------------------------------------
# PARSER DE ARGUMENTOS
# --------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--peers", default="", help="Lista de peers: ip:porta,ip:porta")
    p.add_argument("--blocksize", type=int, default=1024)
    p.add_argument("--file", help="Arquivo a compartilhar (somente se --seed)")
    p.add_argument("--seed", action="store_true")
    p.add_argument("--outfile", help="Nome do arquivo reconstruído no Leecher")
    return p.parse_args()


# --------------------------------------------------------------
# MAIN
# --------------------------------------------------------------

if __name__ == "__main__":
    args = parse_args()
    vizinhos = [x.strip() for x in args.peers.split(",") if x.strip()]

    peer = Peer(
        host=args.host,
        port=args.port,
        vizinhos=vizinhos,
        tamanho_bloco=args.blocksize,
        arquivo_seed=args.file,
        arquivo_saida=args.outfile,
        is_seed=args.seed
    )

    peer.iniciar()

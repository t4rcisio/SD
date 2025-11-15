#!/usr/bin/env python3


import os
import argparse
import hashlib


def gerar_arquivo(path, size_bytes):
    """
    Cria um arquivo binário com conteúdo aleatório.
    """
    with open(path, "wb") as f:
        # Escreve em blocos de 64 KB (evita consumo excessivo de RAM)
        restante = size_bytes
        while restante > 0:
            bloco = min(65536, restante)
            f.write(os.urandom(bloco))
            restante -= bloco

    print(f"Arquivo criado: {path}")
    print(f"Tamanho: {size_bytes} bytes ({size_bytes / 1024:.2f} KB, {size_bytes / 1024 / 1024:.2f} MB)")


def sha256_arquivo(path):
    """
    Calcula o SHA-256 do arquivo gerado.
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            bloco = f.read(65536)
            if not bloco:
                break
            h.update(bloco)
    return h.hexdigest()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gerador de arquivos aleatórios para testes P2P.")

    parser.add_argument("--outfile", required=True, help="Nome do arquivo de saída (ex: fileA.bin).")
    parser.add_argument("--kb", type=int, default=0, help="Tamanho em Kilobytes.")
    parser.add_argument("--mb", type=int, default=0, help="Tamanho em Megabytes.")
    parser.add_argument("--bytes", type=int, default=0, help="Tamanho exato em bytes.")

    args = parser.parse_args()

    # Calcula o tamanho final
    size = args.bytes + (args.kb * 1024) + (args.mb * 1024 * 1024)

    if size <= 0:
        print("Erro: informe um tamanho usando --kb, --mb ou --bytes.")
        exit(1)

    gerar_arquivo(args.outfile, size)

    # Calcula hash para verificação de integridade
    hash_final = sha256_arquivo(args.outfile)
    print("SHA-256:", hash_final)
    print("Pronto para uso nos testes!")

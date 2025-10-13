import os
import random
import math
import time


def verifica_primo(n: int) -> bool:
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    raiz = int(math.sqrt(n)) + 1
    for i in range(3, raiz, 2):
        if n % i == 0:
            return False
    return True



def produtor_consumidor(qtd_numeros: int):

    #Cria os Ids para leitura e escrita
    id_consumidor, id_produtor = os.pipe()

    # Faz o fork do processo
    pid = os.fork()

    if pid == 0: # Entra no proceso consumidor

        # Precisa fechar a conexão de escrita para o consumidor
        os.close(id_produtor)

        while True:

            # Aqui foi padronizado na leitura de 20 bytes
            data = os.read(id_consumidor, 20)
            if not data:
                break

            # Traduz os dados
            num_str = data.decode().strip()
            if not num_str:
                continue

            n = int(num_str)

            if n == 0:
                print("[Consumidor] Recebido 0. FIM")
                break

            resultado = "SIM" if verifica_primo(n) else "NÃO"

            print(f"[Consumidor] VALOR: {n} | PRIMO? {resultado}")

            time.sleep(0.5)

        # Quando finaliza, fecha a conexão de leitura
        os.close(id_consumidor)

    else: # Entra no processo produtor

        os.close(id_consumidor)  # Precisa fechar a conexão de leitura para o produtor


        for N in range(1,qtd_numeros):
            delta = random.randint(1, 100)
            N += delta

            print(f"[Produtor] Inserindo no pipe {N}.")

            #escreve uma string de exatos 20 bytes
            msg = f"{N:<20}".encode()

            # grava no pipe
            os.write(id_produtor, msg)

            time.sleep(0.5)

        # Grava 0 para encerrar o loop
        fim = f"{0:<20}".encode()
        os.write(id_produtor, fim)

        # Fecha a conexão
        os.close(id_produtor)

        # Aguarda o fim do processo filho
        os.wait()
        print("[Produtor] FIM.")


if __name__ == "__main__":
    produtor_consumidor(20)


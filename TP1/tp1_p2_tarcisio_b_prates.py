#!/usr/bin/env python3
"""
Produtor-Consumidor multithreaded com memória compartilhada.
- Buffer circular de tamanho N
- Np produtores, Nc consumidores
- Semáforos: empty, full e mutex para sincronização
- Produtores geram números em [1, 10**7]
- Consumidores testam primalidade
- Termina quando M números forem processados (consumidos)
- Script também roda experimentos para várias combinações e plota resultados
"""

import threading
import random
import math
import time
import argparse
import csv
import os
from collections import defaultdict
import matplotlib
matplotlib.use('Agg')  # Set the backend to a non-GUI one.

import matplotlib.pyplot as plt # Now import pyplot

# ---------------------------------------------------------
# Parâmetros padrão / experimento
M_DEFAULT = 100_000  # total de números a serem processados (pode ajustar)
TEST_REPEATS = 10
N_values = [1, 10, 100, 1000]
combos = [(1,1),(1,2),(1,4),(1,8),(2,1),(4,1),(8,1)]
# ---------------------------------------------------------

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


#Buffer circular compartilhado que gerencia a sincronização entre threads.
class SharedBuffer:

    def __init__(self, size: int, occupancy_log: list = None, start_time_ref=None):
        self.size = size
        self.buf = [None] * size
        self.in_idx = 0
        self.out_idx = 0
        self.count = 0  # Contador de ocupação

        self.occupancy_log = occupancy_log
        self.start_time_ref = start_time_ref

        # semáforos
        self.empty = threading.Semaphore(size)  # Semáforo para contar as posições VAZIAS. Começa cheio (size)
        self.full = threading.Semaphore(0)      # Semáforo para contar as posições OCUPADAS. Começa vazio (0).
        self.mutex = threading.Lock()           # Lock (Mutex) para garantir que apenas uma thread modifique o buffer e os índices por vez.

    def _log_occupancy(self):
        if self.occupancy_log is not None and self.start_time_ref is not None:
            # Registra uma tupla (timestamp, ocupação)
            timestamp = time.perf_counter() - self.start_time_ref[0]
            self.occupancy_log.append((timestamp, self.count))

    # Método usado pelo PRODUTOR para adicionar um item.
    def put(self, item):
        self.empty.acquire()      # Espera por uma vaga livre. Bloqueia se o buffer estiver cheio
        with self.mutex:
            self.buf[self.in_idx] = item
            self.in_idx = (self.in_idx + 1) % self.size
            self.count += 1
            self._log_occupancy()  # Log após produzir
        self.full.release()       # Sinaliza que uma nova vaga foi preenchida.

    # Método usado pelo CONSUMIDOR para retirar um item.
    def get(self):
        self.full.acquire()       # Espera por um item disponível
        with self.mutex:
            item = self.buf[self.out_idx]
            self.buf[self.out_idx] = None
            self.out_idx = (self.out_idx + 1) % self.size
            self.count -= 1
            self._log_occupancy()  # Log após consumir
        self.empty.release()      # Sinaliza que uma nova vaga ficou livre.
        return item

def run_trial(N, Np, Nc, M, seed=None, verbose=False, generate_log=False):

    if seed is not None:
        random.seed(seed)

    buf = SharedBuffer(N)  # Instancia o buffer.

    # Contadores globais e seus locks
    produced_total = 0
    produced_lock = threading.Lock()
    consumed_total = 0
    consumed_lock = threading.Lock()


    start_event = threading.Event()  # sincroniza início das threads

    # Thread produtor
    def producer_thread_fn(tid):
        nonlocal produced_total
        start_event.wait()
        while True:
            # decide se ainda precisamos produzir
            with produced_lock:
                if produced_total >= M:
                    break
                produced_total += 1
            # gera valor aleatório e coloca no buffer
            val = random.randint(1, 10**7)
            buf.put(val)
            if verbose and (produced_total % 10000 == 0):
                print(f"[P{tid}] produziu {produced_total}")

    # Thread consumidor
    def consumer_thread_fn(tid):
        nonlocal consumed_total
        start_event.wait()
        while True:
            # Se já consumimos M, não pegar mais itens
            with consumed_lock:
                if consumed_total >= M:
                    break
            # pegar um item (irá bloquear se vazio)
            item = buf.get()
            # testar primalidade (custo computacional)
            if item is None:
                break

            is_p = verifica_primo(item)
            # incrementar contador consumido
            with consumed_lock:
                consumed_total += 1
                cur = consumed_total
            if verbose and (cur % 10000 == 0):
                print(f"[C{tid}] consumiu {cur} (valor={item}, primo={is_p})")
            # se atingiu M, podemos terminar
            if cur >= M:
                break

    occupancy_log = []
    # Usamos uma lista para t0 ser mutável e acessível por _log_occupancy
    start_time_ref = [-1.0]

    if generate_log:
        buf = SharedBuffer(N, occupancy_log=occupancy_log, start_time_ref=start_time_ref)
    else:
        buf = SharedBuffer(N)

    # criar threads
    producers = [threading.Thread(target=producer_thread_fn, args=(i+1,)) for i in range(Np)]
    consumers = [threading.Thread(target=consumer_thread_fn, args=(i+1,)) for i in range(Nc)]

    # start all threads (they'll wait on start_event)
    for t in producers + consumers:
        t.daemon = True
        t.start()

    t0 = time.perf_counter()
    start_time_ref[0] = t0
    start_event.set()  # libera início simultâneo
    # aguardar término das threads
    for p in producers:
        p.join()
    # producers finished producing M items, but there might remain items to consume;
    # wait for consumers to finish (they stop when consumed_total >= M)

    for _ in range(Nc):
        buf.put(None)

    for c in consumers:
        c.join()
    tf = time.perf_counter()
    total_time = tf - t0

    # sanity checks
    if produced_total < M:
        print("Aviso: foram produzidos menos que M (problema).")
    if consumed_total < M:
        print("Aviso: foram consumidos menos que M (problema).")

    return total_time, occupancy_log

def run_experiments(M=M_DEFAULT, repeats=TEST_REPEATS, out_csv="results.csv", verbose=False):
    """Executa experimentos para todas as combinações e grava CSV com médias."""
    results = []  # linhas: (N, Np, Nc, repeat_index, time)
    summary = defaultdict(list)  # keys: (N,Np,Nc) -> list times

    total_runs = len(N_values) * len(combos) * repeats
    runcount = 0

    for N in N_values:
        for (Np, Nc) in combos:
            for r in range(repeats):
                runcount += 1
                print(f"Run {runcount}/{total_runs}: N={N} Np={Np} Nc={Nc} rep={r+1}")
                t, _ = run_trial(N, Np, Nc, M, seed=None, verbose=verbose)
                results.append((N, Np, Nc, r+1, t))
                summary[(N,Np,Nc)].append(t)

    # grava CSV com todos os tempos brutos e média
    with open(out_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["N","Np","Nc","rep","time_s"])
        for row in results:
            writer.writerow(row)

    # calcula médias por (N,Np,Nc)
    mean_results = []
    for key, times in summary.items():
        N, Np, Nc = key
        mean_t = sum(times)/len(times)
        mean_results.append((N, Np, Nc, mean_t))

    return mean_results, out_csv

def plot_results(mean_results, out_png="plot.png"):
    """Plota curvas: x-axis=combination index (ordered), uma curva por N."""
    # organizando dados por N
    data_by_N = {}
    xlabels = []
    combos_order = combos  # mantém a ordem definida
    for N in N_values:
        row = []
        for (Np, Nc) in combos_order:
            # encontrar mean_results entry
            match = next((m for m in mean_results if m[0]==N and m[1]==Np and m[2]==Nc), None)
            if match:
                row.append(match[3])
            else:
                row.append(float('nan'))
        data_by_N[N] = row

    # x labels as strings "Np,Nc"
    xlabels = [f"{a},{b}" for (a,b) in combos_order]
    x = list(range(len(xlabels)))

    plt.figure(figsize=(10,6))
    for N, times in data_by_N.items():
        plt.plot(x, times, marker='o', label=f"N={N}")
    plt.xticks(x, xlabels, rotation=45)
    plt.xlabel("Combinacao (Np,Nc)")
    plt.ylabel("Tempo medio (s) -- M="+str(M_DEFAULT))
    plt.title("Tempo medio execucao x combinacao produtor/consumidor")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_png)
    print(f"Gráfico salvo em {out_png}")

def plot_occupancy(log_data, title, filename):
    """Plota a ocupação do buffer ao longo do tempo."""
    if not log_data:
        print(f"Não há dados de log para gerar o gráfico: {filename}")
        return

    timestamps, occupancies = zip(*log_data) # Descompacta a lista de tuplas

    plt.figure(figsize=(12, 6))
    plt.plot(timestamps, occupancies, drawstyle='steps-post') # steps-post é ótimo para visualizar mudanças discretas
    plt.xlabel("Tempo (s)")
    plt.ylabel("Ocupação do Buffer")
    plt.title(title)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()
    print(f"Gráfico de ocupação salvo em: {filename}")


# ----------------------- CLI -----------------------
def main():
    parser = argparse.ArgumentParser(description="Produtor-Consumidor multithreaded + experiment runner")
    parser.add_argument("--M", type=int, default=M_DEFAULT, help="Total de itens a serem consumidos")
    parser.add_argument("--repeats", type=int, default=TEST_REPEATS, help="Repeticoes por caso")
    parser.add_argument("--csv", type=str, default="results.csv", help="Arquivo CSV de saida")
    parser.add_argument("--plot", type=str, default="plot.png", help="Arquivo PNG do grafico")
    parser.add_argument("--verbose", action="store_true", help="Imprime logs durante execucao")
    args = parser.parse_args()

    #mean_results, csv_file = run_experiments(M=args.M, repeats=args.repeats, out_csv=args.csv, verbose=args.verbose)
    #plot_results(mean_results, out_png=args.plot)
    #print("Execução finalizada. CSV:", csv_file)

    # --- Geração dos gráficos de cenários específicos ---
    print("\nGerando gráficos de ocupação para cenários específicos...")

    # Cenário 1: Eficiente e Desacoplado
    _, log1 = run_trial(N=1000, Np=1, Nc=8, M=M_DEFAULT, generate_log=True)
    plot_occupancy(log1, "Cenário 1: Eficiente (N=1000, Np=1, Nc=8)", "occupancy_efficient.png")

    # Cenário 2: Consumidores famintos (alta contenção)
    _, log2 = run_trial(N=10, Np=1, Nc=8, M=M_DEFAULT, generate_log=True)
    plot_occupancy(log2, "Cenário 2: Contenção de Consumidores (N=10, Np=1, Nc=8)", "occupancy_consumer_starved.png")

    # Cenário 3: Produtores bloqueados (gargalo no consumidor)
    _, log3 = run_trial(N=100, Np=8, Nc=1, M=M_DEFAULT, generate_log=True)
    plot_occupancy(log3, "Cenário 3: Gargalo no Consumidor (N=100, Np=8, Nc=1)", "occupancy_producer_blocked.png")

if __name__ == "__main__":
    main()

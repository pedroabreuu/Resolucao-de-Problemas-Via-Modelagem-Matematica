import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

class HeuristicaIngenuaFIFO:
    def __init__(self, num_empilhadeiras):
        self.num_empilhadeiras = num_empilhadeiras
        self.resetar()

    def resetar(self):
        self.empilhadeiras = {
            i: {
                'posicao': None,
                'livre_em': None,
                'distancia_total': 0.0,
                'distancia_sem_carga': 0.0,
                'tempo_ocioso_parado': timedelta(0),
                'tempo_ocioso_movimento': timedelta(0),
                'ordens_atendidas': []
            } for i in range(self.num_empilhadeiras)
        }
        self.tempo_atual = None
        self.ordens_pendentes = []
        self.fila_espera_esteira = []

    def esteiras_ativas(self):
        esteiras_ocupadas = set()
        if self.tempo_atual is None:
            return esteiras_ocupadas 
        for emp in self.empilhadeiras.values():
            if emp['livre_em'] and emp['livre_em'] > self.tempo_atual:
                for ordem in emp['ordens_atendidas']:
                    if ordem['hora_entrega'] > self.tempo_atual and 'Esteira' in ordem['origem']:
                        esteiras_ocupadas.add(ordem['origem'])
        return esteiras_ocupadas

    def atribuir_ordem(self, emp_id, ordem, matriz_dist):
        emp = self.empilhadeiras[emp_id]
        
        pos_anterior = emp['posicao'] if emp['posicao'] else ordem['origem']
        dist_sem_carga = matriz_dist.loc[pos_anterior, ordem['origem']]
        tempo_sem_carga = timedelta(seconds=(dist_sem_carga / 10))

        dist_com_carga = matriz_dist.loc[ordem['origem'], ordem['destino']]
        tempo_com_carga = timedelta(seconds=(dist_com_carga / 10))

        hora_inicio_movimento = max(emp['livre_em'] or self.tempo_atual, self.tempo_atual)
        
        if emp['livre_em'] and emp['livre_em'] < hora_inicio_movimento:
            emp['tempo_ocioso_parado'] += (hora_inicio_movimento - emp['livre_em'])

        hora_coleta = hora_inicio_movimento + tempo_sem_carga
        hora_entrega = hora_coleta + tempo_com_carga

        emp['posicao'] = ordem['destino']
        emp['livre_em'] = hora_entrega
        emp['distancia_total'] += dist_sem_carga + dist_com_carga
        emp['distancia_sem_carga'] += dist_sem_carga
        emp['tempo_ocioso_movimento'] += tempo_sem_carga
        
        ordem_executada = {
            **ordem.to_dict(),
            'empilhadeira': emp_id,
            'hora_saida': hora_inicio_movimento,
            'hora_coleta': hora_coleta,
            'hora_entrega': hora_entrega,
            'distancia_sem_carga': dist_sem_carga,
            'distancia_com_carga': dist_com_carga,
            'distancia_total': dist_sem_carga + dist_com_carga,
            'tempo_sem_carga': tempo_sem_carga.total_seconds(),
            'tempo_com_carga': tempo_com_carga.total_seconds()
        }
        emp['ordens_atendidas'].append(ordem_executada)

    def encontrar_proxima_empilhadeira_livre(self):
        proxima_a_liberar_id = -1
        menor_tempo_livre = datetime.max

        for emp_id, emp in self.empilhadeiras.items():
            if emp['livre_em'] is None:
                return emp_id
            
            if emp['livre_em'] < menor_tempo_livre:
                menor_tempo_livre = emp['livre_em']
                proxima_a_liberar_id = emp_id
        
        return proxima_a_liberar_id

    def processar_ordens_fifo(self, ordens, matriz_dist):
        self.resetar()

        ordens['data_hora'] = pd.to_datetime(ordens['data_hora'], errors='coerce')
        ordens = ordens.dropna(subset=['data_hora']).sort_values('data_hora').reset_index(drop=True)

        total_de_ordens = len(ordens)
        ordens_processadas_contador = 0

        matriz_dist = matriz_dist.set_index(matriz_dist.columns[0])
        matriz_dist = matriz_dist.map(lambda x: float(str(x).replace(',', '.')))

        self.ordens_pendentes = ordens.to_dict('records')
        
        i = 0
        while i < len(self.ordens_pendentes):
            ordem = pd.Series(self.ordens_pendentes[i])
            self.tempo_atual = ordem['data_hora']

            esteiras_ocupadas = self.esteiras_ativas()
            origem_e_esteira = 'Esteira' in str(ordem['origem'])
            
            if origem_e_esteira and ordem['origem'] not in esteiras_ocupadas and len(esteiras_ocupadas) >= 2:
                self.fila_espera_esteira.append(self.ordens_pendentes.pop(i))
                continue

            emp_id = self.encontrar_proxima_empilhadeira_livre()
            self.atribuir_ordem(emp_id, ordem, matriz_dist)
            
            self.ordens_pendentes.pop(i)
            
            ordens_processadas_contador += 1
            print(f"Processando: {ordens_processadas_contador}/{total_de_ordens} ordens ({ordens_processadas_contador/total_de_ordens:.1%})", end="\r")
            
            ordens_da_fila_processadas = True
            while ordens_da_fila_processadas:
                ordens_da_fila_processadas = self.tentar_processar_fila_esteira(matriz_dist)
                if ordens_da_fila_processadas:
                    ordens_processadas_contador += 1
                    print(f"Processando: {ordens_processadas_contador}/{total_de_ordens} ordens ({ordens_processadas_contador/total_de_ordens:.1%})", end="\r")
                    
        print()
        
        return self.gerar_resultados()

    def tentar_processar_fila_esteira(self, matriz_dist):
        for idx, ordem_dict in enumerate(self.fila_espera_esteira):
            ordem = pd.Series(ordem_dict)
            esteiras_ocupadas = self.esteiras_ativas()

            if ordem['origem'] not in esteiras_ocupadas and len(esteiras_ocupadas) < 2:
                emp_id = self.encontrar_proxima_empilhadeira_livre()
                
                self.tempo_atual = ordem['data_hora']
                self.atribuir_ordem(emp_id, ordem, matriz_dist)
                
                self.fila_espera_esteira.pop(idx)
                return True
        
        return False


    def gerar_resultados(self):
        resultados = []
        for emp_id, emp in self.empilhadeiras.items():
            for ordem in emp['ordens_atendidas']:
                 resultados.append({
                    'ordem': ordem['ordem'],
                    'material': ordem['material'],
                    'origem': ordem['origem'],
                    'destino': ordem['destino'],
                    'empilhadeira': emp_id,
                    'hora_criacao': ordem['data_hora'],
                    'hora_saida_empilhadeira': ordem.get('hora_saida'),
                    'hora_entrega': ordem.get('hora_entrega'),
                    'distancia_total': ordem.get('distancia_total', 0),
                    'distancia_sem_carga': ordem.get('distancia_sem_carga', 0),
                    'distancia_com_carga': ordem.get('distancia_com_carga', 0),
                    'tempo_espera': (ordem.get('hora_saida') - ordem['data_hora']).total_seconds(),
                    'tempo_movimento': (ordem.get('hora_entrega') - ordem.get('hora_saida')).total_seconds(),
                    'tempo_sem_carga': ordem.get('tempo_sem_carga', 0),
                    'tempo_com_carga': ordem.get('tempo_com_carga', 0)
                })

        metricas = {
            'total_ordens_processadas': len(resultados),
            'ordens_nao_atendidas': len(self.fila_espera_esteira),
            'distancia_total': sum(e['distancia_total'] for e in self.empilhadeiras.values()),
            'distancia_sem_carga': sum(e['distancia_sem_carga'] for e in self.empilhadeiras.values()),
            'distancia_com_carga': sum(e['distancia_total'] for e in self.empilhadeiras.values()) - sum(e['distancia_sem_carga'] for e in self.empilhadeiras.values()),
            'tempo_ocioso_parado_total': sum(e['tempo_ocioso_parado'].total_seconds() for e in self.empilhadeiras.values()),
            'tempo_ocioso_movimento_total': sum(e['tempo_ocioso_movimento'].total_seconds() for e in self.empilhadeiras.values()),
        }
        metricas['tempo_ocioso_total'] = metricas['tempo_ocioso_parado_total'] + metricas['tempo_ocioso_movimento_total']

        return pd.DataFrame(sorted(resultados, key=lambda x: x['hora_criacao'])), metricas

if __name__ == "__main__":
    ordens = pd.read_excel("ordens_unificadas.xlsx")
    matriz_dist = pd.read_excel("matriz_distancias.xlsx")

    NUM_EMPILHADEIRAS = 12

    print("\nIniciando heurística ingênua (FIFO)...")
    start_time = time.time()
    heuristica_fifo = HeuristicaIngenuaFIFO(NUM_EMPILHADEIRAS)
    rotas_fifo, metricas_fifo = heuristica_fifo.processar_ordens_fifo(ordens, matriz_dist)
    
    end_time = time.time()
    duracao_segundos = end_time - start_time

    print("\n=== RESUMO FINAL ===")
    print(f"Número de empilhadeiras: {NUM_EMPILHADEIRAS}")
    print(f"Ordens processadas: {metricas_fifo['total_ordens_processadas']}")
    print(f"Ordens não atendidas (ficaram na fila): {metricas_fifo['ordens_nao_atendidas']}")
    print(f"Distância total: {metricas_fifo['distancia_total']:.2f}m")
    print(f"Distância sem carga: {metricas_fifo['distancia_sem_carga']:.2f}m")
    print(f"Distância com carga: {metricas_fifo['distancia_com_carga']:.2f}m")
    print(f"Tempo ocioso total: {timedelta(seconds=metricas_fifo['tempo_ocioso_total'])}")
    print(f"  - Parado: {timedelta(seconds=metricas_fifo['tempo_ocioso_parado_total'])}")
    print(f"  - Em movimento sem carga (em segundos): {metricas_fifo['tempo_ocioso_movimento_total']:.2f}")
    print(f"Tempo total de execução: {timedelta(seconds=duracao_segundos)}")

    rotas_fifo.to_excel("resultados_heuristica_fifo_detalhado.xlsx", index=False)
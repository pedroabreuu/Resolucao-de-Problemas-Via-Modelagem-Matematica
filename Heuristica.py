import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

class Otimizador:
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
        self.ordens_nao_atendidas = []
        self.fila_espera_prioritaria = []
        self.fila_estoque = []
        self.tempo_atual = None

    def esteiras_ativas(self):
        esteiras_ocupadas = set()
        for emp in self.empilhadeiras.values():
            if emp['livre_em'] and emp['livre_em'] > self.tempo_atual:
                for ordem in emp['ordens_atendidas']:
                    if ordem['hora_entrega'] > self.tempo_atual and 'Esteira' in ordem['origem']:
                        esteiras_ocupadas.add(ordem['origem'])
        return esteiras_ocupadas

    def otimizar(self, ordens, matriz_dist):
        self.resetar()

        ordens['data_hora'] = pd.to_datetime(ordens['data_hora'], errors='coerce')
        ordens = ordens.dropna(subset=['data_hora']).sort_values('data_hora').reset_index(drop=True)

        total_de_ordens = len(ordens)

        matriz_dist = matriz_dist.set_index(matriz_dist.columns[0])
        matriz_dist = matriz_dist.map(lambda x: float(str(x).replace(',', '.')))

        for idx, (_, ordem) in enumerate(ordens.iterrows()):
            self.tempo_atual = ordem['data_hora']

            if idx < self.num_empilhadeiras:
                self.atribuir_ordem(idx, ordem, matriz_dist, forcar_saida_igual=True)
            else:
                self.processar_ordem(ordem, matriz_dist)

            self.tentar_processar_fila(matriz_dist)
    
            print(f"Processando: {idx + 1}/{total_de_ordens} ordens ({(idx + 1)/total_de_ordens:.1%})", end="\r")
            
        print() 

        while self.fila_espera_prioritaria:
            self.tempo_atual = self.fila_espera_prioritaria[0]['data_hora']
            self.tentar_processar_fila(matriz_dist)

        return self.gerar_resultados(matriz_dist)

    def processar_ordem(self, ordem, matriz_dist):
        esteiras_ocupadas = self.esteiras_ativas()
        nova_esteira = ordem['origem']

        if nova_esteira not in esteiras_ocupadas and len(esteiras_ocupadas) >= 2:
            self.adicionar_fila_espera(ordem)
            return

        melhor_emp = None
        melhor_custo = float('inf')

        for emp_id, emp in self.empilhadeiras.items():
            try:
                pos_atual = emp['posicao'] or ordem['origem']
                dist_sem_carga = matriz_dist.loc[pos_atual, ordem['origem']]
                dist_com_carga = matriz_dist.loc[ordem['origem'], ordem['destino']]
                dist_total = dist_sem_carga + dist_com_carga
                
                tempo_espera = max(0, (emp['livre_em'] - ordem['data_hora']).total_seconds()) if emp['livre_em'] else 0
                custo = dist_total + (tempo_espera * 0.1)

                if custo < melhor_custo:
                    melhor_custo = custo
                    melhor_emp = emp_id
            except Exception as e:
                print(f"Erro na ordem {ordem.get('ordem', '?')}: {str(e)}")
                continue

        if melhor_emp is not None:
            self.atribuir_ordem(melhor_emp, ordem, matriz_dist)
        else:
            self.adicionar_fila_espera(ordem)

    def tentar_processar_fila(self, matriz_dist):
        fila_atualizada = []
        for ordem in sorted(self.fila_espera_prioritaria, key=lambda x: x['data_hora']):
            esteiras_ocupadas = self.esteiras_ativas()
            if ordem['origem'] not in esteiras_ocupadas and len(esteiras_ocupadas) >= 2:
                fila_atualizada.append(ordem)
            else:
                self.processar_ordem(pd.Series(ordem), matriz_dist)

        self.fila_espera_prioritaria = fila_atualizada

    def adicionar_fila_espera(self, ordem):
        self.fila_espera_prioritaria.append(ordem.to_dict())

    def atribuir_ordem(self, emp_id, ordem, matriz_dist, forcar_saida_igual=False):
        emp = self.empilhadeiras[emp_id]

        pos_atual = emp['posicao'] if emp['posicao'] else ordem['origem']
        dist_sem_carga = matriz_dist.loc[pos_atual, ordem['origem']]
        tempo_sem_carga = dist_sem_carga / 10

        dist_com_carga = matriz_dist.loc[ordem['origem'], ordem['destino']]
        tempo_com_carga = dist_com_carga / 10

        hora_saida = ordem['data_hora'] if forcar_saida_igual or emp['livre_em'] is None else max(emp['livre_em'], ordem['data_hora']) + timedelta(seconds=tempo_sem_carga)
        
        # tempo ocioso parado antes de começar a mover
        if emp['livre_em'] and emp['livre_em'] < hora_saida:
            tempo_ocioso_parado = hora_saida - emp['livre_em']
            emp['tempo_ocioso_parado'] += tempo_ocioso_parado
        
        # tempo ocioso em movimento, deslocamento sem carga
        tempo_ocioso_movimento = timedelta(seconds=tempo_sem_carga)

        hora_coleta = hora_saida + timedelta(seconds=tempo_sem_carga)
        hora_entrega = hora_coleta + timedelta(seconds=tempo_com_carga)

        self.empilhadeiras[emp_id] = {
            'posicao': ordem['destino'],
            'livre_em': hora_entrega,
            'distancia_total': emp['distancia_total'] + dist_sem_carga + dist_com_carga,
            'distancia_sem_carga': emp['distancia_sem_carga'] + dist_sem_carga,
            'tempo_ocioso_parado': emp['tempo_ocioso_parado'],
            'tempo_ocioso_movimento': emp['tempo_ocioso_movimento'] + tempo_ocioso_movimento,
            'ordens_atendidas': emp['ordens_atendidas'] + [{
                **ordem.to_dict(),
                'hora_saida': hora_saida,
                'hora_coleta': hora_coleta,
                'hora_entrega': hora_entrega,
                'distancia_sem_carga': dist_sem_carga,
                'distancia_com_carga': dist_com_carga,
                'distancia_total': dist_sem_carga + dist_com_carga,
                'tempo_sem_carga': tempo_sem_carga,
                'tempo_com_carga': tempo_com_carga
            }]
        }

    def gerar_resultados(self, _):
        resultados = []
        tempos_ociosos_parado = []
        tempos_ociosos_movimento = []
        
        for emp_id, emp in self.empilhadeiras.items():
            tempos_ociosos_parado.append(emp['tempo_ocioso_parado'].total_seconds())
            tempos_ociosos_movimento.append(emp['tempo_ocioso_movimento'].total_seconds())
            
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
            'total_ordens': len(resultados),
            'fila_esteira_restante': len(self.fila_espera_prioritaria),
            'fila_estoque_restante': len(self.fila_estoque),
            'nao_atendidas': len(self.fila_espera_prioritaria) + len(self.fila_estoque),
            'distancia_total': sum(e['distancia_total'] for e in self.empilhadeiras.values()),
            'distancia_sem_carga': sum(e['distancia_sem_carga'] for e in self.empilhadeiras.values()),
            'distancia_com_carga': sum(e['distancia_total'] for e in self.empilhadeiras.values()) - sum(e['distancia_sem_carga'] for e in self.empilhadeiras.values()),
            'tempo_ocioso_parado_total': sum(tempos_ociosos_parado),
            'tempo_ocioso_movimento_total': sum(tempos_ociosos_movimento),
            'tempo_ocioso_total': sum(tempos_ociosos_parado) + sum(tempos_ociosos_movimento),
            'tempo_ocioso_parado_medio': np.mean(tempos_ociosos_parado) if tempos_ociosos_parado else 0.0,
            'tempo_ocioso_movimento_medio': np.mean(tempos_ociosos_movimento) if tempos_ociosos_movimento else 0.0
        }

        return pd.DataFrame(resultados), metricas

if __name__ == "__main__":
    ordens = pd.read_excel("ordens_unificadas.xlsx")
    matriz_dist = pd.read_excel("matriz_distancias.xlsx")

    NUM_EMPILHADEIRAS = 12

    print("\nIniciando otimização...")
    start_time = time.time()
    otimizador = Otimizador(NUM_EMPILHADEIRAS)
    rotas, metricas = otimizador.otimizar(ordens, matriz_dist)
    
    end_time = time.time()
    duracao_segundos = end_time - start_time

    print("\n=== RESUMO OTIMIZADO ===")
    print(f"Número de empilhadeiras: {NUM_EMPILHADEIRAS}")
    print(f"Ordens processadas: {metricas['total_ordens']}")
    print(f"Ordens não atendidas: {metricas['nao_atendidas']}")
    print(f"Distância total: {metricas['distancia_total']:.2f}m")
    print(f"Distância sem carga: {metricas['distancia_sem_carga']:.2f}m")
    print(f"Distância com carga: {metricas['distancia_com_carga']:.2f}m")
    print(f"Tempo ocioso total: {timedelta(seconds=metricas['tempo_ocioso_total'])}")
    print(f"  - Parado: {timedelta(seconds=metricas['tempo_ocioso_parado_total'])}")
    print(f"  - Em movimento sem carga (em segundos): {metricas['tempo_ocioso_movimento_total']:.2f}")
    print(f"Tempo total de execução: {timedelta(seconds=duracao_segundos)}")

    rotas.to_excel("resultados_otimizacao_detalhado.xlsx", index=False)

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from itertools import permutations
import time

class Otimizador:
    def __init__(self, num_empilhadeiras, janela_consolidacao_min=15):
        self.num_empilhadeiras = num_empilhadeiras
        self.janela_consolidacao = timedelta(minutes=janela_consolidacao_min)
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
                'ordens_atendidas': [],
            } for i in range(self.num_empilhadeiras)
        }
        self.fila_espera_prioritaria = []
        self.tempo_atual = None
        self.ordens_pendentes = []

    def esteiras_ativas(self):
        esteiras_ocupadas = set()
        for emp in self.empilhadeiras.values():
            if emp['livre_em'] and emp['livre_em'] > self.tempo_atual:
                for ordem in emp['ordens_atendidas']:
                    if isinstance(ordem, dict) and ordem.get('hora_entrega_final', self.tempo_atual) > self.tempo_atual and 'Esteira' in ordem['origem']:
                        esteiras_ocupadas.add(ordem['origem'])
        return esteiras_ocupadas

    def otimizar(self, ordens, matriz_dist):
        self.resetar()

        ordens['data_hora'] = pd.to_datetime(ordens['data_hora'], errors='coerce')
        ordens = ordens.dropna(subset=['data_hora']).sort_values('data_hora').reset_index(drop=True)
        self.ordens_pendentes = [ordem for _, ordem in ordens.iterrows()]

        matriz_dist = matriz_dist.set_index(matriz_dist.columns[0])
        matriz_dist = matriz_dist.map(lambda x: float(str(x).replace(',', '.')))
        
        total_de_ordens = len(self.ordens_pendentes)
        ordens_processadas_contador = 0
        
        print() 

        while self.ordens_pendentes:
            ordem_atual = self.ordens_pendentes.pop(0)
            self.tempo_atual = ordem_atual['data_hora']
            
            ordens_processadas_contador += 1
            print(f"Processando: {ordens_processadas_contador}/{total_de_ordens} ordens ({ordens_processadas_contador/total_de_ordens:.1%})", end="\r")

            self.processar_ordem(ordem_atual, matriz_dist)
            self.tentar_processar_fila(matriz_dist)

        print("\n\nProcessando ordens restantes da fila de espera...")
        
        while self.fila_espera_prioritaria:
            self.fila_espera_prioritaria.sort(key=lambda x: x['data_hora'])
            ordem_dict_para_processar = self.fila_espera_prioritaria.pop(0)
            ordem = pd.Series(ordem_dict_para_processar)
            
            print(f"Forçando atribuição da ordem em espera: {ordem['ordem']}", end='\r')

            id_emp_disponivel_mais_cedo = min(self.empilhadeiras, key=lambda i: self.empilhadeiras[i]['livre_em'] or self.tempo_atual)
            emp_disponivel_mais_cedo = self.empilhadeiras[id_emp_disponivel_mais_cedo]

            self.tempo_atual = max(self.tempo_atual, emp_disponivel_mais_cedo['livre_em'] or self.tempo_atual, ordem['data_hora'])
            
            self.atribuir_ordem(id_emp_disponivel_mais_cedo, [ordem], matriz_dist)

        print("\nOtimização concluída.")
        return self.gerar_resultados()

    def processar_ordem(self, ordem, matriz_dist):
        esteiras_ocupadas = self.esteiras_ativas()
        if 'Esteira' in str(ordem['origem']) and (ordem['origem'] in esteiras_ocupadas or len(esteiras_ocupadas) >= 2):
            self.adicionar_fila_espera(ordem)
            return

        melhor_consolidacao = self.buscar_melhor_consolidacao(ordem, matriz_dist)
        melhor_emp_simples, custo_simples = self.encontrar_melhor_empilhadeira_ordem(ordem, matriz_dist)

        if melhor_consolidacao and melhor_consolidacao['custo_total'] < custo_simples:
            self.ordens_pendentes = [o for o in self.ordens_pendentes if o['ordem'] != melhor_consolidacao['ordem_adicional']['ordem']]
            self.atribuir_ordem(melhor_consolidacao['emp_id'], melhor_consolidacao['pacote_ordens'], matriz_dist)
        elif melhor_emp_simples is not None:
            self.atribuir_ordem(melhor_emp_simples, [ordem], matriz_dist)
        else:
            self.adicionar_fila_espera(ordem)
            
    def buscar_melhor_consolidacao(self, ordem_principal, matriz_dist):
        melhor_opcao = None
        melhor_custo_consolidado = float('inf')
        
        limite_tempo = ordem_principal['data_hora'] + self.janela_consolidacao
        candidatas = [o for o in self.ordens_pendentes if o['ordem'] != ordem_principal['ordem'] and o['data_hora'] <= limite_tempo]
        
        if 'base' not in ordem_principal or 'quantidade' not in ordem_principal: return None
        capacidade_max = 3 * ordem_principal['base']

        for ordem_adicional in candidatas:
            if ordem_adicional.get('base') != ordem_principal['base']: continue
            if (ordem_principal['quantidade'] + ordem_adicional.get('quantidade', 0)) > capacidade_max: continue

            pacote_ordens = [ordem_principal, ordem_adicional]
            
            for emp_id, emp in self.empilhadeiras.items():
                pos_atual = emp['posicao'] or pacote_ordens[0]['origem']
                
                dist_consolidada = (matriz_dist.loc[pos_atual, pacote_ordens[0]['origem']] +
                                    matriz_dist.loc[pacote_ordens[0]['origem'], pacote_ordens[1]['origem']] +
                                    matriz_dist.loc[pacote_ordens[1]['origem'], pacote_ordens[0]['destino']] +
                                    matriz_dist.loc[pacote_ordens[0]['destino'], pacote_ordens[1]['destino']])

                hora_disponivel = emp['livre_em'] or self.tempo_atual
                tempo_espera = max(0, (hora_disponivel - self.tempo_atual).total_seconds())
                custo_atual = dist_consolidada + (tempo_espera * 0.1)

                if custo_atual < melhor_custo_consolidado:
                    melhor_custo_consolidado = custo_atual
                    melhor_opcao = {'emp_id': emp_id, 'pacote_ordens': pacote_ordens, 'ordem_adicional': ordem_adicional, 'custo_total': custo_atual}
        return melhor_opcao

    def encontrar_melhor_empilhadeira_ordem(self, ordem, matriz_dist):
        melhor_emp, melhor_custo = None, float('inf')
        for emp_id, emp in self.empilhadeiras.items():
            pos_atual = emp['posicao'] or ordem['origem']
            dist_sem_carga = matriz_dist.loc[pos_atual, ordem['origem']]
            dist_com_carga = matriz_dist.loc[ordem['origem'], ordem['destino']]
            dist_total = dist_sem_carga + dist_com_carga
            
            hora_disponivel = emp['livre_em'] or self.tempo_atual
            tempo_espera = max(0, (hora_disponivel - ordem['data_hora']).total_seconds())
            custo = dist_total + (tempo_espera * 0.1)

            if custo < melhor_custo:
                melhor_custo, melhor_emp = custo, emp_id
        return melhor_emp, melhor_custo

    def tentar_processar_fila(self, matriz_dist):
        fila_processada = []
        # evita modificar a lista enquanto itera sobre ela
        ordens_na_fila = list(self.fila_espera_prioritaria)
        self.fila_espera_prioritaria = [] 
        for ordem_dict in ordens_na_fila:
            ordem = pd.Series(ordem_dict)
            esteiras_ocupadas = self.esteiras_ativas()
            if 'Esteira' in str(ordem['origem']) and (ordem['origem'] in esteiras_ocupadas or len(esteiras_ocupadas) >= 2):
                # se não pode processar adiciona de volta a fila principal
                self.adicionar_fila_espera(ordem)
            else:
                # tenta processar novamente
                self.processar_ordem(ordem, matriz_dist)

    def adicionar_fila_espera(self, ordem):
        self.fila_espera_prioritaria.append(ordem.to_dict())

    def atribuir_ordem(self, emp_id, pacote_ordens, matriz_dist):
        emp = self.empilhadeiras[emp_id]
        pos_inicial_emp = emp['posicao'] or pacote_ordens[0]['origem']
        
        hora_criacao_mais_tarde = max(ordem['data_hora'] for ordem in pacote_ordens)
        hora_disponivel_empilhadeira = emp['livre_em'] or self.tempo_atual
        
        hora_saida_base = max(hora_disponivel_empilhadeira, hora_criacao_mais_tarde)

        dist_sem_carga_viagem = matriz_dist.loc[pos_inicial_emp, pacote_ordens[0]['origem']]
        tempo_sem_carga_viagem = timedelta(seconds=dist_sem_carga_viagem / 10)

        dist_com_carga_viagem = 0
        pos_atual = pacote_ordens[0]['origem']
        
        for i in range(len(pacote_ordens) - 1):
            proxima_origem = pacote_ordens[i+1]['origem']
            dist_com_carga_viagem += matriz_dist.loc[pos_atual, proxima_origem]
            pos_atual = proxima_origem
        
        for ordem in pacote_ordens:
            dist_com_carga_viagem += matriz_dist.loc[pos_atual, ordem['destino']]
            pos_atual = ordem['destino']

        dist_total_viagem = dist_sem_carga_viagem + dist_com_carga_viagem
        tempo_com_carga_viagem = timedelta(seconds=dist_com_carga_viagem / 10)
        
        tempo_movimento_total_viagem = tempo_sem_carga_viagem + tempo_com_carga_viagem
        hora_entrega_final = hora_saida_base + tempo_movimento_total_viagem
        
        if emp['livre_em'] and emp['livre_em'] < hora_saida_base:
            emp['tempo_ocioso_parado'] += (hora_saida_base - emp['livre_em'])
        
        emp['tempo_ocioso_movimento'] += tempo_sem_carga_viagem
        emp['distancia_total'] += dist_total_viagem
        emp['distancia_sem_carga'] += dist_sem_carga_viagem
        emp['posicao'] = pos_atual
        emp['livre_em'] = hora_entrega_final

        for ordem in pacote_ordens:
             emp['ordens_atendidas'].append({
                **ordem.to_dict(),
                'hora_saida_empilhadeira': hora_saida_base,
                'hora_entrega_final': hora_entrega_final,
                'consolidado_com': [o['ordem'] for o in pacote_ordens if o['ordem'] != ordem['ordem']],
                'distancia_total_viagem': dist_total_viagem,
                'distancia_sem_carga_viagem': dist_sem_carga_viagem,
                'distancia_com_carga_viagem': dist_com_carga_viagem,
                'tempo_sem_carga_viagem_s': tempo_sem_carga_viagem.total_seconds(),
                'tempo_com_carga_viagem_s': tempo_com_carga_viagem.total_seconds(),
                'tempo_movimento_total_viagem_s': tempo_movimento_total_viagem.total_seconds(),
            })

    def gerar_resultados(self):
        resultados = []
        for emp_id, emp in self.empilhadeiras.items():
            for ordem in emp['ordens_atendidas']:
                if isinstance(ordem, dict):
                    resultados.append({
                        'ordem': ordem.get('ordem'),
                        'material': ordem.get('material'),
                        'origem': ordem.get('origem'),
                        'destino': ordem.get('destino'),
                        'empilhadeira': emp_id,
                        'hora_criacao': ordem.get('data_hora'),
                        'hora_saida_empilhadeira': ordem.get('hora_saida_empilhadeira'),
                        'hora_entrega': ordem.get('hora_entrega_final'),
                        'distancia_total': ordem.get('distancia_total_viagem'),
                        'distancia_sem_carga': ordem.get('distancia_sem_carga_viagem'),
                        'distancia_com_carga': ordem.get('distancia_com_carga_viagem'),
                        'tempo_movimento_total': ordem.get('tempo_movimento_total_viagem_s'),
                        'tempo_sem_carga': ordem.get('tempo_sem_carga_viagem_s'),
                        'tempo_com_carga': ordem.get('tempo_com_carga_viagem_s'),
                        'consolidado_com': ordem.get('consolidado_com', [])
                    })

        df_resultados = pd.DataFrame(resultados).sort_values(by='hora_criacao').reset_index(drop=True)
        
        dist_total = df_resultados['distancia_total'].sum()
        dist_sem_carga = df_resultados['distancia_sem_carga'].sum()
        dist_com_carga = df_resultados['distancia_com_carga'].sum()
        
        tempo_sem_carga_total = df_resultados['tempo_sem_carga'].sum()
        tempo_com_carga_total = df_resultados['tempo_com_carga'].sum()
        tempo_movimento_total = df_resultados['tempo_movimento_total'].sum()
        
        tempo_inicio = df_resultados['hora_criacao'].min()
        tempo_fim = df_resultados['hora_entrega'].max()
        tempo_total_simulacao = (tempo_fim - tempo_inicio).total_seconds()
        
        tempo_ocioso_total = (self.num_empilhadeiras * tempo_total_simulacao) - tempo_movimento_total
        tempo_ocioso_movimento = tempo_sem_carga_total
        tempo_ocioso_parado = tempo_ocioso_total - tempo_ocioso_movimento

        metricas = {
            'total_ordens_processadas': len(df_resultados),
            'ordens_nao_atendidas_final': len(self.fila_espera_prioritaria),
            'distancia_total': dist_total,
            'distancia_sem_carga': dist_sem_carga,
            'distancia_com_carga': dist_com_carga,
            'tempo_ocioso_total': tempo_ocioso_total,
            'tempo_ocioso_parado': tempo_ocioso_parado,
            'tempo_ocioso_movimento': tempo_ocioso_movimento,
            'tempo_com_carga_total': tempo_com_carga_total,
        }
        return df_resultados, metricas


if __name__ == "__main__":
    ordens = pd.read_excel("ordens_unificadas_1000.xlsx")
    matriz_dist = pd.read_excel("matriz_distancias.xlsx")

    NUM_EMPILHADEIRAS = 12
    JANELA_CONSOLIDACAO_MIN = 15

    print("\nIniciando otimização...")
    start_time = time.time()
    otimizador = Otimizador(NUM_EMPILHADEIRAS, JANELA_CONSOLIDACAO_MIN)
    rotas, metricas = otimizador.otimizar(ordens, matriz_dist)
    
    end_time = time.time()
    duracao_segundos = end_time - start_time

    print(f"\n=== RESUMO FINAL ===")
    print(f"Número de empilhadeiras: {NUM_EMPILHADEIRAS}")
    print(f"Ordens processadas: {metricas['total_ordens_processadas']}")
    print(f"Ordens não atendidas: {metricas['ordens_nao_atendidas_final']}")
    print(f"Distância total: {metricas['distancia_total']:.2f}m")
    percentual_sem_carga = (metricas['distancia_sem_carga'] / metricas['distancia_total']) if metricas['distancia_total'] > 0 else 0
    print(f"Distância sem carga: {metricas['distancia_sem_carga']:.2f}m ({percentual_sem_carga:.2%})")
    print(f"Distância com carga: {metricas['distancia_com_carga']:.2f}m")
    print(f"Tempo ocioso total: {timedelta(seconds=metricas['tempo_ocioso_total'])} ({metricas['tempo_ocioso_total']:.2f}s)")
    print(f"  - Parado: {timedelta(seconds=metricas['tempo_ocioso_parado'])} ({metricas['tempo_ocioso_parado']:.2f}s)")
    print(f"  - Em movimento sem carga: {timedelta(seconds=metricas['tempo_ocioso_movimento'])} ({metricas['tempo_ocioso_movimento']:.2f}s)")
    print(f"Tempo total de execução: {timedelta(seconds=duracao_segundos)}")

    rotas.to_excel("resultados_otimizacao_consolidacao15min.xlsx", index=False)
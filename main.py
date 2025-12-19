import json
import math

import pandas as pd
import yfinance as yf

# --- CONFIGURAÇÕES ---
CONFIG = {
    'DY_MINIMO': 0.06,
    'ALOCACAO_RENDA': 0.80,      # Meta: 80%
    'ALOCACAO_CRESCIMENTO': 0.20, # Meta: 20%
    'SETORES_BEST': ['Bank', 'Electric', 'Water', 'Insurance', 'Telecom', 'Financial', 'Utility', 'Real Estate', 'Industrials']
}

class AnaliseFundamentalista:
    def __init__(self, carteira_dict):
        # Normaliza chaves para garantir .SA e lista de tickers
        self.carteira_qtd = {k.upper().replace('.SA', '') + '.SA': v for k, v in carteira_dict.items()}
        self.tickers = list(self.carteira_qtd.keys())
        self.dados = {}

    def buscar_dados(self):
        print("🔄 Atualizando cotações e indicadores da sua carteira...")
        for t in self.tickers:
            try:
                ticker_obj = yf.Ticker(t)
                info = ticker_obj.info
                hist = ticker_obj.history(period="1y")
                
                if hist.empty: continue

                # 1. Preço e Valor Atual
                preco_atual = info.get('currentPrice') or info.get('regularMarketPreviousClose') or hist['Close'].iloc[-1]
                qtd_atual = self.carteira_qtd[t]
                valor_posicao = preco_atual * qtd_atual
                
                # 2. Tratamento DY
                raw_dy = info.get('dividendYield', 0)
                if raw_dy is None: raw_dy = 0
                dy = raw_dy / 100 if raw_dy > 1.5 else raw_dy

                # 3. Momentum
                momentum = 0
                if len(hist) > 126:
                    momentum = (preco_atual / hist['Close'].iloc[-126]) - 1

                # 4. Classificação
                tipo = 'FII' if '11' in t and ('EQUITY' not in info.get('quoteType', '') and 'ETF' not in info.get('quoteType', '')) else 'ACAO'
                setor = info.get('sector', 'Outros').title()

                self.dados[t] = {
                    'symbol': t.replace('.SA', ''),
                    'price': preco_atual,
                    'qtd_atual': qtd_atual,
                    'valor_posicao': valor_posicao,
                    'dy': dy,
                    'momentum': momentum,
                    'type': tipo,
                    'sector': setor
                }
            except Exception as e:
                print(f"❌ Erro em {t}: {e}")

    def aplicar_regras(self):
        analise = []
        for t, d in self.dados.items():
            score = 0
            motivos = []
            
            # Identificação de Perfil
            e_best = any(k in d['sector'] for k in CONFIG['SETORES_BEST']) or d['type'] == 'FII'
            
            # Score
            if d['dy'] >= CONFIG['DY_MINIMO']: 
                score += 2
                motivos.append("Bom Pagador")
            if e_best: 
                score += 1
                motivos.append("Setor Seguro")
            if d['momentum'] > 0.05: 
                score += 1.5
                motivos.append("Tendência Alta")
            if d['type'] == 'FII' and d['price'] < 2.0:
                score -= 5 # Penalidade Penny Stock

            # Definição de Papel na Carteira
            # Se já tem na carteira, não descartamos, apenas diagnosticamos
            perfil_ativo = "NEUTRO"
            if score >= 3: perfil_ativo = "TOP PICK"
            elif score >= 2 and d['dy'] >= CONFIG['DY_MINIMO']: perfil_ativo = "RENDA"
            elif d['momentum'] > 0.10: perfil_ativo = "CRESCIMENTO"
            else: perfil_ativo = "VENDER/REVISAR" # Score baixo

            analise.append({**d, 'perfil': perfil_ativo, 'score': score, 'motivos': ", ".join(motivos)})
        
        return pd.DataFrame(analise).sort_values(by='score', ascending=False)

class RebalanceadorCarteira:
    def __init__(self, dinheiro_novo):
        self.caixa = dinheiro_novo

    def diagnosticar_e_sugerir(self, df):
        if df.empty: return

        # 1. Calcular Patrimônio Total (Ações + Caixa Novo)
        valor_investido = df['valor_posicao'].sum()
        patrimonio_total = valor_investido + self.caixa

        # 2. Separar Valor Atual por Categoria
        # Vamos assumir que FIIs e Ações High Yield são "Renda", resto é "Crescimento"
        df['bucket'] = df.apply(lambda x: 'RENDA' if (x['type']=='FII' or x['dy'] >= CONFIG['DY_MINIMO']) else 'CRESCIMENTO', axis=1)
        
        atual_renda = df[df['bucket'] == 'RENDA']['valor_posicao'].sum()
        atual_cresc = df[df['bucket'] == 'CRESCIMENTO']['valor_posicao'].sum()

        # 3. Metas
        meta_renda = patrimonio_total * CONFIG['ALOCACAO_RENDA']
        meta_cresc = patrimonio_total * CONFIG['ALOCACAO_CRESCIMENTO']

        print("\n💼 --- DIAGNÓSTICO DE CARTEIRA ---")
        print(f"Patrimônio Total (com aporte): R$ {patrimonio_total:.2f}")
        print("--------------------------------------------------")
        print("CATEGORIA    | ATUAL (%)       | META (%) | STATUS")
        
        p_renda = (atual_renda / patrimonio_total) if patrimonio_total > 0 else 0
        p_cresc = (atual_cresc / patrimonio_total) if patrimonio_total > 0 else 0
        
        status_renda = "✅ OK" if abs(p_renda - CONFIG['ALOCACAO_RENDA']) < 0.05 else ("🔻 Abaixo" if p_renda < CONFIG['ALOCACAO_RENDA'] else "🔺 Acima")
        status_cresc = "✅ OK" if abs(p_cresc - CONFIG['ALOCACAO_CRESCIMENTO']) < 0.05 else ("🔻 Abaixo" if p_cresc < CONFIG['ALOCACAO_CRESCIMENTO'] else "🔺 Acima")

        print(f"Renda        | R$ {atual_renda:.2f} ({p_renda:.1%}) | {CONFIG['ALOCACAO_RENDA']:.0%}      | {status_renda}")
        print(f"Crescimento  | R$ {atual_cresc:.2f} ({p_cresc:.1%}) | {CONFIG['ALOCACAO_CRESCIMENTO']:.0%}      | {status_cresc}")
        print("--------------------------------------------------")

        # 4. Lógica de Aporte (Onde colocar o dinheiro novo?)
        # Calculamos o "GAP" (quanto falta para chegar na meta)
        gap_renda = meta_renda - atual_renda
        gap_cresc = meta_cresc - atual_cresc

        print(f"\n🛒 --- SUGESTÃO DE APORTE (R$ {self.caixa:.2f}) ---")
        
        saldo = self.caixa
        
        # Filtra apenas ativos bons (Score > 2) para reforçar posição
        ativos_qualificados = df[df['score'] >= 2].copy()
        
        # Prioridade: Encher o balde que está mais vazio (maior GAP)
        if gap_renda > gap_cresc:
            print(f"👉 Prioridade: Reforçar RENDA (Deficit: R$ {gap_renda:.2f})")
            ordem_compra = ativos_qualificados[ativos_qualificados['bucket'] == 'RENDA']
        else:
            print(f"👉 Prioridade: Reforçar CRESCIMENTO (Deficit: R$ {gap_cresc:.2f})")
            ordem_compra = ativos_qualificados[ativos_qualificados['bucket'] == 'CRESCIMENTO']

        # Se não tiver ativos qualificados no bucket prioritário, olha tudo
        if ordem_compra.empty:
            print("⚠️ Sem ativos de alta qualidade no setor prioritário. Olhando melhores oportunidades gerais...")
            ordem_compra = ativos_qualificados

        # Ordena pelos melhores scores
        ordem_compra = ordem_compra.sort_values(by=['score', 'dy'], ascending=False)

        # Executa compra simulada
        for _, ativo in ordem_compra.iterrows():
            if saldo < ativo['price']: continue
            
            # Tenta preencher o GAP, mas limitado ao saldo
            qtd = math.floor(saldo / ativo['price'])
            if qtd > 0:
                custo = qtd * ativo['price']
                saldo -= custo
                print(f"   ✅ COMPRAR {qtd}x {ativo['symbol']} (R$ {ativo['price']:.2f}) -> {ativo['perfil']}")
        
        if saldo > 0:
            print(f"   💵 Sobra em caixa: R$ {saldo:.2f}")
            print("   (Dica: Acumule para comprar um ativo mais caro na próxima ou adicione um novo ativo à lista JSON)")
        
        # Alerta de Ativos Ruins
        lixo = df[df['perfil'] == 'VENDER/REVISAR']
        if not lixo.empty:
            print("\n⚠️ --- ATENÇÃO (Revisar Fundamentos) ---")
            for _, row in lixo.iterrows():
                print(f"   ❌ {row['symbol']}: Score Baixo ({row['score']}). Considere vender ou estudar melhor.")

# --- EXECUÇÃO ---

# 1. Carregar Carteira
try:
    with open('carteira.json', 'r') as f:
        carteira_usuario = json.load(f)
except FileNotFoundError:
    print("Crie o arquivo 'carteira.json' antes de rodar!")
    exit()

# 2. Quanto dinheiro novo você vai colocar hoje?
dinheiro_novo = float(input("Valor do aporte: "))

# 3. Rodar
analista = AnaliseFundamentalista(carteira_usuario)
analista.buscar_dados()
df_carteira = analista.aplicar_regras()

rebalanceador = RebalanceadorCarteira(dinheiro_novo)
rebalanceador.diagnosticar_e_sugerir(df_carteira)
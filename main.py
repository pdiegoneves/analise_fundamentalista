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
            justificativas = []
            premissas = []
            
            # Identificação de Perfil
            e_best = any(k in d['sector'] for k in CONFIG['SETORES_BEST']) or d['type'] == 'FII'
            
            # Score e Justificativas
            if d['dy'] >= CONFIG['DY_MINIMO']: 
                score += 2
                justificativas.append(f"Gerador de Renda (DY {d['dy']:.1%}): Ativo cumpre função de fluxo de caixa.")
            
            if e_best: 
                score += 1
                premissas.append(f"Setor Resiliente ({d['sector']}): Historicamente menos volátil em crises.")
            
            if d['momentum'] > 0.05: 
                score += 1.5
                justificativas.append(f"Momentum Positivo (+{d['momentum']:.1%}): Mercado demonstra interesse recente.")
            
            if d['type'] == 'FII' and d['price'] < 2.0:
                score -= 5 # Penalidade Penny Stock
                premissas.append("Risco de Liquidez/Grupamento: Valor nominal muito baixo (Penny Stock).")

            # Definição de Papel na Carteira
            perfil_ativo = "NEUTRO"
            if score >= 3: perfil_ativo = "TOP PICK"
            elif score >= 2 and d['dy'] >= CONFIG['DY_MINIMO']: perfil_ativo = "RENDA"
            elif d['momentum'] > 0.10: perfil_ativo = "CRESCIMENTO"
            else: perfil_ativo = "VENDER/REVISAR" # Score baixo

            analise.append({
                **d, 
                'perfil': perfil_ativo, 
                'score': score, 
                'justificativa_tecnica': " ".join(justificativas),
                'premissas_negocio': " ".join(premissas)
            })
        
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
        df['bucket'] = df.apply(lambda x: 'RENDA' if (x['type']=='FII' or x['dy'] >= CONFIG['DY_MINIMO']) else 'CRESCIMENTO', axis=1)
        
        atual_renda = df[df['bucket'] == 'RENDA']['valor_posicao'].sum()
        atual_cresc = df[df['bucket'] == 'CRESCIMENTO']['valor_posicao'].sum()

        # 3. Metas e Diagnóstico
        meta_renda = patrimonio_total * CONFIG['ALOCACAO_RENDA']
        meta_cresc = patrimonio_total * CONFIG['ALOCACAO_CRESCIMENTO']

        print("\n" + "="*60)
        print("💼 RELATÓRIO DE GESTÃO DE CARTEIRA E RISCO")
        print("="*60)
        
        print(f"\n📊 DIAGNÓSTICO DE ALOCAÇÃO (Patrimônio: R$ {patrimonio_total:.2f})")
        print(f"O rebalanceamento visa alinhar a exposição ao risco conforme a estratégia definida.")
        print("-" * 60)
        print("CATEGORIA    | ATUAL (%)       | META (%) | DESVIO   | STATUS")
        
        p_renda = (atual_renda / patrimonio_total) if patrimonio_total > 0 else 0
        p_cresc = (atual_cresc / patrimonio_total) if patrimonio_total > 0 else 0
        
        # Cálculo de Desvio
        desvio_renda = p_renda - CONFIG['ALOCACAO_RENDA']
        desvio_cresc = p_cresc - CONFIG['ALOCACAO_CRESCIMENTO']
        
        status_renda = "✅ Na Meta" if abs(desvio_renda) < 0.05 else ("🔻 Sub-alocado" if desvio_renda < 0 else "🔺 Super-alocado")
        status_cresc = "✅ Na Meta" if abs(desvio_cresc) < 0.05 else ("🔻 Sub-alocado" if desvio_cresc < 0 else "🔺 Super-alocado")

        print(f"Renda        | R$ {atual_renda:,.2f} ({p_renda:.1%}) | {CONFIG['ALOCACAO_RENDA']:.0%}      | {desvio_renda:+.1%}   | {status_renda}")
        print(f"Crescimento  | R$ {atual_cresc:,.2f} ({p_cresc:.1%}) | {CONFIG['ALOCACAO_CRESCIMENTO']:.0%}      | {desvio_cresc:+.1%}   | {status_cresc}")
        
        if abs(desvio_renda) > 0.10:
            print(f"\n⚠️ ALERTA DE RISCO: Desvio relevante em Renda ({desvio_renda:+.1%}). Ajuste prioritário recomendado.")

        # 4. Lógica de Aporte
        gap_renda = meta_renda - atual_renda
        gap_cresc = meta_cresc - atual_cresc

        print(f"\n🛒 PLANEJAMENTO DE APORTE (Disponível: R$ {self.caixa:.2f})")
        
        saldo = self.caixa
        ativos_qualificados = df[df['score'] >= 2].copy()
        
        justificativa_aporte = ""
        if gap_renda > gap_cresc:
            justificativa_aporte = "Foco em aumentar a base de Renda Passiva para atingir a meta de 80%."
            ordem_compra = ativos_qualificados[ativos_qualificados['bucket'] == 'RENDA']
        else:
            justificativa_aporte = "Foco em Potencialização de Patrimônio (Crescimento) para equilibrar o portfólio."
            ordem_compra = ativos_qualificados[ativos_qualificados['bucket'] == 'CRESCIMENTO']

        if ordem_compra.empty:
            justificativa_aporte += " (Sem ativos 'Top Pick' no setor prioritário, buscando melhores oportunidades gerais)"
            ordem_compra = ativos_qualificados

        print(f"👉 Estratégia: {justificativa_aporte}")

        ordem_compra = ordem_compra.sort_values(by=['score', 'dy'], ascending=False)
        
        total_gasto = 0
        novos_dividendos_ano = 0

        print("\n📋 ORDENS SUGERIDAS:")
        for _, ativo in ordem_compra.iterrows():
            if saldo < ativo['price']: continue
            
            qtd = math.floor(saldo / ativo['price'])
            if qtd > 0:
                custo = qtd * ativo['price']
                saldo -= custo
                total_gasto += custo
                div_projetado = custo * ativo['dy']
                novos_dividendos_ano += div_projetado
                
                print(f"   ✅ COMPRAR {qtd}x {ativo['symbol']} a R$ {ativo['price']:.2f}")
                print(f"      ↳ Motivo: {ativo['justificativa_tecnica']}")
                print(f"      ↳ Impacto: +R$ {div_projetado:.2f}/ano em dividendos estimados.")

        print("\n📈 MÉTRICAS DE IMPACTO DO APORTE")
        print(f"• Total Alocado:       R$ {total_gasto:.2f}")
        print(f"• Incremento de Renda: +R$ {novos_dividendos_ano:.2f} / ano (estimado)")
        if saldo > 0:
            print(f"• Sobra de Caixa:      R$ {saldo:.2f}")
        
        # Alerta de Ativos Ruins
        lixo = df[df['perfil'] == 'VENDER/REVISAR']
        if not lixo.empty:
            print("\n🚨 PONTO DE ATENÇÃO (Revisão Necessária)")
            for _, row in lixo.iterrows():
                print(f"   ❌ {row['symbol']}: Score {row['score']}. {row['justificativa_tecnica']}")

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
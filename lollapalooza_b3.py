import math

import fundamentus
import numpy as np
import pandas as pd

# --- CONFIGURAÇÕES ---
CONFIG = {
    "LIQUIDEZ_MINIMA": 1_000_000,
    "SETORES_EXCLUIDOS": ['AZUL4', 'GOLL4', 'CVCB3', 'IRBR3', 'OIBR3', 'AMER3']
}

print("🎸 INICIANDO ALGORITMO: LOLLAPALOOZA TUPINIQUIM (Com Justificativa) 🇧🇷")
print("==========================================================================")

def limpar_coluna(col_name):
    return col_name.lower().replace('.', '').replace(' ', '').replace('/', '').replace('_', '')

def obter_dados_base():
    print("📥 Stage 0: Baixando dados fundamentais...")
    try:
        df = fundamentus.get_resultado()
    except Exception as e:
        print(f"❌ Erro fatal no Fundamentus: {e}")
        return pd.DataFrame()

    # Mapeamento
    mapa = {
        'cotacao': 'cotacao', 'pl': 'pl', 'pvp': 'pvp',
        'dy': 'dy', 'divyield': 'dy',
        'liq2': 'liq2m', 'liq2m': 'liq2m',
        'patrimliq': 'patrim_liq',
        'divbrut': 'div_bruta_ratio',
        'mrgliq': 'mrgliq', 'roe': 'roe', 'roic': 'roic', 'cresc': 'c5y'
    }

    novos_nomes = {}
    for col in df.columns:
        col_limpa = limpar_coluna(col)
        for chave, valor in mapa.items():
            if chave in col_limpa:
                novos_nomes[col] = valor
                break
    
    df.rename(columns=novos_nomes, inplace=True)
    
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Ajuste de Escala
    for col in ['dy', 'roe', 'roic', 'mrgliq', 'c5y', 'div_bruta_ratio']:
        if col in df.columns and df[col].mean() > 5.0:
            df[col] = df[col] / 100

    # Recuperação de Dados
    if 'patrim_liq' not in df.columns:
        df['patrim_liq'] = 1.0 
        df['ignore_solvencia'] = True
    else:
        df['ignore_solvencia'] = False

    if 'div_bruta_ratio' in df.columns:
        df['div_bruta'] = df['div_bruta_ratio'] * df['patrim_liq']
    else:
        df['div_bruta'] = 0

    if 'liq2m' in df.columns:
        df = df[df['liq2m'] > CONFIG["LIQUIDEZ_MINIMA"]]

    df = df[~df.index.isin(CONFIG["SETORES_EXCLUIDOS"])]
    df = df[df['cotacao'] > 0]
    
    # Valuation
    df['lpa'] = df.apply(lambda row: row['cotacao']/row['pl'] if row['pl'] > 0 else 0, axis=1)
    df['vpa'] = df.apply(lambda row: row['cotacao']/row['pvp'] if row['pvp'] > 0 else 0, axis=1)

    return df

def stage_1_graham_permissivo(df):
    print("🛡️ Stage 1: Filtro de Segurança...")
    candidatos = []
    
    for ticker, row in df.iterrows():
        if not row['ignore_solvencia']:
            patrim = row['patrim_liq']
            if patrim <= 0 and patrim != 1.0: continue
            divida = row.get('div_bruta', 0)
            if patrim > 1 and (divida/patrim) > 3.5: continue

        vi_graham = np.sqrt(22.5 * row['lpa'] * row['vpa']) if (row['lpa']>0 and row['vpa']>0) else 0
        margem = row['cotacao'] / vi_graham if vi_graham > 0 else 999
        dy = row.get('dy', 0)
        pl = row.get('pl', 99)

        # Regras de Entrada (Bazin ou Graham ou Qualidade)
        if (margem <= 1.0 and pl < 25) or (dy >= 0.06 and pl < 25) or (row.get('roe', 0) > 0.20 and pl < 15):
            candidatos.append(ticker)

    return df.loc[candidatos].copy()

def stage_3_ranking_final(df):
    resultados = []
    for ticker, row in df.iterrows():
        score = 0
        factors = [] # Lista para guardar as justificativas
        
        # --- SISTEMA DE PONTUAÇÃO E JUSTIFICATIVA ---
        
        # 1. Qualidade (Buffett)
        roe = row.get('roe', 0)
        if roe > 0.15: 
            score += 10
            factors.append("ROE>15%")
        if roe > 0.25: 
            score += 10
            factors.append("Rentabilidade Top (ROE>25%)")
            
        # 2. Crescimento
        cagr = row.get('c5y', 0)
        if cagr > 0.10: 
            score += 10
            factors.append(f"Crescimento ({cagr:.0%})")
            
        # 3. Preço/Oportunidade (Munger/Bazin)
        pl = row.get('pl', 0)
        if pl < 10 and pl > 0: 
            score += 10
            factors.append("P/L Baixo")
            
        dy = row.get('dy', 0)
        if dy > 0.06: 
            score += 10
            factors.append(f"Dividendos ({dy:.1%})")
        if dy > 0.10: 
            score += 5
            factors.append("Yield Explosivo")
        
        # 4. Graham (Segurança)
        vi = np.sqrt(22.5 * row['lpa'] * row['vpa']) if row['lpa']>0 else 0
        if vi > 0 and row['cotacao'] < (0.7 * vi): 
            score += 15
            factors.append("Desconto Graham (>30%)")

        resultados.append({
            'Ticker': ticker,
            'Preco': row['cotacao'],
            'Score': score,
            'Motivo': ", ".join(factors) # Junta tudo numa string
        })

    # Ordena: Maior Score primeiro, depois Menor Preço (para facilitar compras pequenas)
    return pd.DataFrame(resultados).sort_values(by=['Score', 'Preco'], ascending=[False, True])

def montar_carteira_real(df_ranking):
    print("\n" + "="*80)
    print("💰 CALCULADORA DE CARTEIRA INTELIGENTE")
    print("="*80)
    
    try:
        dinheiro = float(input(">>> Digite quanto você tem para investir (ex: 100): R$ "))
    except ValueError:
        print("Valor inválido.")
        return

    print(f"\n🛒 Calculando a melhor cesta para R$ {dinheiro:.2f}...\n")

    carteira = []
    total_gasto = 0
    saldo = dinheiro
    
    # Filtra apenas os aprovados (Score >= 40)
    top_picks = df_ranking[df_ranking['Score'] >= 40].copy()
    
    if top_picks.empty:
        print("⚠️ Nenhum ativo atingiu a pontuação mínima de robustez (40 pontos).")
        return

    # LÓGICA DE ALOCAÇÃO
    # Tenta comprar pelo menos 1 de cada dos melhores, do mais barato ao mais caro
    if dinheiro < 1000:
        # Modo Capital Pequeno: Compra gulosa (enche o carrinho com o que dá)
        for _, row in top_picks.iterrows():
            if saldo >= row['Preco']:
                qtd = 1
                custo = row['Preco']
                saldo -= custo
                total_gasto += custo
                carteira.append({
                    'Ticker': row['Ticker'], 
                    'Preco': row['Preco'], 
                    'Qtd': qtd, 
                    'Total': custo,
                    'Motivo Compra': row['Motivo'] # <--- AQUI ENTRA A JUSTIFICATIVA
                })
    else:
        # Modo Capital Maior: Tenta balancear
        alvo = min(15, len(top_picks))
        fat = dinheiro / alvo
        for _, row in top_picks.head(alvo).iterrows():
            qtd = math.floor(fat / row['Preco'])
            if qtd > 0:
                custo = qtd * row['Preco']
                saldo -= custo
                total_gasto += custo
                carteira.append({'Ticker': row['Ticker'], 'Preco': row['Preco'], 'Qtd': qtd, 'Total': custo, 'Motivo Compra': row['Motivo']})
        
        # Usa o troco para reforçar
        for item in carteira:
            while saldo >= item['Preco']:
                item['Qtd'] += 1
                item['Total'] += item['Preco']
                saldo -= item['Preco']
                total_gasto += item['Preco']

    # RELATÓRIO FINAL
    df_cart = pd.DataFrame(carteira)
    if not df_cart.empty:
        # Exibe formatado
        cols = ['Ticker', 'Preco', 'Qtd', 'Total', 'Motivo Compra']
        print(df_cart[cols].to_string(index=False, formatters={
            'Preco': 'R$ {:,.2f}'.format,
            'Total': 'R$ {:,.2f}'.format
        }))
        print("-" * 80)
        print(f"Total Investido: R$ {total_gasto:.2f}")
        print(f"Troco (Caixa):   R$ {saldo:.2f}")
        print("-" * 80)
        print("💡 DICA: A coluna 'Motivo Compra' mostra quais critérios (Graham, Bazin, Buffett)")
        print("         fizeram esse ativo pontuar alto no algoritmo.")
    else:
        print("Dinheiro insuficiente para comprar até mesmo o ativo mais barato da lista Top Picks.")

if __name__ == "__main__":
    df = obter_dados_base()
    if not df.empty:
        df = stage_1_graham_permissivo(df)
        if not df.empty:
            df_final = stage_3_ranking_final(df)
            montar_carteira_real(df_final)
        else:
            print("Nenhum ativo passou nos filtros de segurança.")
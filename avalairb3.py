import math

import fundamentus
import pandas as pd
import requests
import yfinance as yf

# --- CONFIGURAÇÕES DE USUÁRIO ---
DINHEIRO_DISPONIVEL = float(input("Dinheiro disponível: "))
MIN_LIQUIDEZ = 200_000       # Liquidez mínima
MIN_DY = 0.06                # 6% ao ano

print("🚀 Iniciando Varredura Global na B3...")
print(f"💰 Buscando ativos abaixo de R$ {DINHEIRO_DISPONIVEL:.2f}")

# --- FUNÇÃO MANUAL PARA FIIs (CORREÇÃO DO ERRO) ---
def listar_fiis_manual():
    """
    Busca a tabela de FIIs diretamente do site Fundamentus,
    já que a biblioteca oficial falhou.
    """
    url = 'https://www.fundamentus.com.br/fii_resultado.php'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        r = requests.get(url, headers=headers)
        # Lê a tabela HTML
        df_list = pd.read_html(r.content, decimal=',', thousands='.')
        if not df_list:
            return pd.DataFrame()
            
        df = df_list[0]
        
        # Renomeia colunas para facilitar
        df.columns = [
            'Papel', 'Segmento', 'Cotacao', 'FFO_Yield', 'DY', 'P_VP', 
            'Valor_Mercado', 'Liquidez', 'Qtd_Imoveis', 'Preco_m2', 
            'Aluguel_m2', 'Cap_Rate', 'Vacancia'
        ]
        
        # Limpeza de dados (Converter strings % e R$ para float)
        # O Pandas read_html com decimal=',' ajuda, mas porcentagens vêm como string "10,5%"
        def limpar_percentual(x):
            if isinstance(x, str):
                return float(x.replace('%', '').replace('.', '').replace(',', '.')) / 100
            return x

        df['DY'] = df['DY'].apply(limpar_percentual)
        df['P_VP'] = df['P_VP'] / 100 if df['P_VP'].max() > 100 else df['P_VP'] # Ajuste se vier sem escala
        
        # Ajusta índice para ser igual ao da biblioteca (Ticker)
        df.set_index('Papel', inplace=True)
        
        # Renomeia para padronizar com o código principal
        df.rename(columns={
            'Cotacao': 'cotacao', 
            'Liquidez': 'liquidez', 
            'DY': 'dy', 
            'P_VP': 'p_vp',
            'Segmento': 'segmento'
        }, inplace=True)
        
        return df
        
    except Exception as e:
        print(f"⚠️ Erro ao buscar FIIs manualmente: {e}")
        return pd.DataFrame()

def buscar_candidatos_fundamentus():
    candidatos = []

    # --- 1. BUSCAR AÇÕES (Biblioteca funciona bem aqui) ---
    print("📥 Baixando dados de TODAS as Ações...")
    try:
        df_acoes = fundamentus.get_resultado()
        
        # Filtros de Ações
        filtro_acoes = (
            (df_acoes['cotacao'] <= DINHEIRO_DISPONIVEL) &
            (df_acoes['liq2m'] > MIN_LIQUIDEZ) &
            (df_acoes['dy'] >= MIN_DY) &
            (df_acoes['pl'] > 0)
        )
        df_acoes_filtrado = df_acoes[filtro_acoes].copy()
        
        for ticker, row in df_acoes_filtrado.iterrows():
            candidatos.append({
                'ticker': ticker + ".SA",
                'tipo': 'ACAO',
                'setor': 'Geral', 
                'preco_base': row['cotacao'],
                'dy_base': row['dy']
            })
    except Exception as e:
        print(f"❌ Erro ao buscar Ações: {e}")

    # --- 2. BUSCAR FIIS (Usando nossa função manual) ---
    print("📥 Baixando dados de TODOS os FIIs (Modo Manual)...")
    df_fiis = listar_fiis_manual()
    
    if not df_fiis.empty:
        # Filtros de FIIs
        filtro_fiis = (
            (df_fiis['cotacao'] <= DINHEIRO_DISPONIVEL) &
            (df_fiis['liquidez'] > MIN_LIQUIDEZ) &
            (df_fiis['dy'] >= MIN_DY) &
            (df_fiis['p_vp'] < 1.3) # Aceita até 1.3 de P/VP
        )
        df_fiis_filtrado = df_fiis[filtro_fiis].copy()

        for ticker, row in df_fiis_filtrado.iterrows():
            candidatos.append({
                'ticker': ticker + ".SA",
                'tipo': 'FII',
                'setor': row['segmento'],
                'preco_base': row['cotacao'],
                'dy_base': row['dy']
            })

    return pd.DataFrame(candidatos)

def refinar_com_yfinance(df_candidatos):
    if df_candidatos.empty:
        return pd.DataFrame()

    print(f"🔬 Refinando {len(df_candidatos)} ativos promissores com dados históricos...")
    
    tickers = df_candidatos['ticker'].tolist()
    
    # Download em batch otimizado
    try:
        dados_hist = yf.download(tickers, period="6mo", group_by='ticker', threads=True, progress=True)
    except Exception as e:
        print(f"Erro no download do Yahoo: {e}")
        return pd.DataFrame()
    
    resultados_finais = []

    for index, row in df_candidatos.iterrows():
        t = row['ticker']
        try:
            # Lidar com MultiIndex do Yahoo ou Index simples
            if len(tickers) > 1:
                hist = dados_hist[t]
            else:
                hist = dados_hist
            
            if hist.empty: continue

            # Preço Atual e Validação
            preco_atual = float(hist['Close'].iloc[-1])
            if math.isnan(preco_atual) or preco_atual > DINHEIRO_DISPONIVEL: continue

            # Momentum
            preco_6m = float(hist['Close'].iloc[0])
            momentum = (preco_atual / preco_6m) - 1

            # Score System
            score = 0
            motivos = []

            # DY
            if row['dy_base'] > 0.10: score += 2.5
            elif row['dy_base'] >= 0.06: score += 2
            motivos.append(f"DY {row['dy_base']:.1%}")

            # Momentum
            if momentum > 0.05: 
                score += 1.5
                motivos.append(f"Alta 6m ({momentum:.1%})")
            elif momentum < -0.10:
                score -= 1 

            # Bonus Setor FII
            if row['tipo'] == 'FII' and isinstance(row['setor'], str):
                 if any(x in row['setor'] for x in ['Recebíveis', 'Papel', 'Logística', 'Híbrido', 'Shoppings']):
                    score += 0.5

            # Perfil
            perfil = "NEUTRO"
            if score >= 3.5: perfil = "💎 JOIA RARA"
            elif score >= 2.5: perfil = "✅ COMPRA FORTE"
            
            if score >= 2:
                resultados_finais.append({
                    'ticker': t.replace('.SA', ''),
                    'tipo': row['tipo'],
                    'setor': row['setor'],
                    'preco': preco_atual,
                    'dy': row['dy_base'],
                    'momentum': momentum,
                    'score': score,
                    'perfil': perfil,
                    'motivos': ", ".join(motivos)
                })

        except Exception:
            continue

    return pd.DataFrame(resultados_finais).sort_values(by='score', ascending=False)

# --- EXECUÇÃO ---
df_bruto = buscar_candidatos_fundamentus()

if df_bruto.empty:
    print("❌ Nenhum ativo encontrado com esses filtros iniciais.")
else:
    df_final = refinar_com_yfinance(df_bruto)

    if not df_final.empty:
        top_pick = df_final.iloc[0]
        qtd_compra = math.floor(DINHEIRO_DISPONIVEL / top_pick['preco'])
        sobra = DINHEIRO_DISPONIVEL - (qtd_compra * top_pick['preco'])

        print("\n🏆 --- O VENCEDOR DA VARREDURA ---")
        print(f"Ativo:   {top_pick['ticker']} ({top_pick['setor']})")
        print(f"Preço:   R$ {top_pick['preco']:.2f}")
        print(f"DY:      {top_pick['dy']:.1%}")
        print(f"Score:   {top_pick['score']}")
        print(f"Motivo:  {top_pick['motivos']}")
        print("\n🛒 ORDEM SUGERIDA:")
        print(f"Comprar: {qtd_compra} unidades")
        print(f"Total:   R$ {qtd_compra * top_pick['preco']:.2f}")
        print(f"Troco:   R$ {sobra:.2f}")

        print("\n📜 --- TOP 10 MELHORES ALTERNATIVAS ---")
        display_cols = ['ticker', 'preco', 'dy', 'momentum', 'score', 'perfil']
        print(df_final[display_cols].head(10).to_string(index=False, formatters={
            'preco': 'R$ {:,.2f}'.format,
            'dy': '{:,.1%}'.format,
            'momentum': '{:,.1%}'.format
        }))
    else:
        print("⚠️ Ativos encontrados na triagem bruta, mas reprovados na análise fina.")
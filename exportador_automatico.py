import pandas as pd
import sqlite3
import numpy as np
import json
import time
import datetime
import os

# --- CAMINHOS DOS BANCOS DE DADOS ---
PATH_CONSUMO = r"\\srv2\EXPAMA\QUALIDADE\base de dados\Consumo01.db"
PATH_EXPEDICAO = r"\\srv2\EXPAMA\QUALIDADE\base de dados\Expedicao01.db"
PATH_ESTOQUE_INICIAL = r"\\srv2\EXPAMA\QUALIDADE\base de dados\EstoqueInicial.db"
PATH_PRODUCAO = r"\\srv2\EXPAMA\QUALIDADE\base de dados\Producao01.db"
PATH_BOM = r"\\srv2\EXPAMA\QUALIDADE\base de dados\Base BOM.db"

# --- CAMINHO DE SAÍDA ---
CAMINHO_SAIDA_JS = r"\\srv2\EXPAMA\QUALIDADE\base de dados\dados_atuais.js"

# --- Nomes das Colunas (Constantes) ---
# (Seus nomes de colunas originais, não precisei alterar)
COL_CARTEIRA_PO = 'Numero_PO'
COL_CARTEIRA_CLIENTE = 'Cliente'
COL_CARTEIRA_PERFIL = 'Perfil'
COL_CARTEIRA_PRODUTO = 'Produto'
COL_CARTEIRA_PECAS_SOL = 'Qtde_Pecas_Solicitada'
COL_CARTEIRA_VOLUME_SOL = 'Volume_M3'
COL_SAIDA_PO = 'NumeroDaPO'
COL_SAIDA_PRODUTO = 'Produto'
COL_SAIDA_PERFIL = 'Perfil'
COL_SAIDA_PECAS_ENV = 'Qtd_Pecas_Enviada'
COL_SAIDA_ESP = 'Espessura'
COL_SAIDA_LAR = 'Largura'
COL_SAIDA_COM = 'Comprimento'
COL_ESTOQUE_PRODUTO = 'Produto'
COL_ESTOQUE_PERFIL = 'Perfil'
COL_ESTOQUE_M3 = 'Quant_M3'
COL_ESTOQUE_PECAS_POR_PACOTE = 'Qtd_Pecas_por_Pacote'
COL_ESTOQUE_PACOTES = 'Qtd_de_Pacotes'
COL_PRODUCAO_PRODUTO = 'Produto'
COL_PRODUCAO_PERFIL = 'ModeloPerfil'
COL_PRODUCAO_M3 = 'QuantM3'
COL_PRODUCAO_PECAS = 'QtdPecasPacote'
COL_PRODUCAO_PACOTES = 'QtdPacote'
COL_PRODUCAO_ESP = 'Espessura'
COL_PRODUCAO_LAR = 'Largura'
COL_PRODUCAO_COM = 'Comprimento'

# --- Classe JSON Encoder ---
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            # Arredondar para 6 casas decimais para evitar números muito longos
            return round(float(obj), 6) 
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

# --- Funções de Leitura ---
def load_estoque_inicial_bruto():
    conn_estoque = None
    try:
        conn_estoque = sqlite3.connect(PATH_ESTOQUE_INICIAL)
        query_inicial = f"""
            SELECT
                TRIM("{COL_ESTOQUE_PRODUTO}") as Produto,
                TRIM("{COL_ESTOQUE_PERFIL}") as Perfil,
                SUM("{COL_ESTOQUE_PECAS_POR_PACOTE}" * "{COL_ESTOQUE_PACOTES}") as Total_Pecas,
                SUM("{COL_ESTOQUE_M3}") as Total_M3
            FROM EstoqueInicial
            GROUP BY 1, 2
        """
        df = pd.read_sql(query_inicial, conn_estoque)
        return df
    except Exception as e:
        print(f"[ERRO] Falha ao ler EstoqueInicial: {e}")
        return pd.DataFrame(columns=['Produto', 'Perfil', 'Total_Pecas', 'Total_M3'])
    finally:
        if conn_estoque: conn_estoque.close()

def load_producao_bruta():
    conn_producao = None
    try:
        conn_producao = sqlite3.connect(PATH_PRODUCAO)
        query_producao = f"""
            SELECT
                TRIM("{COL_PRODUCAO_PRODUTO}") as Produto,
                TRIM("{COL_PRODUCAO_PERFIL}") as Perfil,
                SUM(
                    CASE 
                        WHEN COALESCE("{COL_PRODUCAO_ESP}", 0) * COALESCE("{COL_PRODUCAO_LAR}", 0) * COALESCE("{COL_PRODUCAO_COM}", 0) > 0.00001 AND "{COL_PRODUCAO_M3}" > 0
                        THEN ROUND(
                            "{COL_PRODUCAO_M3}" / ("{COL_PRODUCAO_ESP}" * "{COL_PRODUCAO_LAR}" * "{COL_PRODUCAO_COM}"), 
                            0
                        )
                        WHEN "{COL_PRODUCAO_PECAS}" * "{COL_PRODUCAO_PACOTES}" > 0
                        THEN "{COL_PRODUCAO_PECAS}" * "{COL_PRODUCAO_PACOTES}"
                        ELSE 0 
                    END
                ) as Total_Pecas,
                SUM("{COL_PRODUCAO_M3}") as Total_M3
            FROM Producao01
            GROUP BY 1, 2
        """
        df = pd.read_sql(query_producao, conn_producao)
        # Renomeia a coluna de Perfil para padronizar
        df = df.rename(columns={'Perfil': 'Perfil'}) 
        return df
    except Exception as e:
        print(f"[ERRO] Falha ao ler Producao01: {e}")
        return pd.DataFrame(columns=['Produto', 'Perfil', 'Total_Pecas', 'Total_M3'])
    finally:
        if conn_producao: conn_producao.close()

def load_saidas_brutas_agrupadas():
    conn_expedicao = None
    try:
        conn_expedicao = sqlite3.connect(PATH_EXPEDICAO)
        query = f"""
            SELECT
                TRIM("{COL_SAIDA_PRODUTO}") as Produto,
                TRIM("{COL_SAIDA_PERFIL}") as Perfil,
                SUM("{COL_SAIDA_PECAS_ENV}") as Total_Pecas_Enviadas,
                SUM(
                    "{COL_SAIDA_PECAS_ENV}" * "{COL_SAIDA_ESP}" * "{COL_SAIDA_LAR}" * "{COL_SAIDA_COM}"
                ) as Total_M3_Enviados
            FROM Expedicao
            GROUP BY 1, 2
        """
        df = pd.read_sql(query, conn_expedicao)
        return df
    except Exception as e:
        print(f"[ERRO] Falha ao ler Expedicao (Saídas Brutas): {e}")
        return pd.DataFrame(columns=['Produto', 'Perfil', 'Total_Pecas_Enviadas', 'Total_M3_Enviados'])
    finally:
        if conn_expedicao: conn_expedicao.close()

# ---
# --- FUNÇÃO 1 CORRIGIDA (PRESERVA DIMENSÕES) ---
# ---
def load_pedidos_solicitados_por_po():
    conn_consumo = None
    try:
        conn_consumo = sqlite3.connect(PATH_CONSUMO)
        
        # ASSUMINDO que os nomes das colunas de dimensão em 'CarteiraPedidos' 
        # são 'Espessura', 'Largura', 'Comprimento'
        query = f"""
            SELECT
                TRIM("{COL_CARTEIRA_PO}") as PO,
                TRIM("{COL_CARTEIRA_CLIENTE}") as Cliente,
                TRIM("{COL_CARTEIRA_PERFIL}") as Perfil,
                TRIM("{COL_CARTEIRA_PRODUTO}") as Produto,
                "Espessura",  -- ADICIONADO
                "Largura",    -- ADICIONADO
                "Comprimento",-- ADICIONADO
                SUM("{COL_CARTEIRA_PECAS_SOL}") as Solicitado_Pecas,
                SUM("{COL_CARTEIRA_VOLUME_SOL}") as Solicitado_M3
            FROM CarteiraPedidos
            WHERE "{COL_CARTEIRA_PO}" IS NOT NULL
              AND TRIM("{COL_CARTEIRA_PO}") <> 'SOBRAS'
              AND TRIM("{COL_CARTEIRA_PO}") <> ''
            GROUP BY 1, 2, 3, 4, 5, 6, 7  -- ADICIONADO
        """
        df = pd.read_sql(query, conn_consumo)
        
        # O rename não é mais necessário se as colunas já são 'Produto', 'Perfil'
        # df = df.rename(columns={ ... })
        
        df['Vol_Medio_Peca'] = np.where(
            df['Solicitado_Pecas'] > 0,
            df['Solicitado_M3'] / df['Solicitado_Pecas'],
            0
        )
        return df
    except Exception as e:
        print(f"[ERRO] Falha ao ler CarteiraPedidos (Solicitados): {e}")
        # Adiciona colunas de dimensão ao fallback
        return pd.DataFrame(columns=[
            'PO', 'Cliente', 'Perfil', 'Produto', 
            'Espessura', 'Largura', 'Comprimento',
            'Solicitado_Pecas', 'Solicitado_M3', 'Vol_Medio_Peca'
        ])
    finally:
        if conn_consumo: conn_consumo.close()

# ---
# --- FUNÇÃO 2 CORRIGIDA (PRESERVA DIMENSÕES) ---
# ---
def load_pedidos_enviados_por_po():
    conn_expedicao = None
    try:
        conn_expedicao = sqlite3.connect(PATH_EXPEDICAO)
        
        # Adicionamos as dimensões ao agrupamento e alias para padronizar
        query = f"""
            SELECT
                TRIM("{COL_SAIDA_PO}") as PO,
                TRIM("{COL_SAIDA_PRODUTO}") as Produto,
                TRIM("{COL_SAIDA_PERFIL}") as Perfil,
                "{COL_SAIDA_ESP}" as Espessura,  -- ADICIONADO
                "{COL_SAIDA_LAR}" as Largura,    -- ADICIONADO
                "{COL_SAIDA_COM}" as Comprimento,-- ADICIONADO
                SUM("{COL_SAIDA_PECAS_ENV}") as Enviado_Pecas
            FROM Expedicao
            WHERE "{COL_SAIDA_PO}" IS NOT NULL
              AND TRIM("{COL_SAIDA_PO}") <> 'SOBRAS'
            GROUP BY 1, 2, 3, 4, 5, 6 -- ADICIONADO
        """
        df = pd.read_sql(query, conn_expedicao)
        return df
    except Exception as e:
        print(f"[ERRO] Falha ao ler Expedicao (Enviados por PO): {e}")
        # Adiciona colunas de dimensão ao fallback
        return pd.DataFrame(columns=[
            'PO', 'Produto', 'Perfil', 
            'Espessura', 'Largura', 'Comprimento', 
            'Enviado_Pecas'
        ])
    finally:
        if conn_expedicao: conn_expedicao.close()


# ---
# --- FUNÇÃO 3 CORRIGIDA (PRESERVA DIMENSÕES) ---
# ---
def load_bom_data():
    """
    Lê a tabela de Estrutura de Produto (BOM) do banco de dados,
    incluindo as dimensões-chave.
    """
    conn_bom = None
    try:
        conn_bom = sqlite3.connect(PATH_BOM)
        
        # A consulta agora inclui as dimensões e padroniza os nomes
        query = """
            SELECT
                TRIM("PRODUTO") as "Produto",
                TRIM("PERFIL") as "Perfil",
                "ESPESSURA" as "Espessura",
                "LARGURA" as "Largura",
                "COMPRIMENTO" as "Comprimento",
                TRIM("DESCRIÇÃO DOS INSUMOS") as "Componente",
                "CONSUMO POR 1M³" as "Consumo_M3",
                "CONSUMO PEÇA" as "Consumo_Peca",
                TRIM("UNIDADE MEDIDA") as "Unidade_Medida",
                TRIM("TIPO INSUMO") as "Tipo_Insumo"
            FROM insumos
            WHERE "CONSUMO PEÇA" > 0 OR "CONSUMO POR 1M³" > 0
        """
        df = pd.read_sql(query, conn_bom)
        print(f"Estrutura BOM lida com sucesso ({len(df)} linhas).")
        return df
        
    except Exception as e:
        print(f"[ERRO] Falha ao ler Base BOM.db: {e}")
        return pd.DataFrame(columns=[
            'Produto', 'Perfil', 'Espessura', 'Largura', 'Comprimento',
            'Componente', 'Consumo_M3', 'Consumo_Peca', 'Unidade_Medida', 'Tipo_Insumo'
        ])
    finally:
        if conn_bom: conn_bom.close()


def exportar_dados():
    """
    Função principal que lê todos os bancos, processa os dados
    e salva o arquivo 'dados_atuais.js'.
    """
    print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}] --- Iniciando atualização ---")

    # --- 1. Calcular Estoque Líquido ---
    # (Esta parte está OK, pois o estoque é agregado por Produto/Perfil)
    print("Lendo Estoque Inicial...")
    df_estoque_bruto = load_estoque_inicial_bruto()
    print("Lendo Produção...")
    df_producao_bruta = load_producao_bruta()
    print("Lendo Saídas (Expedição)...")
    df_saidas_brutas = load_saidas_brutas_agrupadas()

    if df_estoque_bruto.empty and df_producao_bruta.empty and df_saidas_brutas.empty:
        print("[AVISO] Tabelas de estoque/produção/saída vazias. Estoque líquido será 0.")
        df_estoque_liquido = pd.DataFrame(columns=['Produto', 'Perfil', 'M3_Estoque_Liquido', 'Pecas_Estoque_Liquido'])
    else:
        df_entradas = pd.merge(
            df_estoque_bruto, df_producao_bruta,
            on=['Produto', 'Perfil'], how='outer', suffixes=('_Estoque', '_Prod')
        ).fillna(0)
        df_entradas['Total_Entrada_Pecas'] = df_entradas['Total_Pecas_Estoque'] + df_entradas['Total_Pecas_Prod']
        df_entradas['Total_Entrada_M3'] = df_entradas['Total_M3_Estoque'] + df_entradas['Total_M3_Prod']

        df_estoque_liquido = pd.merge(
            df_entradas[['Produto', 'Perfil', 'Total_Entrada_Pecas', 'Total_Entrada_M3']],
            df_saidas_brutas, on=['Produto', 'Perfil'], how='outer'
        ).fillna(0)
        df_estoque_liquido['M3_Estoque_Liquido'] = df_estoque_liquido['Total_Entrada_M3'] - df_estoque_liquido['Total_M3_Enviados']
        df_estoque_liquido['Pecas_Estoque_Liquido'] = df_estoque_liquido['Total_Entrada_Pecas'] - df_estoque_liquido['Total_Pecas_Enviadas']

    estoque_liquido_json = df_estoque_liquido[['Produto', 'Perfil', 'M3_Estoque_Liquido', 'Pecas_Estoque_Liquido']].to_dict('records')
    print(f"Estoque Líquido calculado ({len(estoque_liquido_json)} linhas).")


    # --- 2. Calcular Itens Pendentes (COM DIMENSÕES) ---
    print("Lendo Carteira de Pedidos (com dimensões)...")
    df_solicitado_po = load_pedidos_solicitados_por_po()
    print("Lendo Saídas por PO (com dimensões)...")
    df_enviado_po = load_pedidos_enviados_por_po()

    if df_solicitado_po.empty:
        print("[AVISO] Tabela de Carteira vazia. Itens pendentes serão 0.")
        itens_pendentes_json = []
    else:
        # --- CORREÇÃO AQUI ---
        # A chave do merge agora inclui as dimensões
        CHAVE_MERGE = ['PO', 'Produto', 'Perfil', 'Espessura', 'Largura', 'Comprimento']
        
        df_merged_po = pd.merge(
            df_solicitado_po, df_enviado_po,
            on=CHAVE_MERGE, how="left"
        ).fillna(0)

        df_merged_po['Enviado_M3_Calc'] = df_merged_po['Enviado_Pecas'] * df_merged_po['Vol_Medio_Peca']
        df_merged_po['Pendente_M3'] = df_merged_po['Solicitado_M3'] - df_merged_po['Enviado_M3_Calc']
        df_merged_po['Pendente_Pecas'] = df_merged_po['Solicitado_Pecas'] - df_merged_po['Enviado_Pecas']

        # --- CORREÇÃO AQUI ---
        # Adicionamos as dimensões às colunas de exportação
        colunas_pendentes = [
            'PO', 'Cliente', 'Perfil', 'Produto',
            'Espessura', 'Largura', 'Comprimento', # ADICIONADAS
            'Solicitado_Pecas', 'Solicitado_M3',
            'Enviado_Pecas', 'Enviado_M3_Calc',
            'Pendente_M3', 'Pendente_Pecas'
        ]
        colunas_presentes = [col for col in colunas_pendentes if col in df_merged_po.columns]
        itens_pendentes_json = df_merged_po[colunas_presentes].to_dict('records')

    print(f"Itens Pendentes calculados ({len(itens_pendentes_json)} linhas).")


    # --- 3. NOVA ETAPA: Ler a Estrutura BOM ---
    print("Lendo Estrutura BOM (com dimensões)...")
    df_bom = load_bom_data()
    bom_json = df_bom.to_dict('records')


    # --- 4. Gravar o arquivo .js ---
    print("\n--- Gravando arquivo ---")
    try:
        os.makedirs(os.path.dirname(CAMINHO_SAIDA_JS), exist_ok=True)
    except Exception as e:
        print(f"[ERRO GRAVE] Não foi possível criar diretório: {os.path.dirname(CAMINHO_SAIDA_JS)}\n{e}")
        return

    try:
        with open(CAMINHO_SAIDA_JS, 'w', encoding='utf-8') as f:
            f.write("const MOCK_ESTOQUE_LIQUIDO = ")
            json.dump(estoque_liquido_json, f, ensure_ascii=False, indent=2, cls=NpEncoder)
            f.write(";\n\n")

            f.write("const MOCK_DADOS_PENDENTES_DETALHADOS = ")
            json.dump(itens_pendentes_json, f, ensure_ascii=False, indent=2, cls=NpEncoder)
            f.write(";\n\n")

            # Adicionando o BOM ao arquivo JS
            f.write("const DADOS_BOM = ") # Nomeado DADOS_BOM, mais intuitivo
            json.dump(bom_json, f, ensure_ascii=False, indent=2, cls=NpEncoder)
            f.write(";\n")

        print(f"\n[SUCESSO] Arquivo '{CAMINHO_SAIDA_JS}' gravado.")

    except IOError as e:
        print(f"\n[ERRO GRAVE] IO ao escrever: '{CAMINHO_SAIDA_JS}'\nVerifique permissões.\n{e}")
    except Exception as e:
        print(f"\n[ERRO GRAVE] Erro ao gravar JSON:\n{e}")


# --- Loop Principal ---
if __name__ == "__main__":
    print("--- Exportador Automático de Dados (Motor v1.4.0 - MRP Detalhado) ---") # Versão
    print(f"Lendo bancos de: {os.path.dirname(PATH_CONSUMO)}")
    print(f"Salvando resultados em: {CAMINHO_SAIDA_JS}")
    print("Pressione Ctrl+C para parar.")

    intervalo_segundos = 10 * 60 # 10 minutos

    while True:
        exportar_dados()

        print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}] Dormindo por 10 minutos...")
        try:
            time.sleep(intervalo_segundos)
        except KeyboardInterrupt:
            print("\n[INFO] Loop interrompido. Saindo...")
            break

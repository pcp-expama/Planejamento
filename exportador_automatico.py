import pandas as pd
import sqlite3
import numpy as np
import json
import time
import datetime
import os
import subprocess

# --- CAMINHOS DOS BANCOS DE DADOS ---
PATH_CONSUMO = r"\\srv2\EXPAMA\QUALIDADE\base de dados\Consumo01.db"
PATH_EXPEDICAO = r"\\srv2\EXPAMA\QUALIDADE\base de dados\Expedicao01.db"
PATH_ESTOQUE_INICIAL = r"\\srv2\EXPAMA\QUALIDADE\base de dados\EstoqueInicial.db"
PATH_PRODUCAO = r"\\srv2\EXPAMA\QUALIDADE\base de dados\Producao01.db"
PATH_BOM = r"\\srv2\EXPAMA\QUALIDADE\base de dados\Base BOM.db"

# --- CAMINHO DE SAÍDA E GIT ---
DIRETORIO_BASE = r"\\srv2\EXPAMA\QUALIDADE\base de dados"
CAMINHO_SAIDA_JS = os.path.join(DIRETORIO_BASE, "dados_atuais.js")
CAMINHO_TMP_JS = os.path.join(DIRETORIO_BASE, "dados_atuais.js.tmp")

# --- Nomes das Colunas (Constantes) ---
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
COL_ESTOQUE_ESP = 'Espessura' 
COL_ESTOQUE_LAR = 'Largura'
COL_ESTOQUE_COM = 'Comprimento'
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
            return round(float(obj), 6) 
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

# --- Funções de Leitura (Completas e Inalteradas) ---
def load_estoque_inicial_bruto():
    conn_estoque = None
    try:
        conn_estoque = sqlite3.connect(PATH_ESTOQUE_INICIAL, timeout=20)
        query_inicial = f"""
            SELECT
                TRIM("{COL_ESTOQUE_PRODUTO}") as Produto,
                TRIM("{COL_ESTOQUE_PERFIL}") as Perfil,
                COALESCE("{COL_ESTOQUE_ESP}", 0) as Espessura,
                COALESCE("{COL_ESTOQUE_LAR}", 0) as Largura,
                COALESCE("{COL_ESTOQUE_COM}", 0) as Comprimento,
                SUM("{COL_ESTOQUE_PECAS_POR_PACOTE}" * "{COL_ESTOQUE_PACOTES}") as Total_Pecas,
                SUM("{COL_ESTOQUE_M3}") as Total_M3
            FROM EstoqueInicial
            GROUP BY 1, 2, 3, 4, 5
        """
        return pd.read_sql(query_inicial, conn_estoque)
    except Exception as e:
        print(f"  [ERRO] Falha ao ler EstoqueInicial: {e}")
        return pd.DataFrame(columns=['Produto', 'Perfil', 'Espessura', 'Largura', 'Comprimento', 'Total_Pecas', 'Total_M3'])
    finally:
        if conn_estoque: conn_estoque.close()

def load_producao_bruta():
    conn_producao = None
    try:
        conn_producao = sqlite3.connect(PATH_PRODUCAO, timeout=20)
        query_producao = f"""
            SELECT
                TRIM("{COL_PRODUCAO_PRODUTO}") as Produto,
                TRIM("{COL_PRODUCAO_PERFIL}") as Perfil,
                COALESCE("{COL_PRODUCAO_ESP}", 0) as Espessura,
                COALESCE("{COL_PRODUCAO_LAR}", 0) as Largura,
                COALESCE("{COL_PRODUCAO_COM}", 0) as Comprimento,
                SUM(
                    CASE 
                        WHEN COALESCE("{COL_PRODUCAO_ESP}", 0) * COALESCE("{COL_PRODUCAO_LAR}", 0) * COALESCE("{COL_PRODUCAO_COM}", 0) > 0.00001 AND "{COL_PRODUCAO_M3}" > 0
                        THEN ROUND("{COL_PRODUCAO_M3}" / ("{COL_PRODUCAO_ESP}" * "{COL_PRODUCAO_LAR}" * "{COL_PRODUCAO_COM}"), 0)
                        WHEN "{COL_PRODUCAO_PECAS}" * "{COL_PRODUCAO_PACOTES}" > 0
                        THEN "{COL_PRODUCAO_PECAS}" * "{COL_PRODUCAO_PACOTES}"
                        ELSE 0 
                    END
                ) as Total_Pecas,
                SUM("{COL_PRODUCAO_M3}") as Total_M3
            FROM Producao01
            GROUP BY 1, 2, 3, 4, 5
        """
        df = pd.read_sql(query_producao, conn_producao)
        return df.rename(columns={'Perfil': 'Perfil'}) 
    except Exception as e:
        print(f"  [ERRO] Falha ao ler Producao01: {e}")
        return pd.DataFrame(columns=['Produto', 'Perfil', 'Espessura', 'Largura', 'Comprimento', 'Total_Pecas', 'Total_M3'])
    finally:
        if conn_producao: conn_producao.close()

def load_saidas_brutas_agrupadas():
    conn_expedicao = None
    try:
        conn_expedicao = sqlite3.connect(PATH_EXPEDICAO, timeout=20)
        query = f"""
            SELECT
                TRIM("{COL_SAIDA_PRODUTO}") as Produto,
                TRIM("{COL_SAIDA_PERFIL}") as Perfil,
                COALESCE("{COL_SAIDA_ESP}", 0) as Espessura,
                COALESCE("{COL_SAIDA_LAR}", 0) as Largura,
                COALESCE("{COL_SAIDA_COM}", 0) as Comprimento,
                SUM("{COL_SAIDA_PECAS_ENV}") as Total_Pecas_Enviadas,
                SUM("{COL_SAIDA_PECAS_ENV}" * "{COL_SAIDA_ESP}" * "{COL_SAIDA_LAR}" * "{COL_SAIDA_COM}") as Total_M3_Enviados
            FROM Expedicao
            GROUP BY 1, 2, 3, 4, 5
        """
        return pd.read_sql(query, conn_expedicao)
    except Exception as e:
        print(f"  [ERRO] Falha ao ler Expedicao: {e}")
        return pd.DataFrame(columns=['Produto', 'Perfil', 'Espessura', 'Largura', 'Comprimento', 'Total_Pecas_Enviadas', 'Total_M3_Enviados'])
    finally:
        if conn_expedicao: conn_expedicao.close()

def load_pedidos_solicitados_por_po():
    conn_consumo = None
    try:
        conn_consumo = sqlite3.connect(PATH_CONSUMO, timeout=20)
        query = f"""
            SELECT
                TRIM("{COL_CARTEIRA_PO}") as PO,
                TRIM("{COL_CARTEIRA_CLIENTE}") as Cliente,
                TRIM("{COL_CARTEIRA_PERFIL}") as Perfil,
                TRIM("{COL_CARTEIRA_PRODUTO}") as Produto,
                "Espessura", "Largura", "Comprimento",
                SUM("{COL_CARTEIRA_PECAS_SOL}") as Solicitado_Pecas,
                SUM("{COL_CARTEIRA_VOLUME_SOL}") as Solicitado_M3
            FROM CarteiraPedidos
            WHERE "{COL_CARTEIRA_PO}" IS NOT NULL AND TRIM("{COL_CARTEIRA_PO}") <> 'SOBRAS' AND TRIM("{COL_CARTEIRA_PO}") <> ''
            GROUP BY 1, 2, 3, 4, 5, 6, 7 
        """
        df = pd.read_sql(query, conn_consumo)
        df['Vol_Medio_Peca'] = np.where(df['Solicitado_Pecas'] > 0, df['Solicitado_M3'] / df['Solicitado_Pecas'], 0)
        return df
    except Exception as e:
        return pd.DataFrame(columns=['PO', 'Cliente', 'Perfil', 'Produto', 'Espessura', 'Largura', 'Comprimento', 'Solicitado_Pecas', 'Solicitado_M3', 'Vol_Medio_Peca'])
    finally:
        if conn_consumo: conn_consumo.close()

def load_pedidos_enviados_por_po():
    conn_expedicao = None
    try:
        conn_expedicao = sqlite3.connect(PATH_EXPEDICAO, timeout=20)
        query = f"""
            SELECT
                TRIM("{COL_SAIDA_PO}") as PO, TRIM("{COL_SAIDA_PRODUTO}") as Produto, TRIM("{COL_SAIDA_PERFIL}") as Perfil,
                "{COL_SAIDA_ESP}" as Espessura, "{COL_SAIDA_LAR}" as Largura, "{COL_SAIDA_COM}" as Comprimento,
                SUM("{COL_SAIDA_PECAS_ENV}") as Enviado_Pecas
            FROM Expedicao
            WHERE "{COL_SAIDA_PO}" IS NOT NULL AND TRIM("{COL_SAIDA_PO}") <> 'SOBRAS'
            GROUP BY 1, 2, 3, 4, 5, 6
        """
        return pd.read_sql(query, conn_expedicao)
    except Exception as e:
        return pd.DataFrame(columns=['PO', 'Produto', 'Perfil', 'Espessura', 'Largura', 'Comprimento', 'Enviado_Pecas'])
    finally:
        if conn_expedicao: conn_expedicao.close()

def load_bom_data():
    conn_bom = None
    try:
        conn_bom = sqlite3.connect(PATH_BOM, timeout=20)
        query = """
            SELECT
                TRIM("PRODUTO") as "Produto", TRIM("PERFIL") as "Perfil", "ESPESSURA" as "Espessura", "LARGURA" as "Largura", "COMPRIMENTO" as "Comprimento",
                TRIM("DESCRIÇÃO DOS INSUMOS") as "Componente", "CONSUMO POR 1M³" as "Consumo_M3", "CONSUMO PEÇA" as "Consumo_Peca",
                TRIM("UNIDADE MEDIDA") as "Unidade_Medida", TRIM("TIPO INSUMO") as "Tipo_Insumo"
            FROM insumos
            WHERE "CONSUMO PEÇA" > 0 OR "CONSUMO POR 1M³" > 0
        """
        return pd.read_sql(query, conn_bom)
    except Exception as e:
        return pd.DataFrame(columns=['Produto', 'Perfil', 'Espessura', 'Largura', 'Comprimento', 'Componente', 'Consumo_M3', 'Consumo_Peca', 'Unidade_Medida', 'Tipo_Insumo'])
    finally:
        if conn_bom: conn_bom.close()

# --- INTEGRAÇÃO GIT SEGURA ---
def enviar_para_github():
    print("\n[ETAPA 5] Verificando alterações com o Git...")
    try:
        # Força o Git a monitorar o arquivo, ignorando possíveis bloqueios
        subprocess.run(["git", "-C", DIRETORIO_BASE, "add", "-f", "dados_atuais.js"], capture_output=True)
        
        # Pede para o Git checar se o arquivo REALMENTE mudou em relação à nuvem
        status = subprocess.run(["git", "-C", DIRETORIO_BASE, "status", "--porcelain"], capture_output=True, text=True)
        
        if not status.stdout.strip():
            print("  -> [OK] Arquivo gerado é idêntico ao do GitHub. Nenhuma alteração real detectada. Pulando push.")
            return

        print("  -> [!] Alteração detectada! Criando pacote de envio...")
        data_hora = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
        mensagem = f"Auto-update: {data_hora}"
        
        subprocess.run(["git", "-C", DIRETORIO_BASE, "commit", "-m", mensagem], capture_output=True)
        
        print("  -> [!] Fazendo Upload para a Nuvem...")
        push_result = subprocess.run(["git", "-C", DIRETORIO_BASE, "push", "origin", "main"], capture_output=True, text=True)
        
        if "rejected" in push_result.stderr or "fetch first" in push_result.stderr:
             print("  -> [ERRO DE CONFLITO] Sincronização forçada necessária...")
             
             # 1. Puxa as alterações e impõe a nossa versão (-X theirs)
             subprocess.run(["git", "-C", DIRETORIO_BASE, "pull", "origin", "main", "--rebase", "-X", "theirs"], check=True)
             
             # 2. Faz o push final do arquivo resolvido para a nuvem
             subprocess.run(["git", "-C", DIRETORIO_BASE, "push", "origin", "main"], check=True)
             
             print("  -> [SUCESSO] Conflito resolvido e dados publicados no GitHub!")
        else:
             # Só diz que foi sucesso se o erro estiver vazio (push inicial funcionou)
             if push_result.returncode == 0:
                 print("  -> [SUCESSO] Dados publicados no GitHub sem conflitos!")
             else:
                 print(f"  -> [ERRO DESCONHECIDO] O Git retornou: {push_result.stderr}")

    except Exception as e:
        print(f"  -> [ERRO GRAVE GIT] Falha na automação: {e}")

# --- MOTOR PRINCIPAL ---
def exportar_dados():
    print(f"\n=======================================================")
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] INICIANDO CICLO DE ATUALIZAÇÃO")
    
    print("\n[ETAPA 1] Lendo e Calculando Estoque e Produção...")
    df_estoque_bruto = load_estoque_inicial_bruto()
    print(f"  -> Estoque Inicial: {len(df_estoque_bruto)} grupos encontrados.")
    df_producao_bruta = load_producao_bruta()
    print(f"  -> Produção: {len(df_producao_bruta)} grupos encontrados.")
    df_saidas_brutas = load_saidas_brutas_agrupadas()
    print(f"  -> Expedição: {len(df_saidas_brutas)} grupos encontrados.")

    CHAVE_MERGE = ['Produto', 'Perfil', 'Espessura', 'Largura', 'Comprimento']
    
    if df_estoque_bruto.empty and df_producao_bruta.empty and df_saidas_brutas.empty:
        estoque_liquido_json = []
    else:
        df_entradas = pd.merge(df_estoque_bruto, df_producao_bruta, on=CHAVE_MERGE, how='outer', suffixes=('_Estoque', '_Prod')).fillna(0)
        df_entradas['Total_Entrada_Pecas'] = df_entradas['Total_Pecas_Estoque'] + df_entradas['Total_Pecas_Prod']
        df_entradas['Total_Entrada_M3'] = df_entradas['Total_M3_Estoque'] + df_entradas['Total_M3_Prod']

        df_estoque_liquido = pd.merge(df_entradas[CHAVE_MERGE + ['Total_Entrada_Pecas', 'Total_Entrada_M3']], df_saidas_brutas, on=CHAVE_MERGE, how='outer').fillna(0)
        df_estoque_liquido['M3_Estoque_Liquido'] = df_estoque_liquido['Total_Entrada_M3'] - df_estoque_liquido['Total_M3_Enviados']
        df_estoque_liquido['Pecas_Estoque_Liquido'] = df_estoque_liquido['Total_Entrada_Pecas'] - df_estoque_liquido['Total_Pecas_Enviadas']
        estoque_liquido_json = df_estoque_liquido[CHAVE_MERGE + ['M3_Estoque_Liquido', 'Pecas_Estoque_Liquido']].to_dict('records')
    print(f"  -> [OK] Estoque Líquido processado: {len(estoque_liquido_json)} itens.")

    print("\n[ETAPA 2] Lendo e Calculando Carteira de Pedidos...")
    df_solicitado_po = load_pedidos_solicitados_por_po()
    print(f"  -> Pedidos Solicitados: {len(df_solicitado_po)} POs ativas.")
    df_enviado_po = load_pedidos_enviados_por_po()
    print(f"  -> Pedidos Enviados: {len(df_enviado_po)} POs rastreadas.")

    if df_solicitado_po.empty:
        itens_pendentes_json = []
    else:
        CHAVE_MERGE_PO = ['PO', 'Produto', 'Perfil', 'Espessura', 'Largura', 'Comprimento']
        df_merged_po = pd.merge(df_solicitado_po, df_enviado_po, on=CHAVE_MERGE_PO, how="left").fillna(0)
        df_merged_po['Enviado_M3_Calc'] = df_merged_po['Enviado_Pecas'] * df_merged_po['Vol_Medio_Peca']
        df_merged_po['Pendente_M3'] = df_merged_po['Solicitado_M3'] - df_merged_po['Enviado_M3_Calc']
        df_merged_po['Pendente_Pecas'] = df_merged_po['Solicitado_Pecas'] - df_merged_po['Enviado_Pecas']
        itens_pendentes_json = df_merged_po.to_dict('records')
    print(f"  -> [OK] Itens Pendentes processados: {len(itens_pendentes_json)} POs pendentes.")

    print("\n[ETAPA 3] Lendo Estrutura de Insumos (BOM)...")
    df_bom = load_bom_data()
    bom_json = df_bom.to_dict('records')
    print(f"  -> [OK] BOM processado: {len(bom_json)} regras.")

    print("\n[ETAPA 4] Gravando Arquivo JS Local...")
    try:
        os.makedirs(DIRETORIO_BASE, exist_ok=True)
        with open(CAMINHO_TMP_JS, 'w', encoding='utf-8') as f:
            f.write("const MOCK_ESTOQUE_LIQUIDO = ")
            json.dump(estoque_liquido_json, f, ensure_ascii=False, indent=2, cls=NpEncoder)
            f.write(";\n\n")
            f.write("const MOCK_DADOS_PENDENTES_DETALHADOS = ")
            json.dump(itens_pendentes_json, f, ensure_ascii=False, indent=2, cls=NpEncoder)
            f.write(";\n\n")
            f.write("const DADOS_BOM = ")
            json.dump(bom_json, f, ensure_ascii=False, indent=2, cls=NpEncoder)
            f.write(";\n")

        os.replace(CAMINHO_TMP_JS, CAMINHO_SAIDA_JS)
        print(f"  -> [OK] Arquivo '{CAMINHO_SAIDA_JS}' reescrito com sucesso.")
        
        # Inicia a checagem e envio do Git
        enviar_para_github()

    except Exception as e:
        print(f"  -> [ERRO GRAVE] Falha na gravação do arquivo: {e}")

if __name__ == "__main__":
    print("=======================================================")
    print("         MOTOR PCP - EXPORTADOR AUTOMÁTICO v2.0        ")
    print("=======================================================")
    print(f"Diretório Raiz: {DIRETORIO_BASE}")
    print("Pressione Ctrl+C para encerrar o programa.\n")

    while True:
        exportar_dados()
        print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}] Aguardando próximo ciclo (1 minuto)...")
        try:
            time.sleep(60)
        except KeyboardInterrupt:
            print("\n[INFO] Motor encerrado pelo usuário.")
            break

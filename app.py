"""
DRE Interativo - Supermercado Nordestão
Análise de Demonstração do Resultado do Exercício
Suporte a Varejo (Nordestão) e Atacado (Superfácil)
Múltiplos períodos: Consolidado e Parcial
Comparativos dinâmicos por período
"""
import pandas as pd
from flask import Flask, render_template, jsonify, request
import os
import re
from datetime import datetime
from glob import glob

app = Flask(__name__)

# Diretórios de dados
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONSOLIDADO_DIR = os.path.join(BASE_DIR, 'consolidado')
PARCIAL_DIR = os.path.join(BASE_DIR, 'parcial')
SHEET_NAME = 'DRE'

# Mapeamento de meses
MESES = {
    'JAN': '01', 'FEV': '02', 'MAR': '03', 'ABR': '04',
    'MAI': '05', 'JUN': '06', 'JUL': '07', 'AGO': '08',
    'SET': '09', 'OUT': '10', 'NOV': '11', 'DEZ': '12'
}

MESES_NOME = {
    '01': 'Janeiro', '02': 'Fevereiro', '03': 'Março', '04': 'Abril',
    '05': 'Maio', '06': 'Junho', '07': 'Julho', '08': 'Agosto',
    '09': 'Setembro', '10': 'Outubro', '11': 'Novembro', '12': 'Dezembro'
}

CONTAS_PRINCIPAIS = [
    'Receita bruta de vendas',
    'Desc. / Canc. e Devoluções S/ Vendas', 
    'Receita de vendas',
    'Receita bruta de serviços',
    'Impostos e devoluções sobre vendas',
    'Receita líquida total',
    'Custo das mercadorias vendidas',
    'Lucro bruto',
    'Recomposição de margem',
    'Novo lucro bruto',
    'Quebras',
    'Lucro bruto após quebras',
    'GENTE E GESTÃO',
    'EFICIÊNCIA',
    'MANUTENÇÃO',
    'COMERCIAL & MARKETING',
    'Provisões e Depreciações',
    'Prestação de Serviços',
    'Aluguel de Prédios',
    'Outras Desp Financeiras',
    'TI',
    'Gastos ADM & Taxas',
    'Instalações & Utilidades',
    'Mobilidade',
    'Desp. Gerais e Administrativas',
    'Lucro Operacional',
    'OUTROS MARKETING',
    'DESP. CORPORATIVAS',
    'DESP. LOGISTICA',
    'DESP. PRODUÇÕES & INDUSTRIAS',
    'LUCRO ANTES IMPOSTOS',
]

ESTRUTURA_DRE = {
    'Receita bruta de vendas': {'nivel': 1, 'tipo': 'receita', 'expansivel': True},
    'Desc. / Canc. e Devoluções S/ Vendas': {'nivel': 1, 'tipo': 'deducao', 'expansivel': True},
    'Receita de vendas': {'nivel': 1, 'tipo': 'subtotal', 'expansivel': False},
    'Receita bruta de serviços': {'nivel': 1, 'tipo': 'receita', 'expansivel': True},
    'Impostos e devoluções sobre vendas': {'nivel': 1, 'tipo': 'deducao', 'expansivel': True},
    'Impostos e devoluções sobre serviços': {'nivel': 1, 'tipo': 'deducao', 'expansivel': True},
    'Receita líquida total': {'nivel': 0, 'tipo': 'total', 'expansivel': False},
    'Custo das mercadorias vendidas': {'nivel': 1, 'tipo': 'custo', 'expansivel': True},
    'Lucro bruto': {'nivel': 0, 'tipo': 'total', 'expansivel': False},
    'Recomposição de margem': {'nivel': 1, 'tipo': 'receita', 'expansivel': True},
    'Impostos sobre recomposição de margem': {'nivel': 1, 'tipo': 'deducao', 'expansivel': True},
    'Novo lucro bruto': {'nivel': 0, 'tipo': 'total', 'expansivel': False},
    'Quebras': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Lucro bruto após quebras': {'nivel': 0, 'tipo': 'total', 'expansivel': False},
    'GENTE E GESTÃO': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'EFICIÊNCIA': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'MANUTENÇÃO': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'COMERCIAL & MARKETING': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Provisões e Depreciações': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Prestação de Serviços': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Aluguel de Prédios': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Outras Desp Financeiras': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'TI': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Gastos ADM & Taxas': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Instalações & Utilidades': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Mobilidade': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Desp. Gerais e Administrativas': {'nivel': 0, 'tipo': 'subtotal', 'expansivel': False},
    'Lucro Operacional': {'nivel': 0, 'tipo': 'total', 'expansivel': False},
    'OUTROS MARKETING': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'DESP. CORPORATIVAS': {'nivel': 1, 'tipo': 'despesa', 'expansivel': False},
    'DESP. LOGISTICA': {'nivel': 1, 'tipo': 'despesa', 'expansivel': False},
    'DESP. PRODUÇÕES & INDUSTRIAS': {'nivel': 1, 'tipo': 'despesa', 'expansivel': False},
    'LUCRO ANTES IMPOSTOS': {'nivel': 0, 'tipo': 'resultado', 'expansivel': False},
}

def normalize_conta(conta):
    if pd.isna(conta):
        return ''
    return str(conta).strip().lower()

def is_conta_principal(conta):
    conta_norm = normalize_conta(conta)
    for cp in CONTAS_PRINCIPAIS:
        if normalize_conta(cp) == conta_norm:
            return True
    return False

def get_conta_config(conta):
    conta_str = str(conta).strip() if pd.notna(conta) else ''
    for key, config in ESTRUTURA_DRE.items():
        if normalize_conta(key) == normalize_conta(conta_str):
            return config
    if any(x in conta_str.upper() for x in ['LUCRO', 'RESULTADO']):
        if 'ANTES' in conta_str.upper() or 'LÍQUIDO' in conta_str.upper():
            return {'nivel': 0, 'tipo': 'resultado', 'expansivel': False}
        return {'nivel': 0, 'tipo': 'total', 'expansivel': False}
    if conta_str.startswith('(-)'):
        return {'nivel': 2, 'tipo': 'deducao', 'expansivel': False}
    if conta_str.isupper() and len(conta_str) > 3:
        return {'nivel': 2, 'tipo': 'despesa', 'expansivel': False}
    return {'nivel': 2, 'tipo': 'normal', 'expansivel': False}

def find_conta(dados, nome):
    nome_lower = nome.lower()
    for item in dados:
        if item['conta'].lower() == nome_lower:
            return item
    return {'real_anterior': 0, 'real_atual': 0, 'var_ano': 0, 'var_orc_real': 0}

def classificar_loja(nome):
    """Classifica a loja como varejo ou atacado"""
    nome_upper = nome.upper()
    if 'SUPERFÁCIL' in nome_upper or 'SUPERFACIL' in nome_upper:
        return 'atacado'
    return 'varejo'

def extrair_periodo_arquivo(nome_arquivo):
    """Extrai mês e ano do nome do arquivo (ex: DRE_LOJAS_NOV25.xlsx -> NOV/25)"""
    match = re.search(r'_([A-Z]{3})(\d{2})\.xlsx$', nome_arquivo, re.IGNORECASE)
    if match:
        mes = match.group(1).upper()
        ano = match.group(2)
        return {
            'mes_abrev': mes,
            'ano': ano,
            'mes_num': MESES.get(mes, '01'),
            'mes_nome': MESES_NOME.get(MESES.get(mes, '01'), mes),
            'label': f"{mes}/{ano}",
            'sort_key': f"20{ano}{MESES.get(mes, '01')}"
        }
    return None

def listar_periodos():
    """Lista todos os períodos disponíveis (consolidado e parcial)"""
    periodos = {
        'consolidado': [],
        'parcial': [],
        'todos': []
    }
    
    # Listar arquivos consolidados
    if os.path.exists(CONSOLIDADO_DIR):
        for arquivo in glob(os.path.join(CONSOLIDADO_DIR, 'DRE_LOJAS_*.xlsx')):
            nome = os.path.basename(arquivo)
            periodo = extrair_periodo_arquivo(nome)
            if periodo:
                periodo['arquivo'] = arquivo
                periodo['tipo'] = 'consolidado'
                periodos['consolidado'].append(periodo)
    
    # Listar arquivos parciais
    if os.path.exists(PARCIAL_DIR):
        for arquivo in glob(os.path.join(PARCIAL_DIR, 'DRE_LOJAS_*.xlsx')):
            nome = os.path.basename(arquivo)
            periodo = extrair_periodo_arquivo(nome)
            if periodo:
                periodo['arquivo'] = arquivo
                periodo['tipo'] = 'parcial'
                periodos['parcial'].append(periodo)
    
    # Ordenar por data (mais recente primeiro)
    periodos['consolidado'].sort(key=lambda x: x['sort_key'], reverse=True)
    periodos['parcial'].sort(key=lambda x: x['sort_key'], reverse=True)
    
    # Lista de todos os períodos únicos
    todos_periodos = {}
    for p in periodos['consolidado'] + periodos['parcial']:
        if p['label'] not in todos_periodos:
            todos_periodos[p['label']] = {
                'label': p['label'],
                'mes_nome': p['mes_nome'],
                'ano': p['ano'],
                'sort_key': p['sort_key'],
                'tem_consolidado': False,
                'tem_parcial': False
            }
        if p['tipo'] == 'consolidado':
            todos_periodos[p['label']]['tem_consolidado'] = True
        else:
            todos_periodos[p['label']]['tem_parcial'] = True
    
    periodos['todos'] = sorted(todos_periodos.values(), key=lambda x: x['sort_key'], reverse=True)
    
    return periodos

def obter_arquivo(periodo_label, tipo='consolidado'):
    """Obtém o caminho do arquivo para um período e tipo específicos"""
    diretorio = CONSOLIDADO_DIR if tipo == 'consolidado' else PARCIAL_DIR
    
    if not os.path.exists(diretorio):
        return None
    
    for arquivo in glob(os.path.join(diretorio, 'DRE_LOJAS_*.xlsx')):
        nome = os.path.basename(arquivo)
        periodo = extrair_periodo_arquivo(nome)
        if periodo and periodo['label'] == periodo_label:
            return arquivo
    
    return None

def listar_lojas(excel_path):
    """Lista todas as lojas disponíveis no arquivo, separadas por tipo"""
    df = pd.read_excel(excel_path, sheet_name=SHEET_NAME, header=None)
    
    # Data de referência está na célula D4 (coluna 3, linha 3)
    data_ref_raw = df.iloc[3, 3]
    if isinstance(data_ref_raw, (pd.Timestamp, datetime)):
        data_ref = data_ref_raw.strftime('%d/%m/%Y')
    elif pd.notna(data_ref_raw):
        data_ref = str(data_ref_raw)[:10]
    else:
        data_ref = ''
    
    row_lojas = df.iloc[3]
    lojas_varejo = []
    lojas_atacado = []
    
    for col_idx, val in enumerate(row_lojas):
        if pd.notna(val) and isinstance(val, str):
            val_clean = val.strip()
            if 'LJ.' in val_clean or 'SUPERFÁCIL' in val_clean or 'SUPERFACIL' in val_clean:
                tipo = classificar_loja(val_clean)
                loja = {
                    'id': str(col_idx),
                    'nome': val_clean,
                    'col_inicio': col_idx,
                    'data_ref': data_ref,
                    'tipo': tipo
                }
                if tipo == 'varejo':
                    lojas_varejo.append(loja)
                else:
                    lojas_atacado.append(loja)
    
    return {
        'varejo': sorted(lojas_varejo, key=lambda x: x['nome']),
        'atacado': sorted(lojas_atacado, key=lambda x: x['nome']),
        'todas': sorted(lojas_varejo + lojas_atacado, key=lambda x: x['nome']),
        'data_ref': data_ref
    }

def carregar_dre(excel_path, col_inicio=None):
    """Carrega e processa o DRE de uma loja específica"""
    df = pd.read_excel(excel_path, sheet_name=SHEET_NAME, header=None)
    
    lojas = listar_lojas(excel_path)
    
    if col_inicio is None:
        if lojas['todas']:
            col_inicio = lojas['todas'][0]['col_inicio']
        else:
            return None
    
    col_inicio = int(col_inicio)
    
    loja_nome = df.iloc[3, col_inicio]
    tipo_loja = classificar_loja(str(loja_nome))
    
    # Data de referência está na célula D4 (coluna 3, linha 3)
    data_ref_raw = df.iloc[3, 3]
    if isinstance(data_ref_raw, (pd.Timestamp, datetime)):
        data_ref = data_ref_raw.strftime('%d/%m/%Y')
        ano_atual = data_ref_raw.year
    elif pd.notna(data_ref_raw):
        data_ref = str(data_ref_raw)[:10]
        # Tentar extrair ano da string
        try:
            ano_atual = int(str(data_ref_raw)[:4])
        except:
            ano_atual = datetime.now().year
    else:
        data_ref = ''
        ano_atual = datetime.now().year
    
    # Calcular ano anterior para comparativo
    ano_anterior = ano_atual - 1
    
    # Colunas dinâmicas
    col_real_anterior = col_inicio      # Realizado ano anterior
    col_perc_anterior = col_inicio + 1  # % Sobre Fat. ano anterior
    col_orcamento = col_inicio + 2      # Orçamento
    col_perc_orc = col_inicio + 3       # % Sobre Fat. orçamento
    col_real_atual = col_inicio + 4     # Realizado ano atual
    col_perc_atual = col_inicio + 5     # % Sobre Fat. ano atual
    col_var_orc = col_inicio + 6        # Variação Orç x Real
    col_var_ano = col_inicio + 7        # Variação Atual x Anterior
    col_contas = 3
    
    dados = []
    conta_pai = None
    
    def safe_float(val):
        try:
            if pd.isna(val):
                return 0.0
            return float(val)
        except:
            return 0.0
    
    for idx in range(5, len(df)):
        row = df.iloc[idx]
        conta = row[col_contas]
        
        if pd.isna(conta) or str(conta).strip() == '':
            continue
        
        conta_str = str(conta).strip()
        
        if conta_str in ['Nº de Cupons', 'Ticket Médio', 'Nº de Func.', 
                         'Crescimento da Receita (%)', 'Crescimento Ticket Médio (%)',
                         'Crescimento Clientes (%)', 'COD', '2025', '2024', '2026', '2027']:
            continue
        
        if isinstance(conta, pd.Timestamp):
            continue
        
        is_principal = is_conta_principal(conta_str)
        config = get_conta_config(conta_str)
        
        item = {
            'conta': conta_str,
            'real_anterior': safe_float(row[col_real_anterior]),
            'perc_anterior': safe_float(row[col_perc_anterior]),
            'orcamento': safe_float(row[col_orcamento]),
            'perc_orc': safe_float(row[col_perc_orc]),
            'real_atual': safe_float(row[col_real_atual]),
            'perc_atual': safe_float(row[col_perc_atual]),
            'var_orc_real': safe_float(row[col_var_orc]),
            'var_ano': safe_float(row[col_var_ano]),
            'is_principal': is_principal,
            'nivel': config['nivel'],
            'tipo': config['tipo'],
            'expansivel': config['expansivel'] and is_principal,
            'conta_pai': conta_pai if not is_principal else None
        }
        
        if is_principal:
            conta_pai = conta_str
        
        dados.append(item)
    
    indicadores = {}
    for idx in range(len(df)-15, len(df)):
        if idx < 0:
            continue
        row = df.iloc[idx]
        conta = str(row[col_contas]).strip() if pd.notna(row[col_contas]) else ''
        
        if conta == 'Nº de Cupons':
            indicadores['cupons_anterior'] = safe_float(row[col_real_anterior])
            indicadores['cupons_atual'] = safe_float(row[col_real_atual])
        elif conta == 'Ticket Médio':
            indicadores['ticket_anterior'] = safe_float(row[col_real_anterior])
            indicadores['ticket_atual'] = safe_float(row[col_real_atual])
        elif conta == 'Nº de Func.':
            indicadores['func_anterior'] = safe_float(row[col_real_anterior])
            indicadores['func_atual'] = safe_float(row[col_real_atual])
    
    receita = find_conta(dados, 'Receita líquida total')
    lucro_bruto = find_conta(dados, 'Lucro bruto')
    lucro_op = find_conta(dados, 'Lucro Operacional')
    lucro_final = find_conta(dados, 'LUCRO ANTES IMPOSTOS')
    
    # Calcular margem
    margem_bruta = (lucro_bruto.get('real_atual', 0) / receita.get('real_atual', 1)) * 100 if receita.get('real_atual', 0) != 0 else 0
    margem_op = (lucro_op.get('real_atual', 0) / receita.get('real_atual', 1)) * 100 if receita.get('real_atual', 0) != 0 else 0
    margem_liquida = (lucro_final.get('real_atual', 0) / receita.get('real_atual', 1)) * 100 if receita.get('real_atual', 0) != 0 else 0
    
    kpis = {
        'receita_atual': receita.get('real_atual', 0),
        'receita_anterior': receita.get('real_anterior', 0),
        'receita_var': receita.get('var_ano', 0),
        'lucro_bruto_atual': lucro_bruto.get('real_atual', 0),
        'lucro_bruto_var': lucro_bruto.get('var_ano', 0),
        'lucro_op_atual': lucro_op.get('real_atual', 0),
        'lucro_op_var': lucro_op.get('var_ano', 0),
        'lucro_final_atual': lucro_final.get('real_atual', 0),
        'lucro_final_var': lucro_final.get('var_orc_real', 0),
        'margem_bruta': margem_bruta,
        'margem_op': margem_op,
        'margem_liquida': margem_liquida,
    }
    
    return {
        'data_ref': data_ref,
        'ano_atual': ano_atual,
        'ano_anterior': ano_anterior,
        'loja': loja_nome if pd.notna(loja_nome) else '',
        'tipo_loja': tipo_loja,
        'col_inicio': col_inicio,
        'dados': dados,
        'indicadores': indicadores,
        'kpis': kpis,
        'lojas': lojas
    }

@app.route('/')
def index():
    # Obter parâmetros
    periodo = request.args.get('periodo')
    tipo = request.args.get('tipo', 'consolidado')
    loja_id = request.args.get('loja')
    
    # Listar períodos disponíveis
    periodos = listar_periodos()
    
    # Selecionar período padrão (mais recente)
    if not periodo and periodos['todos']:
        periodo = periodos['todos'][0]['label']
    
    # Verificar se o tipo solicitado existe para o período
    if periodo:
        periodo_info = next((p for p in periodos['todos'] if p['label'] == periodo), None)
        if periodo_info:
            if tipo == 'parcial' and not periodo_info['tem_parcial']:
                tipo = 'consolidado'
            elif tipo == 'consolidado' and not periodo_info['tem_consolidado']:
                tipo = 'parcial'
    
    # Obter arquivo
    excel_path = obter_arquivo(periodo, tipo) if periodo else None
    
    if not excel_path:
        return render_template('index.html', 
                             dre=None, 
                             lojas={'varejo': [], 'atacado': [], 'todas': []},
                             periodos=periodos,
                             periodo_selecionado=periodo,
                             tipo_selecionado=tipo,
                             loja_selecionada=None,
                             erro="Nenhum arquivo DRE encontrado. Adicione arquivos nas pastas 'consolidado' ou 'parcial'.")
    
    # Carregar DRE
    col_inicio = int(loja_id) if loja_id else None
    dre_data = carregar_dre(excel_path, col_inicio)
    
    if not dre_data:
        return render_template('index.html',
                             dre=None,
                             lojas={'varejo': [], 'atacado': [], 'todas': []},
                             periodos=periodos,
                             periodo_selecionado=periodo,
                             tipo_selecionado=tipo,
                             loja_selecionada=None,
                             erro="Erro ao carregar dados do DRE.")
    
    # Adicionar info do período ao DRE
    dre_data['periodo'] = periodo
    dre_data['tipo_arquivo'] = tipo
    
    return render_template('index.html', 
                         dre=dre_data, 
                         lojas=dre_data['lojas'],
                         periodos=periodos,
                         periodo_selecionado=periodo,
                         tipo_selecionado=tipo,
                         loja_selecionada=loja_id,
                         erro=None)

@app.route('/api/periodos')
def api_periodos():
    periodos = listar_periodos()
    return jsonify(periodos)

@app.route('/api/dre')
def api_dre():
    periodo = request.args.get('periodo')
    tipo = request.args.get('tipo', 'consolidado')
    loja_id = request.args.get('loja')
    
    excel_path = obter_arquivo(periodo, tipo)
    if not excel_path:
        return jsonify({'erro': 'Arquivo não encontrado'}), 404
    
    col_inicio = int(loja_id) if loja_id else None
    dre_data = carregar_dre(excel_path, col_inicio)
    
    if dre_data:
        dre_data['periodo'] = periodo
        dre_data['tipo_arquivo'] = tipo
    
    return jsonify(dre_data)

if __name__ == '__main__':
    # Criar diretórios se não existirem
    os.makedirs(CONSOLIDADO_DIR, exist_ok=True)
    os.makedirs(PARCIAL_DIR, exist_ok=True)
    
    app.run(debug=True, host='0.0.0.0', port=5000)

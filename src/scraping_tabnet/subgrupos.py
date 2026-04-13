"""Dicionários de apoio: nomes dos subgrupos SIGTAP e UF por prefixo IBGE."""

SUBGRUPO_NOMES: dict[str, str] = {
    "0101": "Ações coletivas/individuais em saúde",
    "0102": "Vigilância em saúde",
    "0201": "Coleta de material",
    "0202": "Diagnóstico em laboratório clínico",
    "0203": "Diagnóstico por anatomia patológica e citopatologia",
    "0204": "Diagnóstico por radiologia",
    "0205": "Diagnóstico por ultrassonografia",
    "0206": "Diagnóstico por tomografia",
    "0207": "Diagnóstico por ressonância magnética",
    "0208": "Diagnóstico por medicina nuclear in vivo",
    "0209": "Diagnóstico por endoscopia",
    "0210": "Diagnóstico por radiologia intervencionista",
    "0211": "Métodos diagnósticos em especialidades",
    "0212": "Diagnóstico e procedimentos especiais em hemoterapia",
    "0213": "Diagnóstico em vigilância epidemiológica e ambiental",
    "0214": "Diagnóstico por teste rápido",
    "0301": "Consultas / Atendimentos / Acompanhamentos",
    "0302": "Fisioterapia",
    "0303": "Tratamentos clínicos (outras especialidades)",
    "0304": "Tratamento em oncologia",
    "0305": "Tratamento em nefrologia",
    "0306": "Hemoterapia",
    "0307": "Tratamentos odontológicos",
    "0309": "Terapias especializadas",
    "0310": "Parto e nascimento",
    "0401": "Pequenas cirurgias (pele, tecido subcutâneo e mucosa)",
    "0403": "Cirurgia do sistema nervoso central e periférico",
    "0404": "Cirurgia das vias aéreas superiores, face, cabeça e pescoço",
    "0405": "Cirurgia do aparelho da visão",
    "0406": "Cirurgia do aparelho circulatório",
    "0407": "Cirurgia do aparelho digestivo e parede abdominal",
    "0408": "Cirurgia do sistema osteomuscular",
    "0409": "Cirurgia do aparelho geniturinário",
    "0410": "Cirurgia de mama",
    "0411": "Cirurgia obstétrica",
    "0412": "Cirurgia torácica",
    "0413": "Cirurgia reparadora",
    "0414": "Bucomaxilofacial",
    "0415": "Outras cirurgias",
    "0417": "Anestesiologia",
    "0418": "Cirurgia em nefrologia",
    "0501": "Coleta/exames para doação de órgãos, tecidos e células",
    "0503": "Ações relacionadas à doação de órgãos e tecidos",
    "0504": "Processamento de tecidos para transplante",
    "0505": "Transplante de órgãos, tecidos e células",
    "0506": "Acompanhamento no pré e pós-transplante",
    "0604": "Componente Especializado da Assistência Farmacêutica",
    "0701": "Órteses, próteses e materiais (não cirúrgicos)",
    "0702": "Órteses, próteses e materiais (cirúrgicos)",
    "0801": "Ações relacionadas ao estabelecimento",
    "0803": "Autorização / Regulação",
}


UF_BY_IBGE_PREFIX: dict[str, str] = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL",
    "28": "SE", "29": "BA",
    "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
    "41": "PR", "42": "SC", "43": "RS",
    "50": "MS", "51": "MT", "52": "GO", "53": "DF",
}


MESES_PT: dict[str, int] = {
    "Jan": 1, "Fev": 2, "Mar": 3, "Abr": 4, "Mai": 5, "Jun": 6,
    "Jul": 7, "Ago": 8, "Set": 9, "Out": 10, "Nov": 11, "Dez": 12,
}


def subgrupo_label(code: str) -> str:
    nome = SUBGRUPO_NOMES.get(code, "")
    return f"{code} — {nome}" if nome else code

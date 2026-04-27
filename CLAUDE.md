## Estilo de código

- Funções: 4-20 linhas. Divida se for maior.
- Arquivos: abaixo de 500 linhas. Divida por responsabilidade.
- Uma coisa por função, uma responsabilidade por módulo (SRP).
- Nomes: específicos e únicos. Evite `data`, `handler`, `Manager`.
  Prefira nomes que retornem menos de 5 hits no grep do codebase.
- Tipos: explícitos. Sem `any`, sem `Dict`, sem funções sem tipo.
- Sem duplicação de código. Extraia lógica compartilhada para uma função/módulo.
- Early returns ao invés de ifs aninhados. Máximo 2 níveis de indentação.
- Mensagens de exceção devem incluir o valor problemático e o formato esperado.

## Comentários

- Mantenha seus próprios comentários. Não os remova em refatorações — eles carregam
  intenção e proveniência.
- Escreva o PORQUÊ, não o O QUÊ. Pule `// incrementa contador` acima de `i++`.
- Docstrings em funções públicas: intenção + um exemplo de uso.
- Referencie números de issue / SHAs de commit quando uma linha existe por causa
  de um bug específico ou restrição externa.

## Testes

- Testes rodam com um único comando: `<específico-do-projeto>`.
- Toda função nova ganha um teste. Correções de bug ganham um teste de regressão.
- Mock de I/O externo (API, DB, sistema de arquivos) com classes fake nomeadas,
  não stubs inline.
- Testes devem ser F.I.R.S.T: rápidos, independentes, repetíveis,
  auto-validáveis, oportunos.

## Dependências

- Injete dependências via construtor/parâmetro, não global/import.
- Encapsule libs de terceiros atrás de uma interface fina pertencente a este projeto.

## Estrutura

- Siga a convenção do framework (Airflow, dbt, docker, fastapi).
- Prefira módulos pequenos e focados a god files.
- Caminhos previsíveis: controller/model/view, src/lib/test, etc.

## Formatação

- Use o formatador padrão da linguagem (`cargo fmt`, `gofmt`, `prettier`,
  `black`, `rubocop -A`). Não discuta estilo além disso.

## Logging

- JSON estruturado quando logar para debug / observabilidade.
- Texto puro apenas para output de CLI voltado ao usuário.

# MVP de Engenharia de Dados — produtividade no chão de fábrica

> **Estado em 22/09/2026:** pipeline executado no Databricks Free Edition; arquivo no volume e cinco tabelas Delta confirmadas no catálogo. Os resultados abaixo foram conferidos nas capturas e no notebook HTML exportado com saídas. Faltam a publicação no GitHub, a incorporação das evidências ao repositório e a revisão da autora.

## Contexto de Negócio e Perguntas (Etapas 2 e 4.1)

Em uma fábrica de confecção, equipes possuem metas de produtividade e registros de desempenho diário. O gestor precisa comparar resultados entre setores e equipes, identificar onde as metas não são atingidas e examinar o comportamento dos registros operacionais antes de decidir o que investigar no processo.

**Objetivo:** construir no Databricks Free Edition um pipeline reproduzível que ingira, valide e organize esses registros para análises da produtividade realizada frente à meta.

**Perguntas de negócio originais:**

1. Qual proporção de registros atinge ou supera a meta de produtividade em cada setor?
2. Como a produtividade realizada e o cumprimento da meta variam entre equipes e ao longo das datas disponíveis?
3. Como se distribuem horas extras, tempo de inatividade e trabalho em andamento nos registros que atingem e não atingem a meta? Esta comparação é descritiva e não demonstra causalidade.

**Unidade de análise:** registro de uma equipe em um setor e uma data. A combinação `(date, department, team)` é única no arquivo inspecionado, mas deve voltar a ser validada na carga.

### Fonte e licença

- Fonte primária: [UCI Machine Learning Repository — Productivity Prediction of Garment Employees](https://archive.ics.uci.edu/dataset/597/productivity+prediction+of+garment+employees), DOI [10.24432/C51S6D](https://doi.org/10.24432/C51S6D).
- Arquivo: `garments_worker_productivity.csv`, distribuído na página da UCI.
- Licença declarada pela UCI: **Creative Commons Attribution 4.0 International (CC BY 4.0)**; atribuir a fonte e os autores nas publicações e no GitHub.
- Os dados foram coletados manualmente e validados por especialistas do setor, segundo a descrição da UCI. Não são dados da empregadora da aluna.
- Os dados não descrevem cervejarias. A experiência em operação industrial orienta a interpretação das métricas, sem atribuir à base uma origem cervejeira.

**Estrutura bruta:** 1.197 linhas, 15 colunas, datas entre 01/01/2015 e 11/03/2015. Os campos são `date`, `quarter`, `department`, `day`, `team`, `targeted_productivity`, `smv`, `wip`, `over_time`, `incentive`, `idle_time`, `idle_men`, `no_of_style_change`, `no_of_workers`, `actual_productivity`. O campo `quarter` representa uma fração do mês na documentação da origem; não deve ser interpretado como trimestre do ano.

## Carga dos Dados (Etapa 4.2)

O CSV da UCI foi enviado manualmente a `/Volumes/workspace/mvp_produtividade/entrada_uci/garments_worker_productivity.csv` e inspecionado no catálogo. O notebook `01_pipeline_produtividade.py` foi executado no Databricks Free Edition. A tabela Delta `workspace.mvp_produtividade.bronze_produtividade` recebeu as **1.197 linhas**; os 15 campos originais foram lidos inicialmente como texto, com `source_file` e `ingested_at` adicionados. O arquivo original permanece no volume. Registrar na versão final a data de obtenção do CSV e anexar as capturas ao repositório.

O Google Colab pode ser usado para explorar os exercícios do curso, mas **não** será o ambiente de execução deste MVP: o edital exige plataforma de nuvem e invalida entregas feitas no Colab.

## Modelagem e Catálogo de Dados (Etapa 4.3)

| Tabela criada em `workspace.mvp_produtividade` | Grão | Origem e função | Linhas verificadas |
|---|---|---|---:|
| `bronze_produtividade` | Linha do CSV original | Preservar valores recebidos da UCI e metadados de ingestão. | 1.197 |
| `silver_produtividade` | Data × setor × equipe | Converter tipos, padronizar setor, preservar nulos de `wip` e criar verificações documentadas. | 1.197 |
| `gold_produtividade_setor` | Setor | Total de registros, registros que atingem a meta, percentual e médias descritivas. | 2 |
| `gold_produtividade_dia_setor` | Data × setor | Mesmas métricas por data e setor para acompanhar a variação observada. | 118 |
| `gold_produtividade_equipe` | Setor × equipe | Métricas de cumprimento de meta e indicadores operacionais por equipe. | 24 |

O modelo é composto por uma tabela detalhada limpa e tabelas agregadas de consumo analítico; o edital aceita modelos *flat* por conceito. Não chamar uma agregação de tabela dimensão ou inventar relacionamentos que não existem na fonte.

### Dicionário de dados e linhagem

Todos os 15 campos da fonte UCI entram na Bronze como `STRING`. A Silver aplica os tipos e regras abaixo. A ausência da definição de unidade na fonte é registrada explicitamente, sem presumir horas, segundos ou moeda. A documentação de origem está na [página UCI](https://archive.ics.uci.edu/dataset/597/productivity+prediction+of+garment+employees).

| Coluna da fonte → Silver | Tipo na Silver | Significado, domínio e regra |
|---|---|---|
| `date` → `data_registro` | DATE | Data informada como mês/dia/ano; período observado de 01/01 a 11/03/2015; conferir com `day`. |
| `quarter` → `quarter` | STRING | Parte do mês: `Quarter1` a `Quarter5` observados. A UCI descreve divisão em quatro partes, mas há `Quarter5` no arquivo; não representar como trimestre civil. |
| `department` → `setor_original`, `setor` | STRING, STRING | Área da fábrica; valores originais `sweing`, `finishing`, `finishing `; retirar espaços, corrigir `sweing` para `sewing` e preservar a grafia inicial. |
| `day` → `day` | STRING | Dia da semana; seis categorias observadas; conferir com `data_registro`. |
| `team` → `equipe` | INT | Identificador de equipe; 12 valores distintos observados. |
| `targeted_productivity` → mesmo nome | DOUBLE | Meta por equipe e dia; intervalo observado de 0,07 a 0,8. |
| `smv` → mesmo nome | DOUBLE | *Standard Minute Value*, tempo padrão alocado à tarefa; observado de 2,9 a 54,56. |
| `wip` → mesmo nome | DOUBLE | Itens inacabados em andamento; 506 valores ausentes, todos em `finishing`; observado de 7 a 2.312 quando informado. |
| `over_time` → mesmo nome | DOUBLE | Tempo extra da equipe em **minutos**; observado de 0 a 25.920. |
| `incentive` → mesmo nome | DOUBLE | Incentivo financeiro em **BDT**; observado de 0 a 3.600. |
| `idle_time` → mesmo nome | DOUBLE | Tempo de interrupção da produção; **unidade não indicada pela UCI**; observado de 0 a 300. |
| `idle_men` → mesmo nome | DOUBLE | Pessoas ociosas durante interrupção; observado de 0 a 45. |
| `no_of_style_change` → mesmo nome | DOUBLE | Número de mudanças de estilo do produto; observado de 0 a 2. |
| `no_of_workers` → mesmo nome | DOUBLE | Tamanho da equipe; de 2 a 89, incluindo frações na origem. |
| `actual_productivity` → mesmo nome | DOUBLE | Produtividade realizada; de 0,233705476 a 1,1204375 observados, embora a UCI descreva faixa 0–1. |

A Bronze acrescenta `source_file` (STRING, nome do CSV) e `ingested_at` (TIMESTAMP, instante de ingestão). A Silver acrescenta `atingiu_meta` (BOOLEAN, `actual_productivity >= targeted_productivity`), `produtividade_acima_de_um` (BOOLEAN, limite documental de 1) e `dia_confere` (BOOLEAN, validação de dia da semana). Mantém `source_file`, `quarter`, `day` e os demais campos não renomeados. Esses indicadores são criados no notebook a partir da Bronze; a Gold deriva **exclusivamente** da Silver.

As três tabelas Gold usam `setor` (STRING), com `data_registro` (DATE) apenas na tabela diária e `equipe` (INT) apenas na tabela por equipe. Suas medidas são: `registros` (BIGINT, contagem de linhas), `metas_atingidas` (BIGINT, soma do indicador), `percentual_metas_atingidas` (DOUBLE, percentual de linhas do grupo que atingem a meta), `meta_media` e `produtividade_media` (DOUBLE, médias), `minutos_extras_medios` (DOUBLE, média de `over_time` em minutos), `inatividade_media` (DOUBLE, média de `idle_time`, unidade original não especificada), `registros_wip_informado` (BIGINT, número de observações não nulas) e `wip_medio_informado` (DOUBLE, média apenas dos valores não nulos). Os tipos Gold apresentados seguem as operações Spark do notebook; conferir o esquema exibido no catálogo antes da entrega final.

## Pipeline de Dados (Etapa 4.4)

**Fluxo executado:** CSV original → Bronze (cópia lógica fiel) → perfil de qualidade inicial → Silver (tipagem, normalização e regras explícitas) → Gold (agregações) → consultas e interpretações. A captura do catálogo após atualização confirmou as cinco tabelas persistentes e um volume.

Regras implementadas e justificadas:

1. Interpretar `date` em formato mês/dia/ano, conferir compatibilidade com `day` e persistir como data.
2. Normalizar os espaços em `department` e registrar os valores originais. Corrigir `sweing` para `sewing` somente com documentação explícita da correção de grafia.
3. Converter medidas numéricas para tipos apropriados, mantendo `no_of_workers` como decimal, pois há frações no arquivo.
4. Criar `atingiu_meta = actual_productivity >= targeted_productivity`, sem alterar os campos originais.
5. **Manter `wip` nulo como desconhecido ou não informado**. A ausência ocorre nos registros do setor `finishing`; não transformá-la automaticamente em zero.
6. Sinalizar valores de `actual_productivity` acima de 1 para revisão. A descrição da UCI cita intervalo de 0 a 1, mas 37 linhas observadas ultrapassam 1. Não truncar nem descartar sem justificativa.

Para reexecução segura, usar criação/substituição determinística das tabelas de resultado, registrar contagens por etapa e confirmar que as agregações reproduzem as contagens da Silver. Referenciar no README final o(s) notebook(s) efetivamente publicados no GitHub, com prints das tabelas persistidas.

## Qualidade de Dados (Etapa 4.5)

**Perfil inicial do CSV, executado localmente apenas para definir o projeto:**

| Checagem | Resultado observado | Tratamento planejado |
|---|---:|---|
| Linhas e colunas | 1.197 × 15 | Reconciliar Bronze e Silver. |
| `wip` vazio | 506 linhas, todas em `finishing` ou `finishing ` | Preservar nulo; comparar com cuidado. |
| Variações em `department` | `sweing` (691), `finishing` (249), `finishing ` (257) | Remover espaço final e documentar correção de grafia. |
| `actual_productivity > 1` | 37 linhas | Sinalizar e verificar impacto nas análises. |
| Duplicação integral de linha | 0 | Revalidar na nuvem. |
| Chave `(date, department, team)` duplicada | 0 | Revalidar antes e depois da limpeza. |
| Divergência entre `date` e `day` | 0 | Revalidar após conversão de data. |

**Verificação na plataforma:** o perfil de todas as 15 colunas mostrou somente `wip` com ausências (506). A Bronze não teve linhas integralmente duplicadas. A Silver manteve 1.197 linhas; as verificações de data, dia da semana, campos obrigatórios e unicidade de `(data_registro, setor, equipe)` terminaram sem falhas. Os 37 registros com produtividade realizada acima de 1 foram sinalizados e mantidos. `wip` tem 691 registros informados, todos em `sewing`; não há média de `wip` para `finishing`. Na fonte, produtividade realizada variou de 0,233705476 a 1,1204375, e o objetivo de produtividade de 0,07 a 0,8. Capturas da execução estão disponíveis para inclusão na entrega.

## Análise de Dados (Etapa 4.5)

**Pergunta 1.** Em `finishing`, 302 de 506 registros atingiram a meta (**59,68%**); em `sewing`, 573 de 691 (**82,92%**). A diferença foi de **23,24 pontos percentuais**, favorável a `sewing`. A produtividade média realizada foi 0,753 em `finishing` e 0,722 em `sewing`; a comparação de médias não substitui a avaliação por meta de cada registro.

**Pergunta 2.** A tabela diária por setor gerou 118 combinações de data e setor, e a tabela por equipe gerou 24 combinações de setor e equipe. Nas linhas exibidas, há variação diária e entre equipes; por exemplo, a equipe 6 de `finishing` atingiu 11 metas em 35 registros (**31,43%**), e a equipe 1 de `sewing`, 53 em 56 (**94,64%**). São exemplos de grupos específicos, não estimativas do período inteiro para cada setor.

**Pergunta 3.** Em `sewing`, os registros que não atingiram a meta (118) tiveram tempo médio de inatividade de **5,31**, ante **0,43** nos que atingiram (573); a fonte deve ser consultada para a unidade e definição de `idle_time`. A média de `wip` foi 845,58 entre os 118 registros sem meta atingida e 1.261,49 entre os 573 com meta atingida. Em `finishing`, os 506 registros não informam `wip`; logo, não é possível comparar o indicador entre os dois setores. Os indicadores de `over_time` estão em minutos segundo a fonte. Essas relações são descritivas e **não comprovam causas**.

As capturas das consultas precisam ser anexadas ao repositório e referenciadas aqui antes da submissão.

## Autoavaliação

**Objetivos atingidos:** ingestão de dados reais em armazenamento de nuvem; cinco tabelas Delta Bronze/Silver/Gold acessíveis pelo catálogo; 1.197 registros reconciliados nas camadas detalhadas; verificações por coluna, regras de conversão e três consultas de negócio executadas. A captura de tela do catálogo inicialmente mostrava zero tabelas por atualização pendente; após atualizar a interface, as cinco apareceram. O notebook exportado em HTML contém as saídas das sete células de código sem erros.

**Objetivos parciais e limites:** a pergunta sobre `wip` só pode ser respondida dentro de `sewing`, pois nenhum registro de `finishing` informa o campo. As diferenças observadas entre setores e equipes e as relações com indicadores operacionais não demonstram causalidade. `quarter` inclui `Quarter5`, apesar da descrição resumida de quatro partes na UCI. Há 37 produtividades acima de 1 sem explicação definitiva na documentação; foram preservadas e sinalizadas. Não há dados de cervejaria, nem fluxo contínuo de novos registros: este é um MVP com carga reproduzível do CSV estático.

**Dificuldades e lições:** a Free Edition levou à ingestão por upload no volume, evitando dependência de download externo na execução. A necessidade de atualizar a visualização do catálogo mostrou por que a verificação de persistência exige consultar as tabelas e registrar uma captura atual. Separar dados brutos, dados tratados e indicadores facilitou rastrear as decisões de qualidade.

**Antes da submissão:** inserir as capturas selecionadas e referências no repositório, validar os tipos exibidos no catálogo e publicar o código/README em GitHub público. Como extensão futura, testar novas cargas incrementais e investigar as exceções de produtividade com conhecimento adicional do processo. Esta autoavaliação deve ser revista pela autora antes da entrega.

## Evidências obrigatórias para a entrega

- Código completo em repositório **público** do GitHub, trabalho individual.
- README ou PDF com os sete títulos exigidos pelo edital, catálogo completo e fonte/licença.
- Capturas de etapas feitas pela interface, do catálogo, das tabelas persistidas e dos resultados das consultas.
- Execução efetiva em plataforma na nuvem, preferencialmente Databricks Free Edition.
- Conferência de que não há áudio ou vídeo como substituto das evidências escritas.

**Próximos passos para entrega:** incorporar as capturas existentes em `evidencias/` e o HTML exportado, conferir tipos no catálogo, revisar este texto pela autora, publicar código e relatório em GitHub público e conferir o endereço final. Não declarar a entrega concluída antes dessas verificações.

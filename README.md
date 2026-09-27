# MVP de Engenharia de Dados — produtividade de equipes na confecção

Projeto individual desenvolvido no Databricks Free Edition com o conjunto público [Productivity Prediction of Garment Employees](https://archive.ics.uci.edu/dataset/597/productivity+prediction+of+garment+employees), disponibilizado pela UCI Machine Learning Repository sob licença [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

O objetivo é analisar o cumprimento de metas de produtividade por setor, data e equipe. O conjunto contém 1.197 registros de equipes da indústria de confecção; não contém dados de uma cervejaria. As análises são descritivas e consideram as limitações e os valores ausentes da fonte.

## Arquivos da entrega

- [Relatório do MVP (PDF)](MVP_Engenharia_Dados_Produtividade.pdf): contexto e perguntas de negócio, carga, modelagem e catálogo, pipeline, qualidade, análise, capturas da execução e autoavaliação.
- [Pipeline Databricks (Python)](01_pipeline_produtividade.py): código do notebook que lê o CSV no volume, constrói as camadas Bronze, Silver e Gold e executa as verificações e consultas.

## Pipeline e resultados

O CSV foi enviado para um volume do Databricks e lido pelo notebook. A Bronze preserva os valores recebidos; a Silver converte tipos e padroniza campos; três tabelas Gold reúnem indicadores por setor, por data e setor, e por setor e equipe. A Bronze e a Silver possuem 1.197 registros; as tabelas Gold possuem, respectivamente, 2, 118 e 24 registros. As evidências da execução e a interpretação dos resultados constam no relatório.

Para reproduzir a carga, obtenha o CSV na página da UCI, envie-o ao volume indicado no notebook e execute as células em ordem no Databricks. O arquivo de dados não está incluído neste repositório.

**Fonte dos dados:** UCI Machine Learning Repository, *Productivity Prediction of Garment Employees*, DOI [10.24432/C51S6D](https://doi.org/10.24432/C51S6D). Licença dos dados: Creative Commons Attribution 4.0 International (CC BY 4.0).

# case_join
# Documentação do Projeto: Case Join

## Descrição do Projeto

Este projeto tem como objetivo realizar consultas SQL em um conjunto de dados relacionados a vendas e clientes, utilizando o ambiente Databricks. As consultas visam responder a três perguntas específicas sobre os dados, com foco em faturamento, cancelamentos de pedidos e informações sobre vendedores.

## Estrutura do Projeto

O projeto consiste em um script Python que contém todas as consultas SQL necessárias para obter as informações desejadas. Os resultados das consultas são salvos em formato Delta conforme solicitado.

### Consultas Executadas

1. &&&Qual país possui a maior quantidade de itens cancelados?&&&
   - Esta consulta agrega o número total de itens cancelados por país.

2. &&&Qual o faturamento da linha de produto mais vendido, considerando os pedidos com status 'Shipped', cujo pedido foi realizado no ano de 2005?&&&
   - Esta consulta calcula o faturamento total por linha de produto, filtrando pelos pedidos que foram enviados em 2005.

3. &&&Nome, sobrenome e e-mail dos vendedores do Japão, com o local-part do e-mail mascarado.&&&
   - Esta consulta retorna informações dos vendedores localizados no Japão, com os endereços de e-mail parcialmente ocultos por motivos de privacidade, ou seja, xxxxxxx@dominio_do_email.com.

## Tecnologias Utilizadas

- &&&Databricks&&&: Ambiente de análise de dados e processamento em nuvem.
- &&&Apache Spark&&&: Framework para processamento de dados em larga escala.
- &&&Delta Lake&&&: Formato de armazenamento que permite gerenciamento de dados em grande escala.

## Configuração do Ambiente

Para executar este projeto, você precisará de um ambiente Databricks configurado com acesso ao cluster do Spark.

### Passos para Execução

1. &&&Importar os Dados&&&: Certifique-se de que todos os dados relevantes estão disponíveis nas tabelas `orders`, `orderdetails`, `customers`, `products`, `product_lines`, `employees` e `offices` no seu ambiente Databricks.

2. &&&Carregar o Script&&&: O script Python `querys.py` deve ser carregado no seu workspace do Databricks.

3. &&&Executar o Script&&&: Execute o script para realizar as consultas e salvar os resultados em formato Delta.

4. &&&Verificar os Resultados&&&: Os resultados das consultas são salvos nos seguintes caminhos:
   - Itens cancelados por país: `/mnt/delta/itens_cancelados_pais`
   - Faturamento da linha de produto: `/mnt/delta/faturamento_linha_produto`
   - Vendedores do Japão: `/mnt/delta/vendedores_japao`

## Resultados

Os resultados podem ser visualizados diretamente no formato Delta, utilizando ferramentas de análise de dados ou através de novas consultas SQL no Databricks.




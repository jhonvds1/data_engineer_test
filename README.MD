# Guia de Execução do Projeto

Abaixo estão as instruções detalhadas para configuração e execução do ambiente.

---

## 1. Clonar o Repositório

Para iniciar, clone o repositório localmente utilizando o Git:

```bash
git clone https://github.com/jhonvds1/data_engineer_test.git
```
Em seguida, acesse a pasta do projeto:
```
cd data_engineer_test
```

## 2. Instalar Dependências

Instale todas as bibliotecas necessárias listadas no arquivo requirements.txt:

```
pip install -r requirements.txt
```

## 3. Inicializar o Docker

O projeto utiliza Docker para gerenciar dependências e serviços. Para iniciar os containers, execute:

```
docker compose up
```

## 4. Executar a Aplicação

Após a inicialização do Docker, execute o arquivo principal do projeto:

```
python -m src.main
```

## 5. Observações Importantes

- O arquivo `.env` já está incluído no repositório para simplificação.
- O pipeline **remove vendas que contêm pelo menos um produto com quantidade menor que a quantidade mínima** configurada no produto.
- Para **ignorar essa validação**, comente a linha abaixo no arquivo `src/transform/transform_carts.py`:

```python
# Ignorar validação de quantidade mínima
# data_carts = remove_invalid_orders(data_carts)
```

Essa alteração fará com que todas as vendas sejam mantidas, mesmo que algum produto esteja abaixo da quantidade mínima.

- Foram feitas **mais tratativas de dados do que o necessário**, mas isso foi **totalmente proposital**, com o objetivo de demonstrar conhecimento em processamento de dados.

# Trabalho de Sensores — Processamento Paralelo com Pthreads

Programa em C que processa em paralelo dois arquivos JSON contendo leituras de sensores meteorologicos das cidades de Caxias do Sul e Bento Goncalves, remove duplicatas por timestamp, calcula estatisticas de temperatura, umidade, pressao e bateria, e gera um log de auditoria.

## Autores
- Luca Giasson Tedesco
- Jhonatan Martins

Disciplina: Fundamentos de Sistemas Operacionais
Professor: Samuel Francisco Ferrigo
Instituicao: Universidade de Caxias do Sul — 2026

## Video de apresentacao
[Cole aqui o link do YouTube nao listado depois de subir]

## Requisitos
- Linux (Ubuntu, Debian, ou similar) ou WSL no Windows
- GCC instalado, com suporte a pthreads
- Os dois arquivos JSON na mesma pasta do executavel:
  - `mqtt_senzemo_cx_bg.json`
  - `senzemo_cx_bg.json`

Para instalar o GCC no Ubuntu, caso necessario:

    sudo apt update
    sudo apt install build-essential

## Preparacao

O arquivo `senzemo_cx_bg.json` foi compactado em `senzemo_cx_bg.zip` por causa do limite de tamanho do GitHub (25 MB por arquivo no upload). Antes de compilar, **descompacte o ZIP na mesma pasta** para que o JSON original fique disponivel para o programa.

No Linux, pelo terminal:

    unzip senzemo_cx_bg.zip

Ou clicando com o botao direito sobre o arquivo no gerenciador de arquivos e escolhendo "Extrair aqui".

Apos a extracao, a pasta deve conter os dois JSONs:
- `mqtt_senzemo_cx_bg.json`
- `senzemo_cx_bg.json`

## Compilacao

Dentro da pasta do projeto, no terminal:

    gcc sensores.c cJSON.c -o sensores -lpthread

## Execucao

    ./sensores

A saida do relatorio aparece no terminal. O arquivo `processamento.log` e gerado automaticamente na mesma pasta.

## Arquitetura

O programa utiliza 4 threads sincronizadas com `pthread_mutex` e `pthread_cond_t`:

- **Thread 1** — le e deserializa `mqtt_senzemo_cx_bg.json`, removendo duplicatas por timestamp.
- **Thread 2** — le e deserializa `senzemo_cx_bg.json`, removendo duplicatas por timestamp.
- **Thread 3** — apos a leitura terminar, percorre os dados limpos e calcula minimas, maximas, medias de temperatura, umidade, pressao, consumo de bateria e Spreading Factors LoRa utilizados.
- **Thread 4** — logger de auditoria que recebe eventos das outras threads por variavel de condicao e grava em `processamento.log` em tempo real.

## Estrutura do projeto

    .
    ├── sensores.c                  Codigo principal com as 4 threads
    ├── cJSON.c                     Biblioteca de parsing JSON
    ├── cJSON.h                     Header da biblioteca cJSON
    ├── mqtt_senzemo_cx_bg.json     Dados de entrada (arquivo 1)
    ├── senzemo_cx_bg.json          Dados de entrada (arquivo 2)
    └── README.md

## Saida esperada

- Relatorio formatado no terminal com:
  - Total de registros processados e duplicatas descartadas por arquivo
  - Periodo analisado
  - Temperatura, umidade e pressao (minima, maxima com data/hora, e media) por cidade
  - Bateria inicial, final e consumo
  - Spreading Factors LoRa utilizados
  - Tempo total de execucao
- Arquivo `processamento.log` com auditoria completa por thread.

## Ambiente testado

Maquina virtual Linux com 8 GB de RAM e 8 processadores.

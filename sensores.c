#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>
#include <unistd.h>
#include "cJSON.h" 

// --- ESTRUTURAS DE DADOS ---
typedef struct {
    double min;
    double max;
    double soma;
    int contagem;
    char time_min[40];
    char time_max[40];
} Metrica;

typedef struct {
    char time[40];
    double temperature;
    double humidity;
    double airpressure;
    double batterylevel;
    int lora_spreading_factor;
} LeituraSensor;

typedef struct {
    LeituraSensor *leituras;  
    int capacidade;
    int total_registros;
    int registros_removidos;
    char ultimo_timestamp[40];
    
    Metrica temp;
    Metrica umi;
    Metrica pressao;
    double bateria_inicial;
    double bateria_final;
    int sf_utilizados[15];
    char data_inicio[40];
    char data_fim[40];
} CidadeData;

typedef struct {
    char nome_arquivo[100];
    int registros_processados;
    int registros_removidos;
    char data_inicio[40];
    char data_fim[40];
    int thread_id; // <-- NOVA VARIÁVEL PARA IDENTIFICAR A THREAD NO LOG
} ArquivoInfo;

// Variáveis Globais
CidadeData caxias;
CidadeData bento;

// Sincronização
pthread_mutex_t dados_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t log_cond = PTHREAD_COND_INITIALIZER;
char log_buffer[1024] = "";
int log_ready = 0;
int processamento_concluido = 0;

// --- INICIALIZAÇÃO ---
void inicializar_cidade(CidadeData *c) {
    c->leituras = NULL; c->capacidade = 0; c->total_registros = 0; c->registros_removidos = 0;
    c->temp.min = 9999.0; c->temp.max = -9999.0; c->temp.soma = 0; c->temp.contagem = 0;
    c->umi.min = 9999.0; c->umi.max = -9999.0; c->umi.soma = 0; c->umi.contagem = 0;
    c->pressao.min = 9999.0; c->pressao.max = -9999.0; c->pressao.soma = 0; c->pressao.contagem = 0;
    c->bateria_inicial = -1.0; c->bateria_final = -1.0;
    strcpy(c->data_inicio, "9999"); 
    strcpy(c->data_fim, "0000");    
    strcpy(c->ultimo_timestamp, "");
    for(int i=0; i<15; i++) c->sf_utilizados[i] = 0;
}

// --- THREAD DE LOG (Thread 4) ---
void enviar_log(const char* mensagem) {
    pthread_mutex_lock(&log_mutex);
    strncpy(log_buffer, mensagem, sizeof(log_buffer) - 1);
    log_ready = 1;
    pthread_cond_signal(&log_cond);
    pthread_mutex_unlock(&log_mutex);
    usleep(1000); 
}

void* thread_logger(void* arg) {
    FILE *f = fopen("processamento.log", "w");
    if (!f) pthread_exit(NULL);
    while (1) {
        pthread_mutex_lock(&log_mutex);
        while (log_ready == 0 && !processamento_concluido) 
            pthread_cond_wait(&log_cond, &log_mutex);
        if (log_ready) {
            fprintf(f, "%s\n", log_buffer); 
            fflush(f);
            log_ready = 0;
        }
        if (processamento_concluido && !log_ready) {
            pthread_mutex_unlock(&log_mutex);
            break;
        }
        pthread_mutex_unlock(&log_mutex);
    }
    fclose(f);
    pthread_exit(NULL);
}

// --- FUNÇÕES AUXILIARES ---
void adicionar_leitura(CidadeData *cidade, LeituraSensor l) {
    pthread_mutex_lock(&dados_mutex);
    if (cidade->total_registros >= cidade->capacidade) {
        cidade->capacidade = (cidade->capacidade == 0) ? 1000 : cidade->capacidade * 2;
        cidade->leituras = realloc(cidade->leituras, cidade->capacidade * sizeof(LeituraSensor));
    }
    cidade->leituras[cidade->total_registros++] = l;
    pthread_mutex_unlock(&dados_mutex);
}

void atualizar_metrica(Metrica *m, double valor, const char *tempo) {
    if (valor < m->min) { m->min = valor; strncpy(m->time_min, tempo, 39); }
    if (valor > m->max) { m->max = valor; strncpy(m->time_max, tempo, 39); }
    m->soma += valor;
    m->contagem++;
}

void processar_periodo(CidadeData *c, const char *tempo) {
    if (strcmp(tempo, c->data_inicio) < 0) strncpy(c->data_inicio, tempo, 39);
    if (strcmp(tempo, c->data_fim) > 0) strncpy(c->data_fim, tempo, 39);
}

// --- THREAD DE LEITURA E DEDUPLICAÇÃO (Threads 1 e 2) ---
void* thread_leitura(void* arg) {
    ArquivoInfo *info = (ArquivoInfo*)arg;
    char msg[256];
    
    snprintf(msg, sizeof(msg), "[Thread %d] Iniciando acesso ao disco e leitura do arquivo: %s", info->thread_id, info->nome_arquivo);
    enviar_log(msg);
    
    FILE *f = fopen(info->nome_arquivo, "r");
    if (!f) {
        snprintf(msg, sizeof(msg), "[Thread %d] Erro critico: Arquivo %s nao encontrado.", info->thread_id, info->nome_arquivo);
        enviar_log(msg);
        pthread_exit(NULL);
    }

    fseek(f, 0, SEEK_END);
    long length = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *data = malloc(length + 1);
    fread(data, 1, length, f);
    fclose(f);
    data[length] = '\0';

    snprintf(msg, sizeof(msg), "[Thread %d] Parsing do JSON iniciado na memoria RAM...", info->thread_id);
    enviar_log(msg);

    cJSON *json_array = cJSON_Parse(data);
    free(data);

    if (json_array) {
        cJSON *item;
        cJSON_ArrayForEach(item, json_array) {
            cJSON *conteudo_str = cJSON_GetObjectItemCaseSensitive(item, "payload");
            if (!conteudo_str) conteudo_str = cJSON_GetObjectItemCaseSensitive(item, "brute_data");
            
            if (conteudo_str && cJSON_IsString(conteudo_str)) {
                cJSON *inner = cJSON_Parse(conteudo_str->valuestring);
                if (!inner) continue;

                cJSON *device = cJSON_GetObjectItemCaseSensitive(inner, "device_name");
                cJSON *vars = cJSON_GetObjectItemCaseSensitive(inner, "data");

                if (device && vars) {
                    CidadeData *cidade = strstr(device->valuestring, "Caxias") ? &caxias : &bento;
                    const char *nome_cidade = strstr(device->valuestring, "Caxias") ? "Caxias do Sul" : "Bento Goncalves";
                    
                    pthread_mutex_lock(&dados_mutex);
                    cJSON *primeira_var = cJSON_GetArrayItem(vars, 0);
                    cJSON *t_item = cJSON_GetObjectItemCaseSensitive(primeira_var, "time");
                    
                    if (t_item && t_item->valuestring) {
                        const char* t_atual = t_item->valuestring;
                        
                        // LÓGICA DE DEDUPLICAÇÃO COM LOG DETALHADO
                        if (strcmp(cidade->ultimo_timestamp, t_atual) == 0) {
                            cidade->registros_removidos++;
                            info->registros_removidos++; 
                            pthread_mutex_unlock(&dados_mutex);
                            
                            // Loga o momento exato em que removeu a duplicata
                            snprintf(msg, sizeof(msg), "[Thread %d] Duplicata descartada -> %s | Timestamp: %s", info->thread_id, nome_cidade, t_atual);
                            enviar_log(msg);
                            
                            cJSON_Delete(inner);
                            continue; 
                        }
                        strncpy(cidade->ultimo_timestamp, t_atual, 39);
                        pthread_mutex_unlock(&dados_mutex);

                        // SUCESSO: Registro válido
                        info->registros_processados++; 
                        
                        if (info->registros_processados % 5000 == 0) {
                            snprintf(msg, sizeof(msg), "[Thread %d] Varredura em andamento: %d registros limpos salvos na RAM...", info->thread_id, info->registros_processados);
                            enviar_log(msg);
                        }

                        if (strcmp(t_atual, info->data_inicio) < 0) strncpy(info->data_inicio, t_atual, 39);
                        if (strcmp(t_atual, info->data_fim) > 0) strncpy(info->data_fim, t_atual, 39);

                        LeituraSensor l_nova;
                        memset(&l_nova, 0, sizeof(LeituraSensor));
                        l_nova.temperature = -9999.0;
                        l_nova.humidity = -9999.0;
                        l_nova.airpressure = -9999.0;
                        l_nova.batterylevel = -1.0; 
                        l_nova.lora_spreading_factor = -1;
                        strncpy(l_nova.time, t_atual, 39);

                        cJSON *var;
                        cJSON_ArrayForEach(var, vars) {
                            cJSON *v_name = cJSON_GetObjectItemCaseSensitive(var, "variable");
                            cJSON *v_val = cJSON_GetObjectItemCaseSensitive(var, "value");

                            if (v_name && v_val) {
                                if (strcmp(v_name->valuestring, "temperature") == 0) l_nova.temperature = v_val->valuedouble;
                                else if (strcmp(v_name->valuestring, "humidity") == 0) l_nova.humidity = v_val->valuedouble;
                                else if (strcmp(v_name->valuestring, "airpressure") == 0) l_nova.airpressure = v_val->valuedouble;
                                else if (strcmp(v_name->valuestring, "batterylevel") == 0) l_nova.batterylevel = v_val->valuedouble;
                                else if (strcmp(v_name->valuestring, "lora_spreading_factor") == 0) l_nova.lora_spreading_factor = (int)v_val->valuedouble;
                            }
                        }
                        adicionar_leitura(cidade, l_nova);
                    } else {
                        pthread_mutex_unlock(&dados_mutex);
                    }
                }
                cJSON_Delete(inner);
            }
        }
        cJSON_Delete(json_array);
    }
    
    snprintf(msg, sizeof(msg), "[Thread %d] Finalizou %s. Extraidos %d registros e descartadas %d duplicatas.", info->thread_id, info->nome_arquivo, info->registros_processados, info->registros_removidos);
    enviar_log(msg);
    pthread_exit(NULL);
}

// --- THREAD DE CÁLCULO DAS ESTATÍSTICAS (Thread 3) ---
void* thread_calculos(void* arg) {
    enviar_log("[Thread 3] Despertada. Iniciando leitura dos dados limpos na memoria RAM para calculo de estatisticas.");
    
    CidadeData *cidades[] = {&caxias, &bento};
    char msg[256];
    
    for (int i = 0; i < 2; i++) {
        CidadeData *c = cidades[i];
        const char *nome_cidade = (i == 0) ? "Caxias do Sul" : "Bento Goncalves";
        
        snprintf(msg, sizeof(msg), "[Thread 3] Iterando array de %s para encontrar minimas, maximas e preparar calculo de medias...", nome_cidade);
        enviar_log(msg);
        
        for (int j = 0; j < c->total_registros; j++) {
            LeituraSensor l = c->leituras[j];
            
            if (l.temperature != -9999.0) atualizar_metrica(&c->temp, l.temperature, l.time);
            if (l.humidity != -9999.0) atualizar_metrica(&c->umi, l.humidity, l.time);
            if (l.airpressure != -9999.0) atualizar_metrica(&c->pressao, l.airpressure, l.time);
            
            if (l.batterylevel != -1.0) {
                if (c->bateria_inicial == -1.0) c->bateria_inicial = l.batterylevel;
                c->bateria_final = l.batterylevel;
            }
            
            if (l.lora_spreading_factor >= 0 && l.lora_spreading_factor < 15) {
                c->sf_utilizados[l.lora_spreading_factor] = 1;
            }
            
            processar_periodo(c, l.time);
        }
        
        snprintf(msg, sizeof(msg), "[Thread 3] Identificando tensoes para consumo de bateria de %s...", nome_cidade);
        enviar_log(msg);
        
        if (c->bateria_inicial == -1.0) { c->bateria_inicial = 0; c->bateria_final = 0; }
    }
    
    enviar_log("[Thread 3] Todos os processamentos matematicos concluidos com sucesso.");
    pthread_exit(NULL);
}

// --- HELPER DE IMPRESSÃO ---
void imprimir_sf(int sf_array[15]) {
    int primeiro = 1;
    for (int i = 0; i < 15; i++) {
        if (sf_array[i]) {
            if (!primeiro) printf(", ");
            printf("SF%d", i);
            primeiro = 0;
        }
    }
}

// --- MAIN ---
int main() {
    struct timeval inicio, fim;
    gettimeofday(&inicio, NULL);
    
    inicializar_cidade(&caxias); 
    inicializar_cidade(&bento);

    // Inicializando arq1 para a Thread 1 e arq2 para a Thread 2
    ArquivoInfo arq1 = {"mqtt_senzemo_cx_bg.json", 0, 0, "9999", "0000", 1};
    ArquivoInfo arq2 = {"senzemo_cx_bg.json", 0, 0, "9999", "0000", 2};

    pthread_t t1, t2, t_calc, t_log;
    
    // Thread 4: Logger fica em background aguardando sinais
    pthread_create(&t_log, NULL, thread_logger, NULL);
    enviar_log("[Thread 4] Servico de logger de auditoria iniciado e aguardando eventos.");

    // Threads 1 e 2: Leitura paralela
    pthread_create(&t1, NULL, thread_leitura, &arq1);
    pthread_create(&t2, NULL, thread_leitura, &arq2);
    
    pthread_join(t1, NULL); 
    pthread_join(t2, NULL);

    // Thread 3: Cálculos
    pthread_create(&t_calc, NULL, thread_calculos, NULL);
    pthread_join(t_calc, NULL);

    processamento_concluido = 1;
    pthread_cond_signal(&log_cond);
    pthread_join(t_log, NULL);

    gettimeofday(&fim, NULL);
    double decorrido = (fim.tv_sec - inicio.tv_sec) + (fim.tv_usec - inicio.tv_usec) / 1000000.0;

    // --- OUTPUT FINAL ---
    printf("============================================================\n");
    printf("ANÁLISE DE DADOS DOS SENSORES\n");
    printf("CityLivingLab\n");
    printf("Processamento utilizando pthreads\n");
    printf("============================================================\n\n");
    
    printf("Arquivo analisado: %s\n", arq1.nome_arquivo);
    printf("Total de registros processados: %d\n", arq1.registros_processados);
    printf("Duplicatas descartadas: %d\n", arq1.registros_removidos); 
    printf("Período analisado: %s a %s\n\n", arq1.data_inicio, arq1.data_fim);
    
    printf("Arquivo analisado: %s\n", arq2.nome_arquivo);
    printf("Total de registros processados: %d\n", arq2.registros_processados);
    printf("Duplicatas descartadas: %d\n", arq2.registros_removidos); 
    printf("Período analisado: %s a %s\n\n", arq2.data_inicio, arq2.data_fim);

    printf("TEMPERATURA (°C)\n");
    printf("--------------------------------------------------------------------------------------------------------\n");
    printf("Cidade            | Minima | Data/Hora                  | Maxima | Data/Hora                  | Media\n");
    printf("--------------------------------------------------------------------------------------------------------\n");
    printf("Caxias do Sul     | %.2f  | %-24s | %.2f  | %-24s | %.2f\n", 
            caxias.temp.min, caxias.temp.time_min, caxias.temp.max, caxias.temp.time_max, (caxias.temp.soma / caxias.temp.contagem));
    printf("Bento Goncalves   | %.2f  | %-24s | %.2f  | %-24s | %.2f\n\n", 
            bento.temp.min, bento.temp.time_min, bento.temp.max, bento.temp.time_max, (bento.temp.soma / bento.temp.contagem));

    printf("UMIDADE (%%)\n");
    printf("--------------------------------------------------------------------------------------------------------\n");
    printf("Cidade            | Minima | Data/Hora                  | Maxima | Data/Hora                  | Media\n");
    printf("--------------------------------------------------------------------------------------------------------\n");
    printf("Caxias do Sul     | %.2f  | %-24s | %.2f  | %-24s | %.2f\n", 
            caxias.umi.min, caxias.umi.time_min, caxias.umi.max, caxias.umi.time_max, (caxias.umi.soma / caxias.umi.contagem));
    printf("Bento Goncalves   | %.2f  | %-24s | %.2f  | %-24s | %.2f\n\n", 
            bento.umi.min, bento.umi.time_min, bento.umi.max, bento.umi.time_max, (bento.umi.soma / bento.umi.contagem));

    printf("PRESSAO ATMOSFERICA (hPa)\n");
    printf("--------------------------------------------------------------------------------------------------------\n");
    printf("Cidade            | Minima | Data/Hora                  | Maxima | Data/Hora                  | Media\n");
    printf("--------------------------------------------------------------------------------------------------------\n");
    printf("Caxias do Sul     | %.2f | %-24s | %.2f | %-24s | %.2f\n", 
            caxias.pressao.min, caxias.pressao.time_min, caxias.pressao.max, caxias.pressao.time_max, (caxias.pressao.soma / caxias.pressao.contagem));
    printf("Bento Goncalves   | %.2f | %-24s | %.2f | %-24s | %.2f\n\n", 
            bento.pressao.min, bento.pressao.time_min, bento.pressao.max, bento.pressao.time_max, (bento.pressao.soma / bento.pressao.contagem));

    printf("BATERIA\n");
    printf("------------------------------------------------------------\n");
    printf("Cidade            | Inicial (V) | Final (V) | Consumo (V)\n");
    printf("------------------------------------------------------------\n");
    printf("Caxias do Sul     | %.2f        | %.2f      | %.2f\n", caxias.bateria_inicial, caxias.bateria_final, (caxias.bateria_inicial - caxias.bateria_final));
    printf("Bento Goncalves   | %.2f        | %.2f      | %.2f\n\n", bento.bateria_inicial, bento.bateria_final, (bento.bateria_inicial - bento.bateria_final));

    printf("SPREADING FACTORS UTILIZADOS\n");
    printf("------------------------------------------------------------\n");
    printf("Cidade            | SF utilizados\n");
    printf("------------------------------------------------------------\n");
    printf("Caxias do Sul     | "); imprimir_sf(caxias.sf_utilizados); printf("\n");
    printf("Bento Goncalves   | "); imprimir_sf(bento.sf_utilizados); printf("\n\n");

    printf("DESEMPENHO\n");
    printf("------------------------------------------------------------\n");
    printf("Tempo total de execucao: %.4f segundos\n", decorrido);
    printf("Threads utilizadas: 4\n");
    printf("Thread 1: leitura do arquivo %s e eliminacao de duplicatas\n", arq1.nome_arquivo);
    printf("Thread 2: leitura do arquivo %s e eliminacao de duplicatas\n", arq2.nome_arquivo);
    printf("Thread 3: calculo das estatisticas\n");
    printf("Thread 4: registro de logs\n");
    printf("Arquivo de log gerado: processamento.log\n\n");

    printf("============================================================\n");
    printf("Processamento finalizado com sucesso.\n");
    printf("============================================================\n");

    if (caxias.leituras != NULL) free(caxias.leituras);
    if (bento.leituras != NULL) free(bento.leituras);

    return 0;
}

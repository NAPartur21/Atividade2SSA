from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pandas as pd
from datetime import datetime, date
import re

# ──────────────────────────────────────────────
# Funções auxiliares das PythonOperators
# ──────────────────────────────────────────────
"""Funções para as tasks PythonOperator da DAG av02_stream_pipeline."""

def _task2_tratar_datas():
    """Converte datas no formato yyyy-mm-dd para dd/mm/yyyy."""
    df = pd.read_csv('/opt/airflow/data/entrada.csv', sep=';')

    def normalizar_data(valor):
        valor = str(valor).strip()
        # Formato americano yyyy-mm-dd
        if re.match(r'^\d{4}-\d{2}-\d{2}$', valor):
            dt = datetime.strptime(valor, '%Y-%m-%d')
            return dt.strftime('%d/%m/%Y')
        return valor  # já está em dd/mm/yyyy

    df['data_execucao'] = df['data_execucao'].apply(normalizar_data)
    df.to_csv('/opt/airflow/data/task2.csv', sep=';', index=False)
    print(f"[TASK-2] Arquivo task2.csv gerado com {len(df)} registros.")


def _task3_remover_nome_vazio(**context):
    """Remove linhas com nome_musica vazio e empurra total descartado via XCom."""
    df = pd.read_csv('/opt/airflow/data/task2.csv', sep=';')
    total_antes = len(df)

    df_limpo = df[df['nome_musica'].notna() & (df['nome_musica'].str.strip() != '')]
    total_depois = len(df_limpo)
    descartados = total_antes - total_depois

    df_limpo.to_csv('/opt/airflow/data/task3.csv', sep=';', index=False)

    # Passa quantidade de descartados para a próxima task via XCom
    context['ti'].xcom_push(key='descartados', value=descartados)
    print(f"[TASK-3] Descartados: {descartados} | Mantidos: {total_depois}")


def _task4_inserir_descartados(**context):
    """Lê o XCom da TASK-3 e insere na tabela descartados."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    ti = context['ti']
    descartados = ti.xcom_pull(task_ids='task3_remover_nome_vazio', key='descartados')
    print(f"[TASK-4] Inserindo {descartados} na tabela descartados.")

    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run(f"INSERT INTO descartados (total) VALUES ({descartados});")


def _task6_enriquecer_genero(**context):
    """
    Lê task3.csv, faz join com os dados de genero_musical
    vindos do XCom da TASK-5 e gera task4.csv.
    """
    ti = context['ti']
    # Resultado do SQLExecuteQueryOperator vem como lista de tuplas
    rows = ti.xcom_pull(task_ids='task5_consultar_genero')
    # rows ex: [('001', 'POP'), ('002', 'ROCK'), ...]
    df_genero = pd.DataFrame(rows, columns=['id_genero', 'nome_genero'])

    df = pd.read_csv('/opt/airflow/data/task3.csv', sep=';')
    # Garante mesmo tipo para o join
    df['id_genero'] = df['id_genero'].astype(str).str.zfill(3)
    df_genero['id_genero'] = df_genero['id_genero'].astype(str).str.zfill(3)

    df_enriquecido = df.merge(df_genero, on='id_genero', how='left')
    df_enriquecido.to_csv('/opt/airflow/data/task4.csv', sep=';', index=False)
    print(f"[TASK-6] task4.csv gerado com {len(df_enriquecido)} registros.")


def _task7_media_avaliacao():
    """Calcula média de avaliação por música a partir de task4.csv."""
    df = pd.read_csv('/opt/airflow/data/task4.csv', sep=';')
    media = (
        df.groupby('nome_musica')['nota']
        .mean()
        .reset_index()
        .rename(columns={'nota': 'media_avaliacao'})
        .sort_values('media_avaliacao', ascending=False)
    )
    media.to_csv('/opt/airflow/data/media_avaliacao.csv', sep=';', index=False)
    print(f"[TASK-7] media_avaliacao.csv gerado com {len(media)} músicas.")


def _task8_total_artista():
    """Calcula total de músicas ouvidas por artista a partir de task4.csv."""
    df = pd.read_csv('/opt/airflow/data/task4.csv', sep=';')
    total = (
        df.groupby('nome_artista')
        .size()
        .reset_index(name='total_musicas')
        .sort_values('total_musicas', ascending=False)
    )
    total.to_csv('/opt/airflow/data/total_artista.csv', sep=';', index=False)
    print(f"[TASK-8] total_artista.csv gerado com {len(total)} artistas.")


# ──────────────────────────────────────────────
# Definição da DAG
# ──────────────────────────────────────────────

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='av02_stream_pipeline',
    default_args=default_args,
    schedule_interval=None,   # execução manual
    catchup=False,
    description='AV-02 SSA – Pipeline de dados de streaming musical',
    tags=['av02', 'stream', 'pandas'],
) as dag:

    # ── TASK-1: copiar arquivo de entrada ──────────────────────────────────
    task1_copiar_arquivo = BashOperator(
        task_id='task1_copiar_arquivo',
        bash_command='cp /opt/airflow/data/dados-stream.csv /opt/airflow/data/entrada.csv',
    )

    # ── TASK-2: tratar datas ───────────────────────────────────────────────
    task2_tratar_datas = PythonOperator(
        task_id='task2_tratar_datas',
        python_callable=_task2_tratar_datas,
    )

    # ── TASK-3: remover nome_musica vazio + push XCom ─────────────────────
    task3_remover_nome_vazio = PythonOperator(
        task_id='task3_remover_nome_vazio',
        python_callable=_task3_remover_nome_vazio,
    )

    # ── TASK-4: inserir descartados no banco ──────────────────────────────
    task4_inserir_descartados = PythonOperator(
        task_id='task4_inserir_descartados',
        python_callable=_task4_inserir_descartados,
    )

    # ── TASK-5: consultar tabela genero_musical ───────────────────────────
    task5_consultar_genero = PostgresOperator(
        task_id='task5_consultar_genero',
        postgres_conn_id='postgres_default',
        sql='SELECT id_genero, nome_genero FROM genero_musical;',
    )

    # ── TASK-6: enriquecer com nome_genero ────────────────────────────────
    task6_enriquecer_genero = PythonOperator(
        task_id='task6_enriquecer_genero',
        python_callable=_task6_enriquecer_genero,
    )

    # ── TASK-7: média de avaliação por música (paralela) ──────────────────
    task7_media_avaliacao = PythonOperator(
        task_id='task7_media_avaliacao',
        python_callable=_task7_media_avaliacao,
    )

    # ── TASK-8: total de músicas por artista (paralela) ───────────────────
    task8_total_artista = PythonOperator(
        task_id='task8_total_artista',
        python_callable=_task8_total_artista,
    )

    # ── TASK-9: remover entrada.csv (trigger_rule=ALL_DONE) ───────────────
    task9_remover_entrada = BashOperator(
        task_id='task9_remover_entrada',
        bash_command='rm -f /opt/airflow/data/entrada.csv',
        trigger_rule='all_done',   # executa mesmo se TASK-7 ou TASK-8 falharem
    )

    # ── TASK-10: marcador de fim de pipeline ──────────────────────────────
    task10_fim = EmptyOperator(
        task_id='task10_fim_pipeline',
    )

    task1_copiar_arquivo >> task2_tratar_datas >> task3_remover_nome_vazio

    task3_remover_nome_vazio >> task4_inserir_descartados
    task3_remover_nome_vazio >> task5_consultar_genero >> task6_enriquecer_genero

    task6_enriquecer_genero >> [task7_media_avaliacao, task8_total_artista]

    [task7_media_avaliacao, task8_total_artista] >> task9_remover_entrada >> task10_fim

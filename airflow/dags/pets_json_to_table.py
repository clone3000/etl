from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'student_name',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pets_json_to_table',
    default_args=default_args,
    description='JSON с питомцами',
    schedule_interval='@once',
    catchup=False,
    tags=['etl', 'json']
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=lambda: {
            "pets": [
                {
                    "name": "Purrsloud",
                    "species": "Cat",
                    "favFoods": ["wet food", "dry food", "<strong>any</strong> food"],
                    "birthYear": 2016,
                    "photo": "https://learnwebcode.github.io/json-example/images/cat-2.jpg"
                },
                {
                    "name": "Barksalot",
                    "species": "Dog",
                    "birthYear": 2008,
                    "photo": "https://learnwebcode.github.io/json-example/images/dog-1.jpg"
                },
                {
                    "name": "Meowsalot",
                    "species": "Cat",
                    "favFoods": ["tuna", "catnip", "celery"],
                    "birthYear": 2012,
                    "photo": "https://learnwebcode.github.io/json-example/images/cat-1.jpg"
                }
            ]
        }
    )
    
    def transform_data(**context):
        pets_data = context['ti'].xcom_pull(task_ids='extract')
        
        transformed = []
        for pet in pets_data['pets']:
            fav_foods = pet.get('favFoods', [])
            fav_foods_str = ', '.join(fav_foods) if isinstance(fav_foods, list) else str(fav_foods)
            
            transformed.append({
                'name': pet['name'],
                'species': pet['species'],
                'fav_foods': fav_foods_str,
                'birth_year': pet['birthYear'],
                'photo': pet['photo']
            })
        
        return transformed
    
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        dag=dag
    )
    
    def load_data(**context):
        import psycopg2
        
        transformed_data = context['ti'].xcom_pull(task_ids='transform')
        
        conn = psycopg2.connect(
            host="postgres",  
            database="airflow",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pets_normalized (
                    id SERIAL PRIMARY KEY,
                    pet_name VARCHAR(100),
                    species VARCHAR(50),
                    fav_foods TEXT,
                    birth_year INTEGER,
                    photo_url TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            for pet in transformed_data:
                cursor.execute("""
                    INSERT INTO pets_normalized (pet_name, species, fav_foods, birth_year, photo_url)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    pet['name'],
                    pet['species'],
                    pet['fav_foods'],
                    pet['birth_year'],
                    pet['photo']
                ))
            
            conn.commit()
            print(f"Успешно загружено {len(transformed_data)} записей")
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()
    
    load_task = PythonOperator(
        task_id='load',
        python_callable=load_data,
        dag=dag
    )

    extract_task >> transform_task >> load_task
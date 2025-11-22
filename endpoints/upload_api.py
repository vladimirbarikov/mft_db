import os
import tempfile
import logging
from pathlib import Path

import requests
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
current_directory = Path(__file__).resolve().parent.parent.parent
env_path = current_directory / '.env'
load_dotenv(dotenv_path=env_path)

# Создание Flask-приложения
app = Flask(__name__)

# Установка секретного ключа из переменной окружения
app.secret_key = os.getenv('FLASK_SECRET_KEY')

@app.route('/upload-excel', methods=['POST'])
def upload_excel():
    '''
    Accepts only specific file named "sample_mft_data.xlsx"
    '''
    # Проверка наличия файла
    if 'file' not in request.files:
        logger.error('Файл не передан.')
        return jsonify({'error': 'Файл не передан'}), 400

    file = request.files['file']

    # Проверка имени файла
    expected_filename = 'sample_mft_data.xlsx'
    if file.filename != expected_filename:
        logger.error(f'Необходимо предоставить файл с именем "{expected_filename}".')
        return jsonify({'error': f'Необходимо предоставить файл с именем "{expected_filename}".'}), 400

    # Проверка расширения файла
    if not file.filename.lower().endswith('.xlsx'):
        logger.error('Только файлы формата *.xlsx разрешены.')
        return jsonify({'error': 'Только файлы формата *.xlsx разрешены'}), 400

    # Создание временного файла
    temp_dir = tempfile.mkdtemp()
    temp_file_path = os.path.join(temp_dir, file.filename)
    file.save(temp_file_path)

    try:
        # Отправка запроса в Airflow
        response = requests.post(
            'http://localhost:8080/api/v1/dags/excel_processing_dag/dagRuns',
            json={
                'conf': {'file_path': temp_file_path},
                'execution_date': 'NOW'
            },
            auth=('airflow', 'airflow'),
            timeout=10  # Timeout ожидания ответа
        )

        if response.status_code != 200:
            logger.error('Ошибка при постановке задачи в очередь: %s', response.text)
            return jsonify({'error': 'Ошибка при постановке задачи в очередь'}), 500

        logger.info('Файл успешно поставлен в очередь на обработку.')
        return jsonify({'message': 'Файл успешно поставлен в очередь на обработку'}), 200
    finally:
        # Временный файл будет удалён в задаче Airflow
        pass

if __name__ == '__main__':
    app.run(debug=True)

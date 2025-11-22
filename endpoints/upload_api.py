import os
import tempfile
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

@app.route('/upload-excel', methods=['POST'])
def upload_excel():
    # Проверяем, прислан ли файл
    if 'file' not in request.files:
        return jsonify({'error': 'Файл не передан'}), 400

    file = request.files['file']

    # Проверяем расширение файла
    if not file.filename.lower().endswith('.xlsx'):
        return jsonify({'error': 'Только файлы формата *.xlsx разрешены'}), 400

    # Создаем временный файл для обработки
    temp_dir = tempfile.mkdtemp()
    temp_file_path = os.path.join(temp_dir, file.filename)
    file.save(temp_file_path)

    try:
        # Отправляем запрос в Airflow
        response = requests.post(
            'http://localhost:8080/api/v1/dags/excel_processing_dag/dagRuns',
            json={
                'conf': {'file_path': temp_file_path},
                'execution_date': 'NOW'
            },
            auth=('airflow', 'airflow'),  # Если включена аутентификация
            timeout=10  # Таймаут ожидания ответа в секундах
        )

        if response.status_code != 200:
            return jsonify({'error': 'Ошибка при постановке задачи в очередь'}), 500

        return jsonify({'message': 'Файл успешно поставлен в очередь на обработку'}), 200
    finally:
        # Пока не удаляем файл, так как он нужен для обработки
        pass

if __name__ == '__main__':
    app.run(debug=True)

# Код для демо к докладу на PiterPy 2025

[Страница доклада](https://piterpy.com/talks/e90ebc6621f44b668b90e420ba364ceb/)

## Airflow

```bash
cd ./airflow-exmpl
pyenv virtualenv 3.12 ppy312-af
pyenv local ppy312-af
pip install 'apache-airflow==3.0.1' \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.1/constraints-3.10.txt"
pip install lxml pandas scikit-learn
```

(последняя строчка ставит либы, который уже используются самим пайплайном)

На всякий случай конкретные версии библиотек в [requirements.txt](./airflow-exmpl/requirements.txt)

Дальше убеждаемся что мы в папке `./airflow-exmpl`, указываем аирфлоу где мы хотим чтобы он раскидал свои файлы и запускаемся:

```bash
export AIRFLOW_HOME=$(pwd)
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow standalone
```

в выводе консоли где-то будет строчка про пароль админа. С ним (и логином `admin`) логинимся в браузере по адресу http://localhost:8080

Код пайплайна: [piterpy_dag.py](./airflow-exmpl/dags/piterpy_dag.py)

Одна из тасок ожидает что есть файл `/tmp/data/sample_data.csv`, если она падает, причина скорее всего в том что его там нет (надо либо поправить путь в файла дага в [строчке 28](https://github.com/shrimpsizemoose/piterpy2025-airflow-talk/blob/b5ed2a7d611e617ae375c19183b23591efeb9f11/airflow-exmpl/dags/piterpy_dag.py#L28) и в [строчке 36](https://github.com/shrimpsizemoose/piterpy2025-airflow-talk/blob/b5ed2a7d611e617ae375c19183b23591efeb9f11/airflow-exmpl/dags/piterpy_dag.py#L36]). А сам файл лежит в папке с сетапом дагстера [sample_data.csv](./dagster-quickstart/data/sample_data.csv)

## Dagster

Просто список комманд:

```bash
cd ./dagster-quickstart
pyenv virtualenv 3.10 ppy-dgstr310
pyenv local ppy-dgstr310
pip install dagster dagster-webserver pandas lxml
```

На всякий случай конкретные версии библиотек в [requirements.txt](./airflow-exmpl/requirements.txt)

Дальше убеждаемся что мы в папке `./dagster-quickstart`  и запускаемся:

```bash
dagster dev -f quickstart/assets.py
```

Определения ассетов: [assets.py](./dagster-quickstart/quickstart/assets.py)

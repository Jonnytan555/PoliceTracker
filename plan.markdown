uk_police_stop_search_mssql/
├─ docker/
│  └─ Dockerfile
├─ sql/
│  └─ schema.sql
├─ app/
│  ├─ __init__.py
│  ├─ config.py
│  ├─ utils.py
│  ├─ db.py
│  ├─ client.py
│  ├─ etl.py
│  ├─ mq.py
│  ├─ downloader_subject.py
│  ├─ observers.py
│  ├─ scheduler_producer.py
│  ├─ etl_worker.py
│  ├─ api.py
│  └─ templates/
│     └─ scala.html
├─ downloader/
│  ├─ __init__.py
│  ├─ config.py
│  ├─ downloader.py
│  ├─ file_download.py
│  ├─ slow_connection_error.py
│  └─ subject.py
├─ notifications/
│  ├─ __init__.py
│  └─ email.py
├─ tests/
│  └─ test_hash.py
├─ .env.sample
├─ requirements.txt
├─ docker-compose.yml
└─ README.md

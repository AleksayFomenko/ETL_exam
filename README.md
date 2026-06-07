<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ETL Module 4 — Exam</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Unbounded:wght@400;700&family=Geologica:wght@300;400;500&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #0d0f14;
  --surface: #13161d;
  --surface2: #1a1e28;
  --border: #252a38;
  --accent: #4f8ef7;
  --accent2: #7c3aed;
  --accent3: #10b981;
  --text: #e8ecf4;
  --text2: #8892a4;
  --text3: #4a5568;
  --code-bg: #0a0c10;
  --tag1: #1e3a5f;
  --tag1t: #60a5fa;
  --tag2: #1a1040;
  --tag2t: #a78bfa;
  --tag3: #0d2e20;
  --tag3t: #34d399;
}

* { margin: 0; padding: 0; box-sizing: border-box; }

body {
  background: var(--bg);
  color: var(--text);
  font-family: 'Geologica', sans-serif;
  font-weight: 300;
  line-height: 1.7;
  min-height: 100vh;
}

.grid-bg {
  position: fixed;
  inset: 0;
  background-image:
    linear-gradient(rgba(79,142,247,0.03) 1px, transparent 1px),
    linear-gradient(90deg, rgba(79,142,247,0.03) 1px, transparent 1px);
  background-size: 40px 40px;
  pointer-events: none;
  z-index: 0;
}

.container {
  max-width: 900px;
  margin: 0 auto;
  padding: 0 2rem 6rem;
  position: relative;
  z-index: 1;
}

/* HEADER */
header {
  padding: 4rem 0 3rem;
  border-bottom: 1px solid var(--border);
  margin-bottom: 3rem;
}

.header-meta {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 1.5rem;
}

.tag {
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  padding: 3px 10px;
  border-radius: 3px;
  letter-spacing: 0.05em;
}

.tag-blue { background: var(--tag1); color: var(--tag1t); border: 1px solid rgba(96,165,250,0.2); }
.tag-purple { background: var(--tag2); color: var(--tag2t); border: 1px solid rgba(167,139,250,0.2); }
.tag-green { background: var(--tag3); color: var(--tag3t); border: 1px solid rgba(52,211,153,0.2); }

h1 {
  font-family: 'Unbounded', sans-serif;
  font-size: clamp(1.6rem, 4vw, 2.4rem);
  font-weight: 700;
  line-height: 1.2;
  letter-spacing: -0.02em;
  margin-bottom: 1rem;
  background: linear-gradient(135deg, #e8ecf4 0%, #8892a4 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}

.header-info {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 1rem;
  margin-top: 2rem;
}

.info-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 1rem 1.25rem;
}

.info-label {
  font-size: 11px;
  color: var(--text3);
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-family: 'JetBrains Mono', monospace;
  margin-bottom: 4px;
}

.info-value {
  font-size: 14px;
  color: var(--text);
  font-weight: 400;
}

/* STACK */
.stack-section {
  margin-bottom: 3rem;
}

.stack-grid {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 1rem;
}

.stack-item {
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px;
  padding: 5px 12px;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 4px;
  color: var(--text2);
  transition: all 0.2s;
}

.stack-item:hover {
  border-color: var(--accent);
  color: var(--accent);
}

/* SCORE */
.score-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 1rem;
  margin-bottom: 3rem;
}

.score-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 1.25rem;
  text-align: center;
  position: relative;
  overflow: hidden;
}

.score-card::before {
  content: '';
  position: absolute;
  top: 0; left: 0; right: 0;
  height: 2px;
}

.score-card.t1::before { background: var(--accent); }
.score-card.t2::before { background: var(--accent2); }
.score-card.t3::before { background: var(--accent3); }
.score-card.t4::before { background: #f59e0b; }
.score-card.total::before { background: linear-gradient(90deg, var(--accent), var(--accent2), var(--accent3)); }

.score-num {
  font-family: 'Unbounded', sans-serif;
  font-size: 2rem;
  font-weight: 700;
  line-height: 1;
  margin-bottom: 6px;
}

.t1 .score-num { color: var(--accent); }
.t2 .score-num { color: var(--accent2); }
.t3 .score-num { color: var(--accent3); }
.t4 .score-num { color: #f59e0b; }
.total .score-num { color: var(--text); }

.score-label {
  font-size: 11px;
  color: var(--text3);
  text-transform: uppercase;
  letter-spacing: 0.06em;
  font-family: 'JetBrains Mono', monospace;
}

/* SECTION HEADINGS */
.section-title {
  font-family: 'Unbounded', sans-serif;
  font-size: 11px;
  font-weight: 400;
  text-transform: uppercase;
  letter-spacing: 0.15em;
  color: var(--text3);
  margin-bottom: 2rem;
  display: flex;
  align-items: center;
  gap: 12px;
}

.section-title::after {
  content: '';
  flex: 1;
  height: 1px;
  background: var(--border);
}

/* TASK BLOCK */
.task {
  margin-bottom: 3.5rem;
}

.task-header {
  display: flex;
  align-items: flex-start;
  gap: 1.25rem;
  margin-bottom: 2rem;
}

.task-num {
  font-family: 'Unbounded', sans-serif;
  font-size: 11px;
  font-weight: 700;
  color: var(--bg);
  background: var(--accent);
  width: 28px;
  height: 28px;
  border-radius: 6px;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  margin-top: 2px;
}

.task-num.p { background: var(--accent2); }
.task-num.g { background: var(--accent3); }
.task-num.a { background: #f59e0b; }

.task-title {
  font-family: 'Unbounded', sans-serif;
  font-size: 1.1rem;
  font-weight: 700;
  color: var(--text);
  line-height: 1.3;
}

.task-subtitle {
  font-size: 13px;
  color: var(--text2);
  margin-top: 4px;
}

/* STEPS */
.step {
  margin-bottom: 2rem;
  padding-left: 1.5rem;
  border-left: 1px solid var(--border);
}

.step-label {
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  color: var(--text3);
  text-transform: uppercase;
  letter-spacing: 0.08em;
  margin-bottom: 6px;
}

.step-title {
  font-size: 15px;
  font-weight: 500;
  color: var(--text);
  margin-bottom: 12px;
}

/* CODE BLOCK */
.code-block {
  background: var(--code-bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  overflow: hidden;
  margin: 1rem 0;
}

.code-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 14px;
  border-bottom: 1px solid var(--border);
  background: var(--surface);
}

.code-lang {
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  color: var(--text3);
  text-transform: uppercase;
  letter-spacing: 0.08em;
}

.code-dots {
  display: flex;
  gap: 5px;
}

.dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--border);
}

pre {
  padding: 1.25rem 1.5rem;
  overflow-x: auto;
  font-family: 'JetBrains Mono', monospace;
  font-size: 13px;
  line-height: 1.6;
  color: #abb2bf;
}

.kw { color: #c678dd; }
.fn { color: #61afef; }
.str { color: #98c379; }
.cm { color: #5c6370; font-style: italic; }
.num { color: #d19a66; }
.type { color: #e5c07b; }

/* SCREENSHOT PLACEHOLDER */
.screenshot {
  background: var(--surface);
  border: 1px dashed var(--border);
  border-radius: 8px;
  padding: 2rem;
  text-align: center;
  margin: 1rem 0;
}

.screenshot-icon {
  width: 36px;
  height: 36px;
  margin: 0 auto 10px;
  opacity: 0.2;
}

.screenshot-path {
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px;
  color: var(--text3);
  margin-bottom: 4px;
}

.screenshot-hint {
  font-size: 12px;
  color: var(--text3);
}

/* REPO TREE */
.tree {
  background: var(--code-bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 1.25rem 1.5rem;
  font-family: 'JetBrains Mono', monospace;
  font-size: 13px;
  line-height: 1.9;
  color: var(--text2);
}

.tree .dir { color: var(--accent); }
.tree .file { color: var(--text2); }
.tree .comment { color: var(--text3); }

/* FOOTER */
footer {
  border-top: 1px solid var(--border);
  padding-top: 2rem;
  margin-top: 4rem;
  font-size: 12px;
  color: var(--text3);
  font-family: 'JetBrains Mono', monospace;
  display: flex;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 8px;
}

img.screen {
  width: 100%;
  border-radius: 8px;
  border: 1px solid var(--border);
  display: block;
  margin: 1rem 0;
}
</style>
</head>
<body>
<div class="grid-bg"></div>
<div class="container">

  <!-- HEADER -->
  <header>
    <div class="header-meta">
      <span class="tag tag-blue">ETL-процессы</span>
      <span class="tag tag-purple">Модуль 4</span>
      <span class="tag tag-green">Экзамен</span>
    </div>
    <h1>Реализация ETL-процесса</h1>
    <div class="header-info">
      <div class="info-card">
        <div class="info-label">Студент</div>
        <div class="info-value">Фамилия Имя</div>
      </div>
      <div class="info-card">
        <div class="info-label">Преподаватель</div>
        <div class="info-value">Артём Озерков</div>
      </div>
      <div class="info-card">
        <div class="info-label">Время выполнения</div>
        <div class="info-value">10 часов</div>
      </div>
      <div class="info-card">
        <div class="info-label">Дисциплина</div>
        <div class="info-value">ETL-процессы</div>
      </div>
    </div>
  </header>

  <!-- STACK -->
  <div class="stack-section">
    <div class="section-title">Стек технологий</div>
    <div class="stack-grid">
      <span class="stack-item">Yandex Data Processing</span>
      <span class="stack-item">Apache Airflow</span>
      <span class="stack-item">Apache Kafka</span>
      <span class="stack-item">PySpark</span>
      <span class="stack-item">Yandex DataTransfer</span>
      <span class="stack-item">Yandex DataLens</span>
      <span class="stack-item">Yandex Object Storage</span>
      <span class="stack-item">YDB</span>
      <span class="stack-item">HiveQL / Spark SQL / YQL</span>
      <span class="stack-item">GitHub</span>
    </div>
  </div>

  <!-- SCORES -->
  <div class="section-title">Оценка</div>
  <div class="score-grid">
    <div class="score-card t1">
      <div class="score-num">2</div>
      <div class="score-label">Задание 1</div>
    </div>
    <div class="score-card p">
      <div class="score-num">2</div>
      <div class="score-label">Задание 2</div>
    </div>
    <div class="score-card g">
      <div class="score-num">3</div>
      <div class="score-label">Задание 3</div>
    </div>
    <div class="score-card a">
      <div class="score-num">1</div>
      <div class="score-label">Задание 4</div>
    </div>
    <div class="score-card total">
      <div class="score-num">8</div>
      <div class="score-label">Итого</div>
    </div>
  </div>

  <!-- TASK 1 -->
  <div class="section-title">Задания</div>

  <div class="task">
    <div class="task-header">
      <div class="task-num">1</div>
      <div>
        <div class="task-title">Yandex DataTransfer</div>
        <div class="task-subtitle">Перенос данных из YDB в Object Storage</div>
      </div>
    </div>

    <div class="step">
      <div class="step-label">Шаг 1.1</div>
      <div class="step-title">Создание БД Yandex DataBase</div>
      <img class="screen" src="screens/task1_ydb_create.png" alt="Создание YDB">
    </div>

    <div class="step">
      <div class="step-label">Шаг 1.2</div>
      <div class="step-title">Подготовка таблицы transactions_v2</div>
      <div class="code-block">
        <div class="code-header">
          <span class="code-lang">YQL</span>
          <div class="code-dots"><div class="dot"></div><div class="dot"></div><div class="dot"></div></div>
        </div>
        <pre><span class="kw">CREATE TABLE</span> <span class="type">transactions_v2</span> (
    call_id           <span class="type">Utf8</span>,
    call_time         <span class="type">Datetime</span>,
    client_id         <span class="type">Utf8</span>,
    region_code       <span class="type">Utf8</span>,
    campaign_type     <span class="type">Utf8</span>,
    call_status       <span class="type">Utf8</span>,
    client_response   <span class="type">Utf8</span>,
    duration_sec      <span class="type">Int32</span>,
    follow_up_required <span class="type">Bool</span>,
    <span class="kw">PRIMARY KEY</span> (call_id)
);</pre>
      </div>
      <img class="screen" src="screens/task1_ydb_table.png" alt="Таблица в YDB">
    </div>

    <div class="step">
      <div class="step-label">Шаг 1.3</div>
      <div class="step-title">Создание трансфера в Object Storage</div>
      <img class="screen" src="screens/task1_transfer_setup.png" alt="Настройка трансфера">
    </div>

    <div class="step">
      <div class="step-label">Шаг 1.4</div>
      <div class="step-title">Проверка работоспособности трансфера</div>
      <img class="screen" src="screens/task1_transfer_result.png" alt="Результат трансфера">
    </div>
  </div>

  <!-- TASK 2 -->
  <div class="task">
    <div class="task-header">
      <div class="task-num p">2</div>
      <div>
        <div class="task-title">Автоматизация Yandex Data Processing</div>
        <div class="task-subtitle">Apache Airflow + PySpark</div>
      </div>
    </div>

    <div class="step">
      <div class="step-label">Шаг 2.1</div>
      <div class="step-title">Подготовка инфраструктуры</div>
      <img class="screen" src="screens/task2_infra.png" alt="Инфраструктура">
    </div>

    <div class="step">
      <div class="step-label">Шаг 2.2</div>
      <div class="step-title">PySpark-задание</div>
      <div class="code-block">
        <div class="code-header">
          <span class="code-lang">pyspark_job.py</span>
          <div class="code-dots"><div class="dot"></div><div class="dot"></div><div class="dot"></div></div>
        </div>
        <pre><span class="cm"># вставить код PySpark-задания</span></pre>
      </div>
      <img class="screen" src="screens/task2_pyspark_job.png" alt="PySpark задание">
    </div>

    <div class="step">
      <div class="step-label">Шаг 2.3</div>
      <div class="step-title">DAG-файл</div>
      <div class="code-block">
        <div class="code-header">
          <span class="code-lang">dag.py</span>
          <div class="code-dots"><div class="dot"></div><div class="dot"></div><div class="dot"></div></div>
        </div>
        <pre><span class="cm"># вставить код DAG</span></pre>
      </div>
      <img class="screen" src="screens/task2_dag.png" alt="DAG в Airflow">
    </div>

    <div class="step">
      <div class="step-label">Шаг 2.4</div>
      <div class="step-title">Результат выполнения DAG</div>
      <img class="screen" src="screens/task2_dag_result.png" alt="Результат DAG">
    </div>
  </div>

  <!-- TASK 3 -->
  <div class="task">
    <div class="task-header">
      <div class="task-num g">3</div>
      <div>
        <div class="task-title">Apache Kafka + PySpark</div>
        <div class="task-subtitle">Потоковая аналитика через Yandex Data Processing</div>
      </div>
    </div>

    <div class="step">
      <div class="step-label">Шаг 3.1</div>
      <div class="step-title">Архитектура решения</div>
      <img class="screen" src="screens/task3_architecture.png" alt="Архитектура">
    </div>

    <div class="step">
      <div class="step-label">Шаг 3.2</div>
      <div class="step-title">PySpark-задание — чтение топика Kafka</div>
      <div class="code-block">
        <div class="code-header">
          <span class="code-lang">kafka_spark_job.py</span>
          <div class="code-dots"><div class="dot"></div><div class="dot"></div><div class="dot"></div></div>
        </div>
        <pre><span class="cm"># вставить код чтения топика</span></pre>
      </div>
      <img class="screen" src="screens/task3_pyspark.png" alt="Kafka PySpark">
    </div>

    <div class="step">
      <div class="step-label">Шаг 3.3</div>
      <div class="step-title">Разворачивание JSON в плоский вид</div>
      <div class="code-block">
        <div class="code-header">
          <span class="code-lang">flatten.py</span>
          <div class="code-dots"><div class="dot"></div><div class="dot"></div><div class="dot"></div></div>
        </div>
        <pre><span class="cm"># вставить код flatten JSON</span></pre>
      </div>
      <img class="screen" src="screens/task3_flatten_result.png" alt="Плоская таблица">
    </div>
  </div>

  <!-- TASK 4 -->
  <div class="task">
    <div class="task-header">
      <div class="task-num a">4</div>
      <div>
        <div class="task-title">Визуализация в DataLens</div>
        <div class="task-subtitle">Дашборды для загруженных данных</div>
      </div>
    </div>

    <div class="step">
      <div class="step-label">Шаг 4.1</div>
      <div class="step-title">Дашборд</div>
      <img class="screen" src="screens/task4_dashboard.png" alt="Дашборд DataLens">
    </div>
  </div>

  <!-- REPO STRUCTURE -->
  <div class="section-title">Структура репозитория</div>
  <div class="tree">
<span class="dir">├── screens/</span>
│   <span class="file">├── task1_ydb_create.png</span>
│   <span class="file">├── task1_ydb_table.png</span>
│   <span class="file">├── task1_transfer_setup.png</span>
│   <span class="file">├── task1_transfer_result.png</span>
│   <span class="file">├── task2_infra.png</span>
│   <span class="file">├── task2_pyspark_job.png</span>
│   <span class="file">├── task2_dag.png</span>
│   <span class="file">├── task2_dag_result.png</span>
│   <span class="file">├── task3_architecture.png</span>
│   <span class="file">├── task3_pyspark.png</span>
│   <span class="file">├── task3_flatten_result.png</span>
│   <span class="file">└── task4_dashboard.png</span>
<span class="dir">├── sql/</span>
│   <span class="file">├── task1_create_table.yql</span>
│   <span class="file">└── task3_flatten.sql</span>
<span class="dir">├── src/</span>
│   <span class="file">├── pyspark_job.py</span>
│   <span class="file">├── dag.py</span>
│   <span class="file">└── kafka_spark_job.py</span>
<span class="file">└── README.html</span>  <span class="comment">← этот файл</span>
  </div>

  <footer>
    <span>ETL-процессы · Модуль 4 · Экзамен</span>
    <span>Преподаватель: Артём Озерков</span>
  </footer>

</div>
</body>
</html>

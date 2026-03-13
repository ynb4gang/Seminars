# Практика SQL по 1 семинару

Темы задач:

-   сложные JOIN
-   контроль кардинальности
-   анти‑соединения (ANTI JOIN)
-   коррелированные подзапросы
-   CTE pipeline
-   оконные функции
-   дедупликация
-   ранжирование
-   аналитические вычисления
-   оптимизация аналитических запросов

------------------------------------------------------------------------

# Схема таблиц

## employees

| column | type | описание |
|---|---|---|
| employee_id | INT | id сотрудника |
| name | TEXT | имя |
| department_id | INT | отдел |
| hire_date | DATE | дата найма |
| salary | NUMERIC | зарплата |
| status | TEXT | active / terminated |

---

## departments

| column | type | описание |
|---|---|---|
| department_id | INT | id |
| department_name | TEXT | название |
| region | TEXT | регион |

---

## projects

| column | type | описание |
|---|---|---|
| project_id | INT | проект |
| department_id | INT | владелец |
| start_date | DATE | дата начала |
| end_date | DATE | дата окончания |

---

## project_assignments

| column | type | описание |
|---|---|---|
| assignment_id | INT | запись |
| employee_id | INT | сотрудник |
| project_id | INT | проект |
| assigned_date | DATE | дата назначения |
| hours_logged | NUMERIC | отработанные часы |

---

## work_logs

| column | type | описание |
|---|---|---|
| log_id | INT | запись |
| employee_id | INT | сотрудник |
| log_date | DATE | дата |
| hours | NUMERIC | часы |
| task_type | TEXT | тип задачи |

---

## system_events

| column | type | описание |
|---|---|---|
| event_id | INT | событие |
| employee_id | INT | сотрудник |
| event_type | TEXT | login / task_create / deploy |
| event_time | TIMESTAMP | время |

------------------------------------------------------------------------

# Задачи

## 1

Найти сотрудников, работающих **минимум в 3 проектах одновременно**.

## 2

Найти сотрудников, которые работают **в проектах разных отделов**.

## 3

Найти отделы, где **средняя зарплата выше медианы по компании**.

## 4

Найти сотрудников, у которых **зарплата выше 90‑го перцентиля их
отдела**.

## 5

Найти сотрудников, которые **не участвовали ни в одном проекте за
последний год**.

## 6

Найти проекты, где **все сотрудники из одного отдела**.

## 7

Найти проекты, где **участвуют сотрудники более чем из 3 отделов**.

## 8

Найти **топ‑3 сотрудников по часам в каждом проекте**.

## 9

Посчитать **накопительные часы сотрудника по времени**.

## 10

Найти сотрудников, которые **работали каждый день последней недели**.

## 11

Найти сотрудников с **самым длинным непрерывным периодом работы без
выходных**.

## 12

Найти **первый проект каждого сотрудника**.

## 13

Найти **последний проект каждого сотрудника**.

## 14

Найти сотрудников, которые **работают больше среднего по своему
отделу**.

## 15

Найти **топ‑5 сотрудников по часам внутри отдела**.

## 16

Посчитать **долю часов сотрудника в проекте**.

## 17

Найти сотрудников, **которые никогда не логинились в систему**.

## 18

Найти сотрудников **без активности последние 30 дней**.

## 19

Найти сотрудников, у которых **login → task_create → deploy происходят в
течение 1 часа**.

## 20

Найти **первое событие каждого сотрудника**.

## 21

Найти сотрудников с **наибольшим ростом часов месяц‑к‑месяцу**.

## 22

Найти проекты с **самым большим ростом активности**.

## 23

Найти сотрудников, которые **работают одновременно над максимальным
количеством проектов**.

## 24

Найти сотрудников, которые **работали во всех проектах своего отдела**.

## 25

Найти сотрудников, которые **работали хотя бы в одном проекте каждого
региона**.

## 26

Построить **ранжирование сотрудников по активности**:

метрика:

hours_logged + количество событий

## 27

Найти сотрудников, у которых **доля часов в одном проекте \> 70%**.

## 28

Найти **цепочки событий пользователей длиной ≥3**.

## 29

Найти **топ‑3 сотрудников по продуктивности внутри региона**.

## 30

Найти **топ‑2 сотрудников в каждом регионе по суммарным часам проектов
за год**,\
учитывая только сотрудников:
-   участвующих минимум в 2 проектах
-   с активным статусом
-   без перерывов в работе более 30 дней

------------------------------------------------------------------------

# Решения

## 1

``` sql
WITH project_ranges AS (
    SELECT
        pa.employee_id,
        p.project_id,
        p.start_date,
        p.end_date
    FROM project_assignments pa
    JOIN projects p USING(project_id)
),

timeline AS (
    SELECT employee_id, start_date AS dt, 1 AS delta FROM project_ranges
    UNION ALL
    SELECT employee_id, end_date, -1 FROM project_ranges
),

running_projects AS (
    SELECT
        employee_id,
        dt,
        SUM(delta) OVER(
            PARTITION BY employee_id
            ORDER BY dt
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS active_projects
    FROM timeline
)

SELECT DISTINCT employee_id
FROM running_projects
WHERE active_projects >= 3;
```

## 2

``` sql
WITH employee_projects AS (
    SELECT
        pa.employee_id,
        p.department_id
    FROM project_assignments pa
    JOIN projects p USING(project_id)
)

SELECT employee_id
FROM employee_projects
GROUP BY employee_id
HAVING COUNT(DISTINCT department_id) > 1;
```

## 3

``` sql
WITH dept_avg AS (
    SELECT
        department_id,
        AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department_id
),

company_median AS (
    SELECT
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary
    FROM employees
)

SELECT d.department_id
FROM dept_avg d
CROSS JOIN company_median m
WHERE d.avg_salary > m.median_salary;
```

## 4

``` sql
WITH salary_rank AS (
    SELECT
        employee_id,
        department_id,
        salary,
        PERCENTILE_CONT(0.9)
        WITHIN GROUP (ORDER BY salary)
        OVER (PARTITION BY department_id) AS p90
    FROM employees
)

SELECT employee_id
FROM salary_rank
WHERE salary > p90;
```

## 5

``` sql
WITH last_year AS (
    SELECT *
    FROM project_assignments
    WHERE assigned_date >= CURRENT_DATE - INTERVAL '1 year'
)

SELECT e.employee_id
FROM employees e
LEFT JOIN last_year p
ON e.employee_id = p.employee_id
WHERE p.employee_id IS NULL;
```

## 6

``` sql
WITH project_depts AS (
    SELECT
        pa.project_id,
        e.department_id
    FROM project_assignments pa
    JOIN employees e USING(employee_id)
)

SELECT project_id
FROM project_depts
GROUP BY project_id
HAVING COUNT(DISTINCT department_id) = 1;
```

## 7

``` sql
WITH project_depts AS (
    SELECT
        pa.project_id,
        e.department_id
    FROM project_assignments pa
    JOIN employees e USING(employee_id)
)

SELECT project_id
FROM project_depts
GROUP BY project_id
HAVING COUNT(DISTINCT department_id) > 3;
```

## 8

``` sql
WITH emp_project_hours AS (
    SELECT
        pa.project_id,
        wl.employee_id,
        SUM(wl.hours) AS hours
    FROM project_assignments pa
    JOIN work_logs wl
        ON pa.employee_id = wl.employee_id
    GROUP BY 1,2
),

ranked AS (
    SELECT
        project_id,
        employee_id,
        hours,
        DENSE_RANK() OVER(
            PARTITION BY project_id
            ORDER BY hours DESC
        ) AS r
    FROM emp_project_hours
)

SELECT project_id, employee_id, hours
FROM ranked
WHERE r <= 3;
```

## 9

``` sql
SELECT
    employee_id,
    log_date,
    SUM(hours) OVER(
        PARTITION BY employee_id
        ORDER BY log_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_hours
FROM work_logs;
```

## 10

``` sql
WITH last_week AS (
    SELECT *
    FROM work_logs
    WHERE log_date >= CURRENT_DATE - INTERVAL '7 days'
),

worked_days AS (
    SELECT
        employee_id,
        COUNT(DISTINCT log_date) AS days
    FROM last_week
    GROUP BY employee_id
)

SELECT employee_id
FROM worked_days
WHERE days = 7;
```

## 11

``` sql
WITH ordered AS (
    SELECT
        employee_id,
        log_date,
        log_date - ROW_NUMBER() OVER(
            PARTITION BY employee_id
            ORDER BY log_date
        ) * INTERVAL '1 day' AS grp
    FROM work_logs
),

streaks AS (
    SELECT
        employee_id,
        COUNT(*) AS streak
    FROM ordered
    GROUP BY employee_id, grp
)

SELECT *
FROM streaks
ORDER BY streak DESC
LIMIT 1;
```

## 12

``` sql
WITH ranked AS (
    SELECT
        employee_id,
        project_id,
        assigned_date,
        ROW_NUMBER() OVER(
            PARTITION BY employee_id
            ORDER BY assigned_date
        ) AS r
    FROM project_assignments
)

SELECT employee_id, project_id
FROM ranked
WHERE r = 1;
```

## 13

``` sql
WITH ranked AS (
    SELECT
        employee_id,
        project_id,
        assigned_date,
        ROW_NUMBER() OVER(
            PARTITION BY employee_id
            ORDER BY assigned_date DESC
        ) AS r
    FROM project_assignments
)

SELECT employee_id, project_id
FROM ranked
WHERE r = 1;
```

## 14

``` sql
WITH emp_hours AS (
    SELECT
        employee_id,
        SUM(hours) AS total_hours
    FROM work_logs
    GROUP BY employee_id
),

dept_avg AS (
    SELECT
        e.department_id,
        AVG(total_hours) AS avg_hours
    FROM emp_hours h
    JOIN employees e USING(employee_id)
    GROUP BY 1
)

SELECT h.employee_id
FROM emp_hours h
JOIN employees e USING(employee_id)
JOIN dept_avg d USING(department_id)
WHERE h.total_hours > d.avg_hours;
```

## 15

``` sql
WITH emp_hours AS (
    SELECT
        employee_id,
        SUM(hours) AS total_hours
    FROM work_logs
    GROUP BY employee_id
),

ranked AS (
    SELECT
        e.department_id,
        h.employee_id,
        h.total_hours,
        DENSE_RANK() OVER(
            PARTITION BY e.department_id
            ORDER BY h.total_hours DESC
        ) AS r
    FROM emp_hours h
    JOIN employees e USING(employee_id)
)

SELECT *
FROM ranked
WHERE r <= 5;
```

## 16

``` sql
WITH emp_project_hours AS (
    SELECT
        employee_id,
        project_id,
        SUM(hours_logged) AS hours
    FROM project_assignments
    GROUP BY 1,2
),

project_total AS (
    SELECT
        project_id,
        SUM(hours) AS total
    FROM emp_project_hours
    GROUP BY 1
)

SELECT
    e.employee_id,
    e.project_id,
    e.hours / t.total AS share
FROM emp_project_hours e
JOIN project_total t USING(project_id);
```

## 17

``` sql
SELECT e.employee_id
FROM employees e
LEFT JOIN system_events s
ON e.employee_id = s.employee_id
AND s.event_type = 'login'
WHERE s.employee_id IS NULL;
```

## 18

``` sql
WITH last_activity AS (
    SELECT
        employee_id,
        MAX(event_time) AS last_event
    FROM system_events
    GROUP BY employee_id
)

SELECT employee_id
FROM last_activity
WHERE last_event < CURRENT_DATE - INTERVAL '30 days';
```

## 19

``` sql
WITH ordered AS (
    SELECT
        employee_id,
        event_type,
        event_time,
        LEAD(event_type,1) OVER w AS next_event,
        LEAD(event_type,2) OVER w AS third_event,
        LEAD(event_time,2) OVER w AS third_time
    FROM system_events
    WINDOW w AS (
        PARTITION BY employee_id
        ORDER BY event_time
    )
)

SELECT employee_id
FROM ordered
WHERE event_type='login'
AND next_event='task_create'
AND third_event='deploy'
AND third_time - event_time <= INTERVAL '1 hour';
```

## 20

``` sql
WITH ranked AS (
    SELECT
        employee_id,
        event_type,
        event_time,
        ROW_NUMBER() OVER(
            PARTITION BY employee_id
            ORDER BY event_time
        ) AS r
    FROM system_events
)

SELECT employee_id, event_type
FROM ranked
WHERE r = 1;
```

## 21

``` sql
WITH monthly_hours AS (
    SELECT
        employee_id,
        DATE_TRUNC('month', log_date) AS month,
        SUM(hours) AS hours
    FROM work_logs
    GROUP BY 1,2
),

growth AS (
    SELECT
        employee_id,
        month,
        hours,
        hours - LAG(hours) OVER(
            PARTITION BY employee_id
            ORDER BY month
        ) AS growth
    FROM monthly_hours
),

max_growth AS (
    SELECT
        employee_id,
        MAX(growth) AS max_growth
    FROM growth
    GROUP BY employee_id
)

SELECT employee_id, max_growth
FROM max_growth
ORDER BY max_growth DESC
LIMIT 1;
```

## 22

``` sql
WITH monthly AS (
    SELECT
        project_id,
        DATE_TRUNC('month', assigned_date) AS m,
        COUNT(*) AS activity
    FROM project_assignments
    GROUP BY 1,2
),

growth AS (
    SELECT
        project_id,
        activity - LAG(activity) OVER(
            PARTITION BY project_id
            ORDER BY m
        ) AS diff
    FROM monthly
)

SELECT project_id
FROM growth
ORDER BY diff DESC
LIMIT 1;
```

## 23

``` sql
WITH project_counts AS (
    SELECT
        employee_id,
        COUNT(DISTINCT project_id) AS cnt
    FROM project_assignments
    GROUP BY employee_id
)

SELECT *
FROM project_counts
WHERE cnt = (SELECT MAX(cnt) FROM project_counts);
```

## 24

``` sql
WITH dept_projects AS (
    SELECT
        department_id,
        COUNT(DISTINCT project_id) AS total_projects
    FROM projects
    GROUP BY department_id
),

emp_projects AS (
    SELECT
        e.employee_id,
        e.department_id,
        COUNT(DISTINCT pa.project_id) AS cnt
    FROM employees e
    JOIN project_assignments pa USING(employee_id)
    GROUP BY 1,2
)

SELECT e.employee_id
FROM emp_projects e
JOIN dept_projects d USING(department_id)
WHERE e.cnt = d.total_projects;
```

## 25

``` sql
WITH emp_regions AS (
    SELECT
        pa.employee_id,
        d.region
    FROM project_assignments pa
    JOIN projects p USING(project_id)
    JOIN departments d USING(department_id)
),

regions_total AS (
    SELECT COUNT(DISTINCT region) AS total
    FROM departments
)

SELECT employee_id
FROM emp_regions
GROUP BY employee_id
HAVING COUNT(DISTINCT region) = (SELECT total FROM regions_total);
```

## 26

``` sql
WITH hours AS (
    SELECT employee_id, SUM(hours) AS h
    FROM work_logs
    GROUP BY employee_id
),

events AS (
    SELECT employee_id, COUNT(*) AS e
    FROM system_events
    GROUP BY employee_id
),

activity AS (
    SELECT
        COALESCE(h.employee_id, e.employee_id) AS employee_id,
        COALESCE(h.h,0) + COALESCE(e.e,0) AS score
    FROM hours h
    FULL JOIN events e USING(employee_id)
)

SELECT *,
RANK() OVER(ORDER BY score DESC) AS r
FROM activity;
```

## 27

``` sql
WITH emp_proj AS (
    SELECT
        employee_id,
        project_id,
        SUM(hours_logged) AS hours
    FROM project_assignments
    GROUP BY 1,2
),

totals AS (
    SELECT
        employee_id,
        SUM(hours) AS total
    FROM emp_proj
    GROUP BY 1
)

SELECT
    e.employee_id,
    e.project_id
FROM emp_proj e
JOIN totals t USING(employee_id)
WHERE e.hours / t.total > 0.7;
```

## 28

``` sql
WITH ordered AS (
    SELECT
        employee_id,
        event_time,
        ROW_NUMBER() OVER(
            PARTITION BY employee_id
            ORDER BY event_time
        ) AS rn
    FROM system_events
),

groups AS (
    SELECT
        employee_id,
        event_time,
        event_time - rn * INTERVAL '1 second' AS grp
    FROM ordered
),

chains AS (
    SELECT
        employee_id,
        COUNT(*) AS chain_length
    FROM groups
    GROUP BY employee_id, grp
)

SELECT employee_id
FROM chains
WHERE chain_length >= 3;
```

## 29

``` sql
WITH emp_hours AS (
    SELECT
        e.employee_id,
        d.region,
        SUM(w.hours) AS hours
    FROM work_logs w
    JOIN employees e USING(employee_id)
    JOIN departments d USING(department_id)
    GROUP BY 1,2
),

ranked AS (
    SELECT *,
        DENSE_RANK() OVER(
            PARTITION BY region
            ORDER BY hours DESC
        ) AS r
    FROM emp_hours
)

SELECT *
FROM ranked
WHERE r <= 3;
```

## 30

``` sql
WITH yearly_hours AS (
    SELECT
        pa.employee_id,
        pa.project_id,
        DATE_TRUNC('year', pa.assigned_date) AS y,
        SUM(pa.hours_logged) AS hours
    FROM project_assignments pa
    GROUP BY 1,2,3
),

project_count AS (
    SELECT
        employee_id,
        COUNT(DISTINCT project_id) AS projects
    FROM yearly_hours
    GROUP BY 1
),

active_employees AS (
    SELECT employee_id
    FROM employees
    WHERE status = 'active'
),

activity_gaps AS (
    SELECT
        employee_id,
        log_date,
        log_date - LAG(log_date)
        OVER(PARTITION BY employee_id ORDER BY log_date) AS gap
    FROM work_logs
),

valid_employees AS (
    SELECT employee_id
    FROM activity_gaps
    GROUP BY employee_id
    HAVING MAX(gap) <= INTERVAL '30 days'
),

emp_region_hours AS (
    SELECT
        e.employee_id,
        d.region,
        SUM(pa.hours_logged) AS hours
    FROM project_assignments pa
    JOIN employees e USING(employee_id)
    JOIN departments d USING(department_id)
    GROUP BY 1,2
),

filtered AS (
    SELECT *
    FROM emp_region_hours
    WHERE employee_id IN (SELECT employee_id FROM active_employees)
      AND employee_id IN (SELECT employee_id FROM valid_employees)
      AND employee_id IN (
          SELECT employee_id
          FROM project_count
          WHERE projects >= 2
      )
),

ranked AS (
    SELECT *,
        DENSE_RANK() OVER(
            PARTITION BY region
            ORDER BY hours DESC
        ) AS r
    FROM filtered
)

SELECT *
FROM ranked
WHERE r <= 2;
```

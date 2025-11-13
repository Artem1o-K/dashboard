-- Витрина данных для анализа медицинских страховых выплат
-- Вариант задания №30
-- Создает VIEW на основе данных из stg_insurance

DROP VIEW IF EXISTS insurance_datamart;

CREATE VIEW insurance_datamart AS
SELECT
    age_group,
    sex,
    bmi_category,
    is_smoker,
    ROUND(AVG(charges)::numeric, 2) AS avg_charges
FROM
    stg_insurance
GROUP BY
    age_group, sex, bmi_category, is_smoker;

COMMENT ON VIEW insurance_datamart IS
'Витрина для анализа медицинских страховых выплат: средние charges по группам.';

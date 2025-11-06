USE company;

SELECT department, MAX(salary) AS max_salary
FROM employee
GROUP BY Department;
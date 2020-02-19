-- Apache pig (Haddop in real world - Task)
-- Load with column names and data types
employee = LOAD 'input/employee/employee_dataset_chicago' USING PigStorage(',') as (id:int, name:chararray, position:chararray, department:int, salary:float);
department = LOAD 'input/employee/department_dataset_chicago' USING PigStorage(';') as (id:int, name:chararray, address:map[]); 
bonus = LOAD 'input/employee/employee_bonus_chicago' USING PigStorage(',') as (empid:int, bonus:float); 

-- Select data (GENERATE key word)
dept = FOREACH department GENERATE id, name, address#'street', address#'city', address#'state';

-- Join employee and department dataset to display Department Name and Department Address
emp_dept_join = JOIN employee BY (department) , department BY (id);	

-- Join employee and bonus dataset and filter employess who got bonus
emp_bonus_join = JOIN employee BY (id) LEFT OUTER, bonus BY (empid);
emp_with_bonus = FILTER emp_bonus_join BY bonus::empid IS NOT NULL; 

-- average bonus by department.
emp_dept_bonus = FOREACH emp_with_bonus GENERATE employee::department, bonus::bonus;
groupby_dept = GROUP emp_dept_bonus BY employee::department;
avg_dept_bonus = FOREACH groupby_dept GENERATE group, ROUND(AVG(emp_dept_bonus.bonus)) as average_bonus;

-- number of employess in each department
emp_dept = FOREACH employee GENERATE id, department;
groupby_dept_emp = GROUP emp_dept BY department;
emp_dept_count = FOREACH groupby_dept_emp GENERATE group, COUNT(emp_dept.id) as ids;


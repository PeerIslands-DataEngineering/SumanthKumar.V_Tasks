import pandas as pd

data = {
    'Employee': ['Alice', 'Bob', 'Charlie', 'David', 'Emma', 'Frank', 'Grace', 'Hannah'],
    'Department': ['HR', 'IT', 'IT', 'HR', 'Finance', 'Finance', 'IT', 'HR'],
    'Age': [25, 32, 29, 41, 37, 45, 26, 38],
    'Salary': [50000, 70000, 65000, 80000, 75000, 90000, 62000, 85000],
    'Experience': [2, 7, 5, 15, 10, 20, 3, 12]
}

df = pd.DataFrame(data)

avg_salary = df.groupby('Department')['Salary'].mean()
print(avg_salary)

highest_paid = df.loc[df.groupby('Department')['Salary'].idxmax()]
print(highest_paid[['Department', 'Employee', 'Salary']])

count_filtered = df[(df['Experience'] > 5) & (df['Salary'] > 65000)].shape[0]
print(count_filtered)

def get_seniority(exp):
    if exp < 5:
        return 'Junior'
    elif 5 <= exp <= 10:
        return 'Mid-Level'
    else:
        return 'Senior'

df['Seniority'] = df['Experience'].apply(get_seniority)
print(df[['Employee', 'Experience', 'Seniority']])

it_sorted = df[df['Department'] == 'IT'].sort_values(by='Salary', ascending=False)
print(it_sorted[['Employee', 'Department', 'Salary']])

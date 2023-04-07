# Task 3: RESTful API using Flask

The mission was to create RESTful API using Flask package. Localhost is used as the API server with employees.csv file acting as the database. The following endpoints were required:
- GET /employee – obtains a json output for all existing employees in the database
- GET /employee/id – obtain a json output of an existing employee from the database
- POST /employee – Add a new employee to the database
- POST /employee/id – Update an existing employee in the database (id should not be
updatable)
- DELETE /employee/id – delete an existing employee from the database

To run this project:
- Create a new virtual environment: python3 -m venv venv
- Activate the virtual environment: source venv/bin/activate
- Install the requirements: pip install -r requirements.txt
- Launch the API: python3 employee_api.py

The file requests_examples.py shows different examples of requests made on the API


Here are some details about the implementation:
- Adding a new employee: requires at least the field 'employee' (its name). Any field that was not provided will be set to '' (empty string)  for strings, and 0 for integers.
- Updating the content of an employee: Only the provided fields will be updated, the remaining fields will remain intact.
- Any POST request modifies directly the content of the original csv file.
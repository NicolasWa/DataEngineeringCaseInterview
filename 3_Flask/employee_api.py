import os
from flask import Flask
from flask_restful import reqparse, abort, Api, Resource
from utils import csv_to_dict, dict_to_csv
import json


def abort_if_not_exists(id_employee, employees):
    """Aborts the request if the id of the employee 'id_employee' requested is not present in the data."""
    if id_employee not in employees:
        abort(404, message="Employee {} doesn't exist".format(id_employee))


app = Flask(__name__)
api = Api(app)

# Defining the parsers
parser_update = reqparse.RequestParser()
parser_update.add_argument('employee', type=str, help='employee must be a string')
parser_update.add_argument('gender', choices=('M', 'F'), type=str, help='Bad choice: {error_msg}. Possible values for gender are M or F')
parser_update.add_argument('age', type=int, help='age must be an integer')
parser_update.add_argument('salary', type=int, help='salary must be an integer')
parser_update.add_argument('town', type=str, help='town must be a string')

# parser_insert is similar to parser_update with the difference that it is mandatory to provide an employee name (field 'employee').
parser_insert = reqparse.RequestParser()
parser_insert.add_argument('employee', type=str, required=True, help='employee is required and must be a string')
parser_insert.add_argument('gender', choices=('M', 'F'), type=str, help='Bad choice: {error_msg}. Possible values for gender are M or F')
parser_insert.add_argument('age', type=int, help='age must be an integer')
parser_insert.add_argument('salary', type=int, help='salary must be an integer')
parser_insert.add_argument('town', type=str, help='town must be a string')


class Employee(Resource):
    def get(self, id):
        """GET to access a specific employee by providing its id."""
        employees = csv_to_dict(path_csv)
        abort_if_not_exists(id, employees)
        return json.dumps(employees[id], indent = 4), 200  # 200 OK: Indicates that the request has succeeded.
    

    def delete(self, id):
        """DELETE to delete a specific employee by providing its id."""
        employees = csv_to_dict(path_csv)
        abort_if_not_exists(id, employees)
        del employees[str(id)]
        dict_to_csv(employees, path_csv)
        return '', 204  # 204 No Content: The server has fulfilled the request but does not need to return a response body.
    
    def post(self, id):
        """POST to update a specific employee by providing its id. Note that PUT would be preferable, but I sticked to the test instructions"""
        employees = csv_to_dict(path_csv)
        abort_if_not_exists(id, employees)
        args = parser_update.parse_args(strict=True)
        data_update = employees[id]
        for field in args:
             # the fields can only be those defined in the parser. Any other given field will be ignored
            if args[field] is not None:
                data_update[field] = args[field]  
        # 'id' field should not be updatable, so it is hardwritten after the update as an extra security to avoid any id change attempt
        data_update['id'] = id 
        employees[id] = data_update
        dict_to_csv(employees, path_csv)
        return '', 200   # 200 OK: Indicates that the request has succeeded.   #json.dumps(employees[id], indent = 4),


class EmployeeList(Resource):
    def get(self):
        """GET to show the list of all the employees."""
        employees = csv_to_dict(path_csv)
        return json.dumps(employees, indent = 4), 200  # 200 OK: Indicates that the request has succeeded.
    

    def post(self):
        """POST to add a new employee."""
        args = parser_insert.parse_args(strict=True) 
        employees = csv_to_dict(path_csv)
        new_id = int(max([int(x) for x in employees.keys()])) + 1  # creating a new index for the new employee
        new_data = {'id': new_id}  
        for field in args:
            # the fields can only be those defined in the parser. Any other given field will be ignored
            if args[field] == None:
                if field in ["age", "salary"]:
                    new_data[field] = 0  # missing integer values are set to 0
                else:
                    new_data[field] = '' # missing string values are set to '' (empty string)
            else:
                new_data[field] = args[field]  
        employees[new_id] = new_data
        dict_to_csv(employees, path_csv)

        return '', 201  # 201 Indicates that the request has succeeded and a new resource has been created as a result.  # json.dumps(employees[new_id], indent = 4)


## Setting up the API resources
api.add_resource(EmployeeList, '/employee')  # Resource employee to access all the employees or add a new one
api.add_resource(Employee, '/employee/<id>')  # Resource employee/id to access a specific employee, delete it, or update it


if __name__ == '__main__':
    """RESTful API using Flask package to access the data content of employee.csv"""
    file_name = "employees.csv"
    path_csv = os.path.join(os.getcwd(), file_name)    
    app.run(host='0.0.0.0', port=105, debug=True)
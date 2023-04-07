import requests

if __name__ == '__main__':
    host = "http://localhost:105"
    # GET requests
    ## Getting all the employees
    print("All the employees :")
    print(requests.get(host + "/employee").json()) 
    ## Getting a specific employee, here employee with id=1
    print("\nA specific employee: ")
    print(requests.get(host + "/employee/1").json())

    # POST requests
    ## adding a new employee (town was purposefully not provided, town will be '' (empty string))
    print("\nAdding a new employee: ")
    print(requests.post(host + "/employee", json={'employee': 'Diane', 'gender': 'M', 'age': 25, 'salary': 6500}))
    ## updating an existing employee, here employee with id=4
    print("\nUpdating a specific employee: ")
    print(requests.post(host + "/employee/4", json={'age': 32, 'salary': 12000}))

    # DELETE request, mention the id
    if requests.delete(host + "/employee/2").status_code==204:
            print("Employee with id=2 was deleted")
    else:
        print("Could not delete employee with id=2 (probably already deleted)")
        
       
    



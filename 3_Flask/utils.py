import csv

employee_fields = ["id", "employee", "gender", "age", "salary", "town"]

def csv_to_dict(csv_file_path):
    data_dict = {}

    with open(csv_file_path, encoding = 'utf-8') as file:
        csv_reader = csv.DictReader(file)
 
        for row in csv_reader:
            key = row['id']
            employee = row
            employee["id"] = int(employee["id"])
            employee["age"] = int(employee["age"])
            employee["salary"] = int(employee["salary"])
            data_dict[key] = employee

    return data_dict


def dict_to_csv(d, csv_file_path):
    ls = [d[i] for i in d]
    with open(csv_file_path, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = employee_fields)
        writer.writeheader()
        writer.writerows(ls)
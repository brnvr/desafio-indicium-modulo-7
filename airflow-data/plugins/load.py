import csv

def load_into_csv(filename:str, data:list[any]):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        
        for row in data:
            writer.writerow(row)

def load_into_txt(filename:str, data):
    with open(filename, 'w') as file:
        file.write(str(data))


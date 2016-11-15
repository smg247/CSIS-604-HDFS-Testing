FILES = {'XS' : 50, 'S' : 500, 'M' : 1000, 'L' : 10000, 'XL' : 100000, 'XXL' : 1000000}

def create_files():
    for key, value in FILES.iteritems():
        file_name = '../' + key + '.txt'
        create_file(file_name, value)


def create_file(file_name, number_of_lines):
    f = open(file_name, "w")
    for i in range(0, number_of_lines, 1):
        f.write("This is a line in a file; there are many like it, but this one is mine.\n")
    f.close()

create_files()